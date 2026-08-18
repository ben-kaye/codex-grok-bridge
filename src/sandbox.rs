//! Bubblewrap (`bwrap`) sandbox for `command/exec`.
//!
//! Wraps a command invocation inside a `bwrap` sandbox based on the Codex
//! `SandboxPolicy`.  Uses Linux user namespaces for filesystem isolation
//! (`--ro-bind`, `--bind`) and `--unshare-net` for network isolation.
//!
//! Call [`check_availability`] at startup to detect whether `bwrap` is
//! installed and functional.  [`wrap_command`] will refuse to run if it isn't.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Result, bail};
use codex_protocol::protocol::{SandboxPolicy, WritableRoot};
use tracing::{debug, info, warn};

/// Cached result of the startup `bwrap` availability check.
static BWRAP_AVAILABLE: OnceLock<bool> = OnceLock::new();

/// Check whether `bwrap` is installed and functional.  Call once at startup;
/// the result is cached for the lifetime of the process.
///
/// Probes with a real (minimal) sandbox invocation rather than just
/// `--version`, so systems with disabled user namespaces are detected.
pub fn check_availability() -> bool {
    *BWRAP_AVAILABLE.get_or_init(run_probe)
}

/// Actually execute the bwrap probe.  Separated from [`check_availability`]
/// so that both it and [`is_available`] can pass this to `OnceLock::get_or_init`
/// without re-entrant deadlock.
fn run_probe() -> bool {
    match std::process::Command::new("bwrap")
        .args([
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--unshare-pid",
            "--proc",
            "/proc",
            "--",
            "/bin/true",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .status()
    {
        Ok(status) if status.success() => {
            info!("bwrap sandbox available and functional");
            true
        }
        Ok(status) => {
            warn!(
                exit_code = ?status.code(),
                "bwrap found but sandbox probe failed (user namespaces may be disabled)"
            );
            false
        }
        Err(e) => {
            warn!(error = %e, "bwrap not found — sandboxed command/exec will be rejected");
            false
        }
    }
}

/// Returns `true` if bwrap was detected as available at startup.
/// Lazily runs the probe if [`check_availability`] hasn't been called yet.
pub fn is_available() -> bool {
    *BWRAP_AVAILABLE.get_or_init(run_probe)
}

/// Returns `true` if the given policy requires bwrap to enforce.
pub fn policy_requires_bwrap(policy: &SandboxPolicy) -> bool {
    matches!(
        policy,
        SandboxPolicy::ReadOnly { .. } | SandboxPolicy::WorkspaceWrite { .. }
    )
}

/// If the policy requires sandboxing, wrap `(program, args)` inside a bwrap
/// invocation.  Returns the (possibly rewritten) `(program, args)`.
///
/// Returns `Err` if the policy requires bwrap but:
/// - bwrap is not available
/// - restricted read access is requested (not yet supported, matching upstream)
/// - a writable root does not exist
///
/// - `DangerFullAccess` / `ExternalSandbox` → passthrough (no bwrap).
/// - `ReadOnly` → whole FS read-only, no network.
/// - `WorkspaceWrite` → read-only FS + writable roots, protected subpaths
///   (`.git`, `.codex`, `.agents`) re-locked read-only, optional network.
pub fn wrap_command(
    program: &str,
    args: &[String],
    cwd: &Path,
    policy: &SandboxPolicy,
) -> Result<(String, Vec<String>)> {
    match policy {
        SandboxPolicy::DangerFullAccess | SandboxPolicy::ExternalSandbox { .. } => {
            Ok((program.to_string(), args.to_vec()))
        }
        SandboxPolicy::ReadOnly { .. } | SandboxPolicy::WorkspaceWrite { .. } => {
            if !is_available() {
                bail!("sandbox policy requires bwrap but it is not installed or not functional");
            }

            // Restricted read access is not yet supported — fail closed,
            // matching the upstream Codex linux-sandbox implementation.
            if !policy.has_full_disk_read_access() {
                bail!(
                    "restricted read-only access is not yet supported by the bwrap sandbox backend"
                );
            }

            let writable_roots = policy.get_writable_roots_with_cwd(cwd);
            ensure_mount_targets_exist(&writable_roots)?;

            let mut bwrap_args: Vec<String> = Vec::new();

            // Session and lifecycle.
            bwrap_args.extend(["--new-session".into(), "--die-with-parent".into()]);

            // Start with the entire filesystem read-only.
            bwrap_args.extend(["--ro-bind".into(), "/".into(), "/".into()]);

            // Minimal /dev with standard devices (null, zero, full, random,
            // urandom, tty).  Must come before writable binds so that explicit
            // /dev/* writable roots remain visible.
            bwrap_args.extend(["--dev".into(), "/dev".into()]);

            // Bind writable roots (overlays the ro-bind).
            for wr in &writable_roots {
                let root = path_to_string(wr.root.as_path());
                bwrap_args.extend(["--bind".into(), root.clone(), root]);
            }

            // Re-apply read-only protections on subpaths within writable roots.
            let allowed_write_paths: Vec<PathBuf> = writable_roots
                .iter()
                .map(|wr| wr.root.as_path().to_path_buf())
                .collect();

            for subpath in collect_read_only_subpaths(&writable_roots) {
                // Symlink attack: if a protected path is a symlink inside a
                // writable root, mount /dev/null on the symlink to block rewiring.
                if let Some(symlink_path) = find_symlink_in_path(&subpath, &allowed_write_paths) {
                    bwrap_args.extend([
                        "--ro-bind".into(),
                        "/dev/null".into(),
                        path_to_string(&symlink_path),
                    ]);
                    continue;
                }

                if !subpath.exists() {
                    // Non-existent protected path: mount /dev/null on the first
                    // missing component to prevent creation.
                    if let Some(first_missing) = find_first_non_existent_component(&subpath)
                        && is_within_allowed_write_paths(&first_missing, &allowed_write_paths)
                    {
                        bwrap_args.extend([
                            "--ro-bind".into(),
                            "/dev/null".into(),
                            path_to_string(&first_missing),
                        ]);
                    }
                    continue;
                }

                // Existing protected path inside a writable root: re-lock.
                if is_within_allowed_write_paths(&subpath, &allowed_write_paths) {
                    let s = path_to_string(&subpath);
                    bwrap_args.extend(["--ro-bind".into(), s.clone(), s]);
                }
            }

            // PID namespace isolation.
            bwrap_args.push("--unshare-pid".into());

            // Network isolation (empty network namespace = no interfaces).
            if !policy.has_full_network_access() {
                bwrap_args.push("--unshare-net".into());
            }

            // Mount /proc so the sandboxed process can inspect itself.
            bwrap_args.extend(["--proc".into(), "/proc".into()]);

            // Separator + original command.
            bwrap_args.push("--".into());
            bwrap_args.push(program.to_string());
            bwrap_args.extend(args.iter().cloned());

            debug!(
                bwrap_args = ?bwrap_args,
                writable_roots = writable_roots.len(),
                "sandbox: wrapping command with bwrap"
            );

            Ok(("bwrap".to_string(), bwrap_args))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers (ported from codex-rs/linux-sandbox/src/bwrap.rs)
// ---------------------------------------------------------------------------

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

/// Collect unique read-only subpaths across all writable roots.
fn collect_read_only_subpaths(writable_roots: &[WritableRoot]) -> Vec<PathBuf> {
    let mut subpaths = std::collections::BTreeSet::new();
    for wr in writable_roots {
        for sp in &wr.read_only_subpaths {
            subpaths.insert(sp.as_path().to_path_buf());
        }
    }
    subpaths.into_iter().collect()
}

/// Validate that writable roots exist before constructing mounts.
fn ensure_mount_targets_exist(writable_roots: &[WritableRoot]) -> Result<()> {
    for wr in writable_roots {
        let root = wr.root.as_path();
        if !root.exists() {
            bail!(
                "sandbox expected writable root {}, but it does not exist",
                root.display()
            );
        }
    }
    Ok(())
}

/// Returns `true` when `path` is under any allowed writable root.
fn is_within_allowed_write_paths(path: &Path, allowed_write_paths: &[PathBuf]) -> bool {
    allowed_write_paths
        .iter()
        .any(|root| path.starts_with(root))
}

/// Find the first symlink along `target_path` that is also under a writable root.
fn find_symlink_in_path(target_path: &Path, allowed_write_paths: &[PathBuf]) -> Option<PathBuf> {
    let mut current = PathBuf::new();
    for component in target_path.components() {
        use std::path::Component;
        match component {
            Component::RootDir => {
                current.push(Path::new("/"));
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir => {
                current.pop();
                continue;
            }
            Component::Normal(part) => current.push(part),
            Component::Prefix(_) => continue,
        }
        let metadata = match std::fs::symlink_metadata(&current) {
            Ok(m) => m,
            Err(_) => break,
        };
        if metadata.file_type().is_symlink()
            && is_within_allowed_write_paths(&current, allowed_write_paths)
        {
            return Some(current);
        }
    }
    None
}

/// Find the first missing path component while walking `target_path`.
fn find_first_non_existent_component(target_path: &Path) -> Option<PathBuf> {
    let mut current = PathBuf::new();
    for component in target_path.components() {
        use std::path::Component;
        match component {
            Component::RootDir => {
                current.push(Path::new("/"));
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir => {
                current.pop();
                continue;
            }
            Component::Normal(part) => current.push(part),
            Component::Prefix(_) => continue,
        }
        if !current.exists() {
            return Some(current);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn danger_full_access_passthrough() {
        let (prog, args) = wrap_command(
            "ls",
            &["-la".into()],
            Path::new("/tmp"),
            &SandboxPolicy::DangerFullAccess,
        )
        .unwrap();
        assert_eq!(prog, "ls");
        assert_eq!(args, vec!["-la"]);
    }

    #[test]
    fn external_sandbox_passthrough() {
        let policy = SandboxPolicy::ExternalSandbox {
            network_access: codex_protocol::protocol::NetworkAccess::Enabled,
        };
        let (prog, args) = wrap_command("ls", &[], Path::new("/tmp"), &policy).unwrap();
        assert_eq!(prog, "ls");
        assert!(args.is_empty());
    }

    #[test]
    fn read_only_wraps_with_bwrap() {
        if !is_available() {
            eprintln!("skipping: bwrap not available");
            return;
        }
        let (prog, args) = wrap_command(
            "cat",
            &["/etc/passwd".into()],
            Path::new("/tmp"),
            &SandboxPolicy::new_read_only_policy(),
        )
        .unwrap();
        assert_eq!(prog, "bwrap");
        assert!(args.contains(&"--ro-bind".to_string()));
        assert!(args.contains(&"--unshare-net".to_string()));
        assert!(args.contains(&"--unshare-pid".to_string()));
        // Original command is at the end after "--".
        let sep_pos = args.iter().position(|a| a == "--").unwrap();
        assert_eq!(args[sep_pos + 1], "cat");
        assert_eq!(args[sep_pos + 2], "/etc/passwd");
    }

    #[test]
    fn workspace_write_includes_bind_mounts() {
        if !is_available() {
            eprintln!("skipping: bwrap not available");
            return;
        }
        let policy = SandboxPolicy::new_workspace_write_policy();
        // Use /tmp which always exists.
        let (prog, args) =
            wrap_command("echo", &["hello".into()], Path::new("/tmp"), &policy).unwrap();
        assert_eq!(prog, "bwrap");
        let bind_positions: Vec<_> = args
            .iter()
            .enumerate()
            .filter(|(_, a)| *a == "--bind")
            .map(|(i, _)| i)
            .collect();
        assert!(
            !bind_positions.is_empty(),
            "should have writable --bind mounts"
        );
    }

    #[test]
    fn workspace_write_with_network_skips_unshare_net() {
        if !is_available() {
            eprintln!("skipping: bwrap not available");
            return;
        }
        let policy = SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![],
            read_only_access: codex_protocol::protocol::ReadOnlyAccess::FullAccess,
            network_access: true,
            exclude_tmpdir_env_var: false,
            exclude_slash_tmp: false,
        };
        let (prog, args) = wrap_command(
            "curl",
            &["https://example.com".into()],
            Path::new("/tmp"),
            &policy,
        )
        .unwrap();
        assert_eq!(prog, "bwrap");
        assert!(!args.contains(&"--unshare-net".to_string()));
    }

    #[test]
    fn restricted_read_access_rejected() {
        if !is_available() {
            eprintln!("skipping: bwrap not available");
            return;
        }
        let policy = SandboxPolicy::ReadOnly {
            access: codex_protocol::protocol::ReadOnlyAccess::Restricted {
                include_platform_defaults: true,
                readable_roots: vec![],
            },
        };
        let result = wrap_command("ls", &[], Path::new("/tmp"), &policy);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("restricted read-only access"),
            "error should mention restricted read: {msg}"
        );
    }

    #[test]
    fn nonexistent_writable_root_rejected() {
        if !is_available() {
            eprintln!("skipping: bwrap not available");
            return;
        }
        let policy = SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![
                codex_utils_absolute_path::AbsolutePathBuf::try_from(Path::new(
                    "/nonexistent_sandbox_test_root_12345",
                ))
                .unwrap(),
            ],
            read_only_access: codex_protocol::protocol::ReadOnlyAccess::FullAccess,
            network_access: false,
            exclude_tmpdir_env_var: true,
            exclude_slash_tmp: true,
        };
        let result = wrap_command("ls", &[], Path::new("/tmp"), &policy);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("does not exist"),
            "error should mention missing root: {msg}"
        );
    }
}
