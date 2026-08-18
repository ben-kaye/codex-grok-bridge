use std::path::Path;

use anyhow::{Context, Result};

/// Read a text file, optionally starting at a 1-based `line` offset and
/// limiting the number of lines returned.
pub async fn read_text_file(path: &Path, line: Option<u32>, limit: Option<u32>) -> Result<String> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read file: {}", path.display()))?;

    // If no line/limit requested, return the entire file.
    if line.is_none() && limit.is_none() {
        return Ok(content);
    }

    let start = line.map(|l| l.saturating_sub(1) as usize).unwrap_or(0);
    let lines: Vec<&str> = content.lines().collect();

    let end = match limit {
        Some(n) => (start + n as usize).min(lines.len()),
        None => lines.len(),
    };

    if start >= lines.len() {
        return Ok(String::new());
    }

    Ok(lines[start..end].join("\n"))
}

/// Write the full content of a text file, creating parent directories if needed.
pub async fn write_text_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directories for: {}", path.display()))?;
    }
    tokio::fs::write(path, content)
        .await
        .with_context(|| format!("failed to write file: {}", path.display()))
}
