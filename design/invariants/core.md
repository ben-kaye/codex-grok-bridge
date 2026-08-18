# Core invariants

1. The Codex UI talks only to a Codex app-server-compatible JSON-RPC endpoint.
2. Native Codex task traffic, including `thread/start`, is proxied byte-for-byte
   to and from the bundled Codex app-server. The bridge does not correlate,
   translate, persist ownership for, or reimplement native tasks. Only shared
   discovery responses are aggregated.
3. The bridge talks to Grok Build only through its supported ACP endpoint.
4. A task is pinned to one backend when it is created. A later model selection
   cannot silently move its history or execution to another backend.
5. One runtime owns each side effect. Tool activity is projected into the UI
   and is never executed again by the bridge or the other runtime.
6. Credentials remain owned by native Codex and Grok Build. The bridge does not
   read, copy, log, or persist either runtime's tokens.
7. Protocol inputs are validated at every boundary; unknown messages fail
   explicitly or degrade to a documented display-only form.
8. Cancellation, permission decisions, and terminal failures cross the bridge
   without being converted into success.
9. Desktop, native Codex app-server, and ACP versions are checked before a task
   starts. Unsupported combinations fail explicitly.
10. The signed desktop application is not modified. A launcher selects the
    bridge as the app-server executable and supplies the bundled Codex path.
    Launcher-only environment overrides are removed from the native child.
11. XAI-specific ACP extensions and request metadata are not portable ACP.
    Any dependency on them is covered by the Grok Build version gate and a
    focused protocol fixture.
