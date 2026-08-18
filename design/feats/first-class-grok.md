# First-class Grok

## Outcome

Launch the Codex desktop UI once, choose either a native Codex model or Grok
4.6, and retain the selected runtime's native task behavior.

## Acceptance criteria

- Native Codex models and Grok 4.6 appear in the same model picker.
- Each model advertises only the reasoning efforts and input modalities its
  backend supports.
- Selecting a native Codex model preserves native task, history, approval, and
  streaming behavior through transparent app-server proxying.
- Users can create, cancel, resume, and list Grok-backed tasks.
- Text, reasoning, plans, tool activity, usage, and errors stream incrementally.
- Permission requests are decided in the Codex UI and returned to Grok.
- Codex-backed and Grok-backed tasks can run concurrently.
- Task identity and backend ownership survive bridge restarts.
- Attempting to change an existing task to the other backend fails clearly.
- Unsupported Codex-only features are hidden or fail clearly.

## Not in the first release

- Patching the desktop bundle.
- Moving an existing task between Codex and Grok backends.
- Translating Grok into an OpenAI Responses model provider.
- Reimplementing Grok tools, authentication, plugins, or session storage.
