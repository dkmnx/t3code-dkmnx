# xi

xi is a terminal-native coding agent. T3 Code reads its model catalog and
uses the agent over `xi rpc`.

## Set up xi

1. Install the `xi` binary and put it on the server's `PATH`.
2. Sign in with `xi login <provider>`, or set a provider key in the
   environment.
3. Open Settings → Providers, enable **xi**, and open a thread.

`xi models --json` must list at least one model. The provider card shows a
warning when the catalog is empty.

Each model carries its own reasoning levels. A model with no levels shows
no reasoning control, because xi advertises none for it.

## Configuration

The provider settings accept a binary path and extra launch arguments.
Use `XI_CONFIG_DIR`, `XI_STATE_DIR`, `XI_CODING_AGENT_DIR`, and
`XI_CACHE_DIR` on the instance environment for a separate account or
workspace. xi keeps one workspace per process, so the thread directory
must match the process directory.

## Status

The model catalog is live. Thread execution over `xi rpc` arrives with the
adapter milestone; until then, opening a session reports that the agent
session is unavailable.
