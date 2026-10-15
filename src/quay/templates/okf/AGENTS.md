---
type: Runbook
title: ${AGENT_TITLE_YAML}
description: Repository-local routing instructions for agents working in this project.
status: stable
generated:
  by: quay/scaffold-v1
  at: ${GENERATED_AT}
---

# Agent instructions

Read [okf/index.md](okf/index.md) before changing this repository. Runtime coordination
state lives in `.quay/` and must remain untracked; durable project knowledge lives in
`okf/` and is reviewed through Git.

- Preserve existing work and refuse destructive operations without explicit authority.
- Keep status, decisions, and handoffs attributable to their sources.
- Treat local producer names as consistency metadata, not cryptographic identity.
- Run the repository's checks before handing implementation work off.
- Do not edit generated handoffs; correct the source event or add a superseding concept.
