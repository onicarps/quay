---
type: Runbook
title: ${DROID_TITLE_YAML}
description: Bounded local guidance for dispatching implementation work to Droid.
status: stable
generated:
  by: quay/scaffold-v1
  at: ${GENERATED_AT}
---

# Droid mission dispatch

Give Droid an explicit implementation boundary, repository path, acceptance checks, and
forbidden release actions. Select the installed Droid version's supported model and
reasoning flags before dispatch. `--auto` and `--skip-permissions-unsafe` are mutually
exclusive; use only the permission mode authorized by the operator.

Droid may implement and run development checks for its own change. It does not certify
independent review, publish releases, or claim another agent's work.
