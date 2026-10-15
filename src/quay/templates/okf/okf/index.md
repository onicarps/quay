---
type: Index
title: ${INDEX_TITLE_YAML}
description: Entry point for repository-local project knowledge and agent context.
status: stable
generated:
  by: quay/scaffold-v1
  at: ${GENERATED_AT}
---

# ${PROJECT_NAME}

- [Status](status.md)
- [Intent](intent.yaml)
- [Recent handoffs](log.md)
- [Runner configuration](harness/runners.toml)
- [Droid dispatch guidance](harness/droid.md)

Distilled handoffs are stored under `handoffs/YYYY/MM/`. Runtime ledger files stay in
`.quay/` and are never project knowledge.
