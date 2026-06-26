#!/usr/bin/env bash
#
# gosec.sh — security scan for DataQL.
#
# The following rules are excluded because they are inherent to a single-user,
# local-file CLI tool (not a multi-tenant network service). The rationale is
# recorded here so the exclusion is a deliberate, reviewable decision:
#
#   G104 - unchecked errors: enforced instead by errcheck (golangci-lint) with a
#          curated policy; gosec's G104 double-reports the same call sites.
#   G201 - SQL string formatting: building SQL from the user's own files/queries
#          IS the product. There is no untrusted multi-tenant input.
#   G304 - file inclusion via variable: opening the path the user passed on the
#          command line IS the product.
#   G301 - directory permissions: output/cache dirs use standard CLI conventions
#          on the user's own machine.
#   G306 - WriteFile permissions: ditto for output/cache files.
#
# All other rules stay active. Site-specific exceptions use a `#nosec <RULE>`
# comment with a justification (see pkg/github/issues.go, decompression paths).

set -euo pipefail
cd "$(dirname "$0")/.."

exec go run github.com/securego/gosec/v2/cmd/gosec@latest \
  -exclude=G104,G201,G304,G301,G306 \
  "$@" ./...
