#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>" >&2
  echo "Example: $0 v0.3.0" >&2
  exit 1
fi

VERSION="$1"
PLAIN_VERSION="${VERSION#v}"

if [[ "${VERSION}" != v* ]]; then
  echo "Version must start with 'v' (for example: v0.2.3)." >&2
  exit 1
fi

if [[ -z "${PLAIN_VERSION}" || "${PLAIN_VERSION}" == "${VERSION}" ]]; then
  echo "Unable to derive Maven version from '${VERSION}'." >&2
  exit 1
fi

mvn versions:set -DnewVersion="${PLAIN_VERSION}" -DgenerateBackupPoms=false
mvn -DskipTests package

cat <<EOF
Prepared Maven project for release ${VERSION}.

Next steps:
1. Review the changed POM files.
2. Run validation as required by CLAUDE.md.
3. Commit with: chore: release ${VERSION}
4. Open a PR to main and merge it after checks pass.
5. Create and push tag ${VERSION} from the merge commit on main.
6. After release, bump to the next SNAPSHOT version.
EOF
