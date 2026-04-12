#!/bin/bash
# Script to verify commit messages follow conventional commits format
# Usage: ./scripts/verify-commits.sh [from-ref] [to-ref]

FROM_REF=${1:-HEAD~1}
TO_REF=${2:-HEAD}

echo "Verifying commit messages from $FROM_REF to $TO_REF..."
echo ""

# Check if commitlint is available
if ! command -v commitlint &> /dev/null; then
    echo "commitlint not found. Installing..."
    cd /Users/casazza/Repositories/olivecasazza/consortium
    npm install --save-dev @commitlint/config-conventional@^19.5.0 @commitlint/cli@^19.5.0
fi

# Run commitlint
npx commitlint --from $FROM_REF --to $TO_REF --verbose

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✓ All commit messages are valid!"
    exit 0
else
    echo ""
    echo "✗ Some commit messages are invalid. Please fix them."
    echo ""
    echo "Format: <type>(<scope>): <subject>"
    echo ""
    echo "Types: chore, ci, docs, feat, fix, perf, refactor, revert, style, test, build, ops, hotfix"
    echo ""
    echo "Examples:"
    echo "  git commit -m 'feat: add new feature'"
    echo "  git commit -m 'fix: resolve bug in module'"
    echo "  git commit -m 'docs: update README'"
    echo ""
    echo "For breaking changes, add 'BREAKING CHANGE:' in the commit footer:"
    echo "  git commit -m 'feat: remove deprecated API' -m 'BREAKING CHANGE: The old API has been removed.'"
    exit 1
fi
