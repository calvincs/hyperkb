#!/usr/bin/env bash
#
# release.sh — tag and push a new hyperkb release.
# Usage: ./scripts/release.sh [major|minor|patch]  (default: patch)
#
set -euo pipefail

BUMP="${1:-patch}"

# Validate bump type
case "$BUMP" in
    major|minor|patch) ;;
    *) echo "Usage: $0 [major|minor|patch]"; exit 1 ;;
esac

# Must be on main branch
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$BRANCH" != "main" ]; then
    echo "Error: must be on main branch (currently on '$BRANCH')."
    exit 1
fi

# Must have clean working tree
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: working tree is dirty. Commit or stash changes first."
    exit 1
fi

# Run tests
echo "Running tests..."
.venv/bin/pytest tests/ -v
echo ""

# Get current version from latest tag
CURRENT_TAG="$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")"
CURRENT="${CURRENT_TAG#v}"  # strip leading v

IFS='.' read -r MAJOR MINOR PATCH_NUM <<< "$CURRENT"

case "$BUMP" in
    major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH_NUM=0 ;;
    minor) MINOR=$((MINOR + 1)); PATCH_NUM=0 ;;
    patch) PATCH_NUM=$((PATCH_NUM + 1)) ;;
esac

NEXT="v${MAJOR}.${MINOR}.${PATCH_NUM}"

echo "Current: $CURRENT_TAG"
echo "Next:    $NEXT"
read -rp "Tag and push $NEXT? [y/N] " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

git tag -a "$NEXT" -m "Release $NEXT"
git push origin main --tags

echo ""
echo "Released $NEXT"
