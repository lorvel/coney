#!/bin/sh
set -e

VERSION=$(node -p "require('./package.json').version")
TAG="v$VERSION"

# The publish job builds the TAGGED commit and refuses to publish unless
# package.json matches the tag, so everything that will ship must already be
# committed and pushed. Checking it here turns a failed pipeline into a one-line
# message before anything reaches origin.
if [ -n "$(git status --porcelain)" ]; then
  echo "refusing to release: the working tree is dirty — commit or stash first" >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "refusing to release: tag $TAG already exists — bump \"version\" in package.json first" >&2
  exit 1
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ -n "$(git log --oneline "origin/$BRANCH..$BRANCH" 2>/dev/null)" ]; then
  echo "refusing to release: $BRANCH has commits that are not on origin — push them first" >&2
  exit 1
fi

echo "Creating tag $TAG..."
git tag "$TAG"

echo "Pushing tag $TAG to origin..."
git push origin "$TAG"

echo "Done! Tag $TAG pushed. Pipeline will now run."
