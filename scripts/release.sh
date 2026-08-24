#!/bin/sh
set -e

VERSION=$(node -p "require('./package.json').version")
TAG="v$VERSION"

echo "Creating tag $TAG..."
git tag "$TAG"

echo "Pushing tag $TAG to origin..."
git push origin "$TAG"

echo "Done! Tag $TAG pushed. Pipeline will now run."
