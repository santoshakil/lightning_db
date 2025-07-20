#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if version argument is provided
if [ $# -eq 0 ]; then
    print_error "Please provide a version number or increment type"
    echo "Usage: $0 <version|major|minor|patch>"
    echo "Examples:"
    echo "  $0 1.2.3"
    echo "  $0 patch  # increments patch version"
    echo "  $0 minor  # increments minor version"
    echo "  $0 major  # increments major version"
    exit 1
fi

# Get current version from Cargo.toml
CURRENT_VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
print_info "Current version: $CURRENT_VERSION"

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Determine new version
case "$1" in
    major)
        NEW_VERSION="$((MAJOR + 1)).0.0"
        ;;
    minor)
        NEW_VERSION="$MAJOR.$((MINOR + 1)).0"
        ;;
    patch)
        NEW_VERSION="$MAJOR.$MINOR.$((PATCH + 1))"
        ;;
    *)
        # Validate version format
        if [[ ! "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.*)?$ ]]; then
            print_error "Invalid version format: $1"
            exit 1
        fi
        NEW_VERSION="$1"
        ;;
esac

print_info "New version: $NEW_VERSION"

# Check if we're on the main branch
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "master" && "$CURRENT_BRANCH" != "main" ]]; then
    print_warn "Not on main branch (current: $CURRENT_BRANCH)"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check for uncommitted changes
if [[ -n $(git status -s) ]]; then
    print_error "Uncommitted changes detected"
    git status -s
    exit 1
fi

# Update version in Cargo.toml files
print_info "Updating Cargo.toml files..."
sed -i.bak "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" Cargo.toml
sed -i.bak "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" lightning_db_ffi/Cargo.toml
rm Cargo.toml.bak lightning_db_ffi/Cargo.toml.bak

# Update Cargo.lock
print_info "Updating Cargo.lock..."
cargo update -p lightning_db -p lightning_db_ffi

# Run tests
print_info "Running tests..."
cargo test --all-features

# Run clippy
print_info "Running clippy..."
cargo clippy --all-targets --all-features -- -D warnings

# Build to ensure everything compiles
print_info "Building release..."
cargo build --release

# Commit version bump
print_info "Committing version bump..."
git add Cargo.toml lightning_db_ffi/Cargo.toml Cargo.lock
git commit -m "chore: bump version to $NEW_VERSION"

# Create tag
print_info "Creating tag v$NEW_VERSION..."
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"

# Show what would be pushed
print_info "Changes to be pushed:"
git log --oneline -n 5
echo
print_info "Tag to be pushed: v$NEW_VERSION"

# Confirm before pushing
read -p "Push changes and tag? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Pushing to remote..."
    git push origin "$CURRENT_BRANCH"
    git push origin "v$NEW_VERSION"
    print_info "✅ Release v$NEW_VERSION has been pushed!"
    print_info "GitHub Actions will now build and publish the release."
    print_info "Monitor progress at: https://github.com/santoshakil/lightning_db/actions"
else
    print_warn "Push cancelled. To push manually, run:"
    echo "  git push origin $CURRENT_BRANCH"
    echo "  git push origin v$NEW_VERSION"
fi