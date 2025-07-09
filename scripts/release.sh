#!/bin/bash

# Lightning DB Release Script
# Automates version bumping, changelog generation, and release publishing

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PACKAGE_NAME="lightning_db"
CARGO_TOML="Cargo.toml"
CHANGELOG="CHANGELOG.md"
README="README.md"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to get current version from Cargo.toml
get_current_version() {
    grep '^version = ' "$CARGO_TOML" | sed 's/version = "\(.*\)"/\1/'
}

# Function to validate semantic version
validate_version() {
    local version=$1
    if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+)?$ ]]; then
        log_error "Invalid semantic version format: $version"
        log_info "Expected format: MAJOR.MINOR.PATCH or MAJOR.MINOR.PATCH-PRERELEASE"
        exit 1
    fi
}

# Function to bump version
bump_version() {
    local current_version=$1
    local bump_type=$2
    
    IFS='.' read -ra VERSION_PARTS <<< "$current_version"
    local major=${VERSION_PARTS[0]}
    local minor=${VERSION_PARTS[1]}
    local patch=${VERSION_PARTS[2]}
    
    case $bump_type in
        "major")
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        "minor")
            minor=$((minor + 1))
            patch=0
            ;;
        "patch")
            patch=$((patch + 1))
            ;;
        *)
            log_error "Invalid bump type: $bump_type"
            log_info "Valid options: major, minor, patch"
            exit 1
            ;;
    esac
    
    echo "$major.$minor.$patch"
}

# Function to update version in Cargo.toml
update_cargo_version() {
    local new_version=$1
    log_info "Updating version in $CARGO_TOML to $new_version"
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s/^version = \".*\"/version = \"$new_version\"/" "$CARGO_TOML"
    else
        # Linux
        sed -i "s/^version = \".*\"/version = \"$new_version\"/" "$CARGO_TOML"
    fi
}

# Function to update version in README badges
update_readme_version() {
    local new_version=$1
    log_info "Updating version badges in $README"
    
    if [[ -f "$README" ]]; then
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/crates\.io%2F[^-]*-/crates.io%2F$new_version-/" "$README"
            sed -i '' "s/docs\.rs\/lightning_db\/[^\/]*/docs.rs\/lightning_db\/$new_version/" "$README"
        else
            # Linux
            sed -i "s/crates\.io%2F[^-]*-/crates.io%2F$new_version-/" "$README"
            sed -i "s/docs\.rs\/lightning_db\/[^\/]*/docs.rs\/lightning_db\/$new_version/" "$README"
        fi
    fi
}

# Function to generate changelog entry
generate_changelog_entry() {
    local version=$1
    local date=$(date +%Y-%m-%d)
    
    log_info "Generating changelog entry for version $version"
    
    # Get commits since last tag
    local last_tag=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
    local commits
    
    if [[ -n "$last_tag" ]]; then
        commits=$(git log --pretty=format:"- %s" "$last_tag"..HEAD)
    else
        commits=$(git log --pretty=format:"- %s")
    fi
    
    # Create changelog entry
    local changelog_entry="## [$version] - $date

### Added
### Changed
### Fixed
### Security

$commits

"
    
    # Prepend to changelog
    if [[ -f "$CHANGELOG" ]]; then
        local temp_file=$(mktemp)
        echo "$changelog_entry" > "$temp_file"
        cat "$CHANGELOG" >> "$temp_file"
        mv "$temp_file" "$CHANGELOG"
    else
        echo "# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

$changelog_entry" > "$CHANGELOG"
    fi
}

# Function to run tests
run_tests() {
    log_info "Running tests..."
    cargo test --all-features
    log_success "All tests passed"
}

# Function to run linting
run_linting() {
    log_info "Running linting..."
    cargo clippy --all-targets --all-features -- -D warnings
    cargo fmt --check
    log_success "Linting passed"
}

# Function to build documentation
build_docs() {
    log_info "Building documentation..."
    cargo doc --all-features --no-deps
    log_success "Documentation built successfully"
}

# Function to create git tag
create_git_tag() {
    local version=$1
    local tag_name="v$version"
    
    log_info "Creating git tag: $tag_name"
    
    # Add all changes
    git add -A
    
    # Commit version bump
    git commit -m "chore: bump version to $version

- Update Cargo.toml version
- Update README badges
- Generate changelog entry for $version

🤖 Generated with [Claude Code](https://claude.ai/code)"
    
    # Create annotated tag
    git tag -a "$tag_name" -m "Release version $version

This release includes:
$(git log --pretty=format:"- %s" $(git describe --tags --abbrev=0 2>/dev/null || git rev-list --max-parents=0 HEAD)..HEAD)

🤖 Generated with [Claude Code](https://claude.ai/code)"
    
    log_success "Git tag $tag_name created"
}

# Function to publish to crates.io
publish_crate() {
    log_info "Publishing to crates.io..."
    
    # Dry run first
    log_info "Performing dry run..."
    cargo publish --dry-run
    
    # Ask for confirmation
    echo
    read -p "Proceed with publishing to crates.io? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cargo publish
        log_success "Package published to crates.io"
    else
        log_warning "Publishing to crates.io cancelled"
    fi
}

# Function to push to remote
push_to_remote() {
    log_info "Pushing to remote repository..."
    git push origin main
    git push origin --tags
    log_success "Changes and tags pushed to remote"
}

# Function to create GitHub release
create_github_release() {
    local version=$1
    local tag_name="v$version"
    
    log_info "Creating GitHub release..."
    
    # Check if gh CLI is available
    if ! command -v gh &> /dev/null; then
        log_warning "GitHub CLI (gh) not found. Please create the release manually at:"
        log_info "https://github.com/santoshakil/lightning_db/releases/new?tag=$tag_name"
        return
    fi
    
    # Generate release notes
    local last_tag=$(git describe --tags --abbrev=0 2>/dev/null | grep -v "^$tag_name$" | head -1 || echo "")
    local release_notes
    
    if [[ -n "$last_tag" ]]; then
        release_notes=$(git log --pretty=format:"- %s" "$last_tag".."$tag_name")
    else
        release_notes=$(git log --pretty=format:"- %s" "$tag_name")
    fi
    
    # Create release
    gh release create "$tag_name" \
        --title "Lightning DB $version" \
        --notes "## Changes in this release:

$release_notes

## Installation

Add this to your \`Cargo.toml\`:

\`\`\`toml
[dependencies]
lightning_db = \"$version\"
\`\`\`

## Documentation

- [API Documentation](https://docs.rs/lightning_db/$version)
- [Repository](https://github.com/santoshakil/lightning_db)

🤖 Generated with [Claude Code](https://claude.ai/code)"
    
    log_success "GitHub release created: $tag_name"
}

# Function to display usage
usage() {
    echo "Lightning DB Release Script"
    echo
    echo "Usage: $0 [OPTIONS] <VERSION_OR_BUMP_TYPE>"
    echo
    echo "Arguments:"
    echo "  VERSION_OR_BUMP_TYPE    Either a specific version (e.g., 1.2.3) or bump type (major|minor|patch)"
    echo
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -n, --dry-run           Perform a dry run without making changes"
    echo "  --skip-tests            Skip running tests"
    echo "  --skip-lint             Skip linting"
    echo "  --skip-docs             Skip building documentation"
    echo "  --skip-publish          Skip publishing to crates.io"
    echo "  --skip-github           Skip creating GitHub release"
    echo
    echo "Examples:"
    echo "  $0 1.2.3               Release version 1.2.3"
    echo "  $0 patch               Bump patch version (0.1.0 -> 0.1.1)"
    echo "  $0 minor               Bump minor version (0.1.0 -> 0.2.0)"
    echo "  $0 major               Bump major version (0.1.0 -> 1.0.0)"
    echo "  $0 --dry-run patch     Show what would happen without making changes"
}

# Main function
main() {
    local version_input=""
    local dry_run=false
    local skip_tests=false
    local skip_lint=false
    local skip_docs=false
    local skip_publish=false
    local skip_github=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -n|--dry-run)
                dry_run=true
                shift
                ;;
            --skip-tests)
                skip_tests=true
                shift
                ;;
            --skip-lint)
                skip_lint=true
                shift
                ;;
            --skip-docs)
                skip_docs=true
                shift
                ;;
            --skip-publish)
                skip_publish=true
                shift
                ;;
            --skip-github)
                skip_github=true
                shift
                ;;
            -*)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                if [[ -z "$version_input" ]]; then
                    version_input="$1"
                else
                    log_error "Too many arguments"
                    usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    # Check if version input is provided
    if [[ -z "$version_input" ]]; then
        log_error "Version or bump type is required"
        usage
        exit 1
    fi
    
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        log_error "Not in a git repository"
        exit 1
    fi
    
    # Check if working directory is clean
    if [[ -n $(git status --porcelain) ]]; then
        log_error "Working directory is not clean. Please commit or stash changes first."
        exit 1
    fi
    
    # Get current version
    local current_version
    current_version=$(get_current_version)
    log_info "Current version: $current_version"
    
    # Determine new version
    local new_version
    if [[ "$version_input" =~ ^(major|minor|patch)$ ]]; then
        new_version=$(bump_version "$current_version" "$version_input")
        log_info "Bumping $version_input version: $current_version -> $new_version"
    else
        new_version="$version_input"
        validate_version "$new_version"
        log_info "Setting specific version: $new_version"
    fi
    
    if [[ "$dry_run" == true ]]; then
        log_warning "DRY RUN MODE - No changes will be made"
        log_info "Would update version from $current_version to $new_version"
        exit 0
    fi
    
    # Pre-release checks
    if [[ "$skip_tests" != true ]]; then
        run_tests
    fi
    
    if [[ "$skip_lint" != true ]]; then
        run_linting
    fi
    
    if [[ "$skip_docs" != true ]]; then
        build_docs
    fi
    
    # Update version files
    update_cargo_version "$new_version"
    update_readme_version "$new_version"
    generate_changelog_entry "$new_version"
    
    # Create git tag and commit
    create_git_tag "$new_version"
    
    # Push changes
    push_to_remote
    
    # Publish package
    if [[ "$skip_publish" != true ]]; then
        publish_crate
    fi
    
    # Create GitHub release
    if [[ "$skip_github" != true ]]; then
        create_github_release "$new_version"
    fi
    
    log_success "Release $new_version completed successfully!"
    log_info "Next steps:"
    log_info "1. Monitor the release at https://github.com/santoshakil/lightning_db/releases"
    log_info "2. Check crates.io publication at https://crates.io/crates/lightning_db"
    log_info "3. Update documentation if needed"
}

# Run main function with all arguments
main "$@"