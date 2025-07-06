#!/bin/bash
# Cargokit iOS/macOS build script for Lightning DB

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PLUGIN_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Run cargokit build tool
cd "$PLUGIN_ROOT"
"$SCRIPT_DIR/build_tool/bin/build_tool" build-pod