#!/bin/bash
set -e

# Download latest release asset from GitHub

# --- CONFIGURATION ---
OWNER="ontodev"           # GitHub username or org
REPO="robot"        # Repository name
ASSET_PATTERN="robot.jar"     # Part of the filename to match (optional)
INSTALL_DIR="./scripts/robot/"    # Where to save the file
# ----------------------

mkdir -p "INSTALL_DIR"

# Get latest release info from GitHub API
API_URL="https://api.github.com/repos/$OWNER/$REPO/releases/latest"

echo "Fetching latest release info for $OWNER/$REPO ..."
JSON=$(curl -sL "$API_URL")

# Extract asset download URL (matching pattern, if given)
ASSET_URL=$(echo "$JSON" | grep "browser_download_url" | grep "$ASSET_PATTERN" | head -n 1 | cut -d '"' -f 4)

if [ -z "$ASSET_URL" ]; then
    echo "❌ No asset found matching '$ASSET_PATTERN'!"
    exit 1
fi

FILENAME=$(basename "$ASSET_URL")

OUTPUT_PATH="$INSTALL_DIR/$FILENAME"

echo "Saving file at: $OUTPUT_PATH"

echo "Downloading $FILENAME ..."

# ✅ Check if file already exists
if [ -f "$OUTPUT_PATH" ]; then
    echo "✅ File already exists at $OUTPUT_PATH, skipping download."
else
    echo "Downloading $FILENAME ..."
    curl -L -o "$OUTPUT_PATH" "$ASSET_URL"
fi

echo "$INSTALL_DIR""$ASSET_PATTERN"

# make it executable
chmod +x "$INSTALL_DIR""$ASSET_PATTERN"

# add to PATH for *current shell session*
JAR_PATH=$(realpath "$INSTALL_DIR$ASSET_PATTERN")
SCRIPT_PATH=$(realpath "${INSTALL_DIR}robot.sh")

echo "jar_path: $JAR_PATH"
echo "script_path: $SCRIPT_PATH"

export PATH="$JAR_PATH:$PATH"
export PATH="$SCRIPT_PATH:$PATH"

echo "✅ Installed robot to $INSTALL_DIR"
echo "PATH updated: $INSTALL_DIR"

sh ./scripts/robot/robot.sh

sh ./scripts/mini_mondo.sh
sh ./scripts/mini_hp.sh