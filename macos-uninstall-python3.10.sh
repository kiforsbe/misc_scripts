#!/bin/bash
# Safe uninstall script for Python 3.10 installed via python.org pkg installer on macOS
# Adds sanity checks and user confirmation before removal

PYVER="3.10"
FRAMEWORK_DIR="/Library/Frameworks/Python.framework/Versions/$PYVER"
APP_DIR="/Applications/Python $PYVER"

echo ">>> Preparing to uninstall Python $PYVER"

# Sanity checks
echo "Checking installation paths..."
if [ ! -d "$FRAMEWORK_DIR" ] && [ ! -d "$APP_DIR" ]; then
  echo "No Python $PYVER installation found in expected locations."
  echo "Aborting."
  exit 1
fi

echo "Found the following components to remove:"
[ -d "$FRAMEWORK_DIR" ] && echo " - Framework: $FRAMEWORK_DIR"
[ -d "$APP_DIR" ] && echo " - Application bundle: $APP_DIR"

echo " - Symlinks in /usr/local/bin pointing to Python $PYVER"
for link in /usr/local/bin/*python* /usr/local/bin/pip*; do
  if [ -L "$link" ] && [[ "$(readlink "$link")" == *"$PYVER"* ]]; then
    echo "   -> $link -> $(readlink "$link")"
  fi
done

echo " - pkgutil receipts matching org.python.python.$PYVER"

pkgutil --pkgs | grep "org.python.python.$PYVER"

echo
read -p "Do you want to proceed with uninstalling Python $PYVER? (y/N): " CONFIRM

if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
  echo "Aborted by user."
  exit 0
fi

echo ">>> Proceeding with uninstall..."

# 1. Remove framework
[ -d "$FRAMEWORK_DIR" ] && sudo rm -rf "$FRAMEWORK_DIR"

# 2. Remove application bundle
[ -d "$APP_DIR" ] && sudo rm -rf "$APP_DIR"

# 3. Remove symlinks
for link in /usr/local/bin/*python* /usr/local/bin/pip*; do
  if [ -L "$link" ] && [[ "$(readlink "$link")" == *"$PYVER"* ]]; then
    sudo rm "$link"
  fi
done

# 4. Forget pkgutil receipts
for receipt in $(pkgutil --pkgs | grep "org.python.python.$PYVER"); do
  sudo pkgutil --forget "$receipt"
done

echo ">>> Python $PYVER has been removed."
echo "Run 'which python3' and 'python3 --version' to verify remaining installations."
