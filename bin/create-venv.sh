#!/usr/bin/env sh

set -e  # Exit immediately if a command exits with a non-zero status. (Especially key for test below)
set -u  # Treat unset variables as an error when substituting.
set -x  # Print commands and their arguments as they are executed.

HERE="$(dirname -- "$0")"
test -f "${HERE}/israelGtfsRoutesInShape.py"  # Ensure we're in the right directory

VENV="${HERE}/venv"

python3 -m venv --clear "${VENV}"  # Create the virtual environment; --clear makes it purge any previously created environment
"${VENV}/bin/pip" --require-virtualenv install shapely # Install packages in the virtual environment
