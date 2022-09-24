#!/bin/bash
cp ~/pip/pip-dev.ini ~/pip/pip.ini
. venv/Scripts/activate
pip uninstall -y libpycommon
pip install libpycommon
