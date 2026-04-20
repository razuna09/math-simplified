#!/usr/bin/env python3
# .github/scripts/sanity_check.py
# Simple import & location/version check for numpy and pandas.
# Exit code 0 on success, non-zero on failure.

import importlib
import sys
import traceback
import os

packages = ("numpy", "pandas")

def show_numpy_libs(numpy_mod):
    try:
        base = os.path.dirname(getattr(numpy_mod, "__file__", "") or "")
        libs = os.path.join(base, "numpy.libs")
        if os.path.isdir(libs):
            print("numpy.libs contents:")
            for fn in sorted(os.listdir(libs)):
                print("  ", fn)
        else:
            print("numpy.libs: (not present)")
    except Exception:
        print("Failed to list numpy.libs")
        traceback.print_exc()

for pkg in packages:
    try:
        mod = importlib.import_module(pkg)
        path = getattr(mod, "__file__", None)
        ver = getattr(mod, "__version__", None)
        print(f"{pkg} -> {path!s}  ver: {ver!s}")
        if pkg == "numpy":
            show_numpy_libs(mod)
    except Exception as exc:
        print(f"{pkg} IMPORT ERROR: {exc}")
        traceback.print_exc()
        sys.exit(2)

print("Sanity checks passed.")
sys.exit(0)
