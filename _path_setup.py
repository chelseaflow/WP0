"""
AgriFlow WP0 — Path Setup
============================
Import this at the top of any module to ensure WP0 root is in sys.path.

Usage (in any .py file):
    import _path_setup  # noqa  (if file is at WP0 root)
    
Or for files in subdirectories (collectors/, utils/):
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
"""

import sys
import os

# WP0 root = directory containing this file
WP0_ROOT = os.path.dirname(os.path.abspath(__file__))

# Add WP0 root to Python path so "config", "collectors", "utils" are importable
if WP0_ROOT not in sys.path:
    sys.path.insert(0, WP0_ROOT)
