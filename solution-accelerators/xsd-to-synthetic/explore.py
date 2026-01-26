#!/usr/bin/env python3
"""
Quick script to explore XSD packages locally.

Usage:
    python explore.py /path/to/xsd-directory
    python explore.py /path/to/schema.xsd
    python explore.py /path/to/xsd-directory IRS990T   # Preview specific element
"""
import sys
import json

def load_library():
    """Load the notebook library into globals."""
    with open('notebooks/XSD to Synthetic - Library.ipynb', 'r') as f:
        nb = json.load(f)
    
    code = '\n\n'.join([
        ''.join(c['source']) for c in nb['cells'] 
        if c['cell_type'] == 'code' 
        and '%pip' not in ''.join(c['source']) 
        and 'dbutils' not in ''.join(c['source'])
    ])
    
    # Mock Spark for local use
    class SparkSession: pass
    class DataFrame: pass
    
    globs = {'SparkSession': SparkSession, 'DataFrame': DataFrame}
    exec(code, globs)
    return globs

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    path = sys.argv[1]
    root_element = sys.argv[2] if len(sys.argv) > 2 else None
    
    print("Loading library...")
    globs = load_library()
    
    print(f"\nExploring: {path}")
    if root_element:
        print(f"Root element: {root_element}")
    
    globs['explore_xsd'](path, root_element=root_element)
