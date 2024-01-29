#!/bin/env python3

"""
- List contents of target file
- Get name of directory and file inside
- Transfer to /var/lib/mysql/clientdb
"""

import os
import sys
import shutil

USAGE = f"""Usage: {sys.argv[0]} from_path to_path
"""

if len(sys.argv) <= 1:
    print(USAGE)
    exit(1)

dfrom = sys.argv[1]
dto = sys.argv[2]

for parent, dirs, files in os.walk(dfrom, topdown=False):

    if not files:
        continue

    ffrom = os.path.join(parent, files[0])
    filename = os.path.basename(parent).split('-')[0] + '.csv'

    fto = os.path.join(dto, filename)

    print('copy', ffrom, 'to', fto)

    shutil.copy(ffrom, fto)










