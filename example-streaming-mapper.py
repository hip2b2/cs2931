#!/usr/bin/python 
import sys
import re

for line in sys.stdin:
  clean = line.strip()
  m = re.match("^.*\/([A-Za-z0-9.-]*)\'$", clean)
  if (m): 
    print "%s,%d" % (m.group(1), 1)
