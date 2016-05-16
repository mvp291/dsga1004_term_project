#!/usr/bin/python
import sys

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	line = line.replace('\n', '')
	print line
