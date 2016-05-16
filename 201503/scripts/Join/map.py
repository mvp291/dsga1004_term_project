#!/usr/bin/python
import sys

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	splitted = line.strip().split(",")
	if len(splitted) ==  45:
		key =  splitted[0]
		table =  "P"
	elif len(splitted) == 5:
		key = splitted[0]
		table =  "I"
		#print splitted
	content =  splitted[1:]
	content.insert(0, table)
	try:
		float(key)
		print "%s\t%s" %(key, ','.join(content))
	except:
		continue
