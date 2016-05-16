#!/usr/bin/python
import sys
key_current = None
total =  list()
inc = list()
pop = list()

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	#line = line.replace('\n', '')
	splitted = line.strip().split("\t")

	if len(splitted)==2:
		key =  splitted[0]
		content = splitted[1]
		
		if not key_current:
			key_current = key
		
		if key == key_current:
			total.append(content)
		else:
			if key_current:
				for pair in range(len(total)):
					intext =  total[pair].split(",")
					if intext[0]=='I':
						inc.append(intext[1:])
					elif intext[0]=='P':
						pop.append(intext[1:])
				
			
				if len(inc)>0:
					for p in range(len(pop)):
						for i in range(len(inc)):

							print_cont = pop[p]+inc[i]
							print "%s,%s" %(key_current, ",".join(print_cont))
				elif len(inc) ==0 :
					inc_u = ['']*4
					for p in range(len(pop)):	
						print_cont = pop[p]+inc_u
						print "%s, %s" %(key_current, ",".join(print_cont))

			pop = []
			inc = []
			key_current = key
			total = [content]


for pair in range(len(total)):
	intext =  total[pair].split(",")
	if intext[0]=='I':
		inc.append(intext[1:])
	elif intext[0]=='P':
		pop.append(intext[1:])


if len(inc)>0:
	for p in range(len(pop)):
		for i in range(len(inc)):
			print_cont = pop[p]+inc[i]
			print "%s,%s" %(key_current, ",".join(print_cont) )
elif len(inc) ==0 :
	for p in range(len(pop)):
		inc_u =  [' ']*4
		print_cont = pop[p]+inc_u
		print "%s,%s" %(key_current, ",".join(print_cont))
		
