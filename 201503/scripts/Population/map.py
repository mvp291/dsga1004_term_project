#!/usr/bin/python
import sys

range_pop1 = [ [0, 8250], [8250, 20250], [20250, 27500], [27500, 35000], [35000, 41000], [41000, 48250], [48250, 60000], [60000, 71500], [71500, 86000]]
range_pop1.insert(len(range_pop1), [86000, float('inf')])

range_pop2= [[round(10000*i,2),(i+1)*10000] for i in range(0,10,1)]
range_pop2.insert(len(range_pop2), [100000, float('inf')])

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	splitted = line.replace("\n","").replace("\r","").split(",")
	if len(splitted) == 327:
		key = splitted[2].strip().split()[-1] #Zip code
		idx_cont =  [3, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 67, 73, 77, 81, 85, 89, 93, 97, 101, 105, 109, 261, 265, 285, 289, 293, 297, 301, 305, 309, 313]
		cont =  [splitted[i] for i in idx_cont]				
		try: 			
			population =  float(cont[0])
			for rang in range_pop1:
				if ((population >= rang[0]) and (population <rang[-1])):
		            		range_val1 = rang
			range_ind1 = range_pop1.index(range_val1)
			
			for rang in range_pop2:
				if ((population >= rang[0]) and (population <rang[-1])):
	                       		range_val2 = rang
			range_ind2 =  range_pop2.index(range_val2)

			r1 = range_pop1[range_ind1]
			r1 = map(lambda x: str(x), r1)
			r2 = range_pop2[range_ind2]
			r2 = map(lambda x: str(x), r2)
			p1 =  '-'.join(r1)
			p2 =  '-'.join(r2)

			cont.insert(1, p1)
			cont.insert(2, p2)
			print "%s,%s" %(key, ','.join(cont))
		except Exception as e:
			print e
			continue

