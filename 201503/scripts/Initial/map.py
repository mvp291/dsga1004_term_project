#!/usr/bin/python
import sys

range_income1 = [[0,32000], [32000, 40000], [40000, 45500], [45500, 52500], [52500, 58250], [58250, 64724], [64724, 72800], [72800, 86000], [86000, 103000]]
range_income1.insert(0, [103000, float('inf')])

range_income2= [[round(10000*i,2),(i+1)*10000] for i in range(0,9,1)]
range_income2.insert(len(range_income2), [100000, float('inf')])

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	splitted = line.replace("\n","").replace("\r","").split(",")
	if len(splitted) == 5 or len(splitted) ==6 :
		key = splitted[2].strip().split()[-1] #Zip code
		if len(splitted)== 6:
			income = 250001
			error =  0			
	
		else:
			income = splitted[3]
			error =  splitted[4]				
		try: 
			
			income =  float(income)
			error =  float(error)
			for rang in range_income1:
				if ((income >= rang[0]) and (income <rang[-1])):
		            		range_val1 = rang
			range_ind1 = range_income1.index(range_val1)
			
			for rang in range_income2:
				if ((income >= rang[0]) and (income <rang[-1])):
                        		range_val2 = rang
			range_ind2 =  range_income2.index(range_val2)

			r1 = range_income1[range_ind1]
			r1 = map(lambda x: str(x), r1)
			r2 = range_income2[range_ind2]
			r2 = map(lambda x: str(x), r2)
			p1 =  '-'.join(r1)
			p2 =  '-'.join(r2)
			print "%s,%s,%s,%s,%s" %(key, income, error, p1, p2)
		except Exception as e:
			continue

