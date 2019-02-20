
# r2d2c3p0
# 2/19/2019

import time

go_forward=0

while not go_forward:
	start_time=time.time()
	user_decision = raw_input("Continue [y/N]  ")
	if user_decision == "y":
		go_forward=1
		end_time=time.time()
	#endIf
#endWhile

total_seconds="%.0f" %(end_time-start_time)

if total_seconds >59:
	print "Total pause time: %s seconds" %(total_seconds)
else:
	print "Total pause time: %.0f minutes" %(int(total_seconds)/60)
#endIfElse