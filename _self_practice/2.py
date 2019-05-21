
import sys

try:
	inputArgument=int(sys.argv[1])
	Factorial=1
	for i in range(1, inputArgument+1):
		Factorial=Factorial*i
	print (Factorial)
except IndexError:
	print ("ERROR: Provide input number")
	sys.exit(1)
	