
import sys

def revString(String):
	placeHolder=""
	for letter in String:
		print ("letter: "+letter)
		placeHolder = letter+placeHolder
		print (placeHolder)
		
	return placeHolder

print (revString(sys.argv[1]))