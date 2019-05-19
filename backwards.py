
import sys

arguments = len(sys.argv) - 1
position = 1
finalString=[]

def revString(String):
	placeHolder=""
	for letter in String:
		placeHolder = letter+placeHolder
	#endFor
	return placeHolder
#endDef

while (arguments >= position):
	finalString.append((revString(sys.argv[position])))
	position+=1
#endWhile

print (" ".join(finalString[::-1]))
