
import math

oBucket=[]
iBucket=input("Enter comma separated number:").split(",")
C=50
H=30

for I in iBucket:
	X=round(math.sqrt((2 * C * int(I))/H))
	oBucket.append(str(X))
	
print (",".join(oBucket))