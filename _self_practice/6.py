
class A(object):
	
	def __init__(self):
		self.String=""
		
	def getString(self):
		self.String = input("Enter a string:")
		
	def printString(self):
		print (self.String)

a=A()
a.getString()
a.printString()