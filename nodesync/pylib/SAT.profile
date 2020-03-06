import sys, java
import waslib

scriptDirectory="pylib"
sys.path.insert(0, scriptDirectory)

sys.modules['AdminApp']=AdminApp
sys.modules['AdminConfig']=AdminConfig
sys.modules['AdminControl']=AdminControl
sys.modules['AdminTask']=AdminTask
sys.modules['Help']=Help

if __name__ == "main":
	print "SAT profile is running standalone..."
else:
	print  __name__ , "module is loaded"
	print __name__,"reading..."
	print dir()
#endIfElse

#end_SAT.profile