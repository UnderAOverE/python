#
# Author: r2d2c3p0.
# 01/06/2016.
# Version: 1.0
#

# Import modules.
import sys
import os

# Initialization.
__file__="syncNode.py"
__author__="r2d2c3p0"
pwdPath=os.path.dirname(os.path.abspath(__file__))
execfile(pwdPath+"/pylib/utilib")
execfile(pwdPath+"/pylib/customprint")
execfile(pwdPath+"/pylib/waslib")
execfile(pwdPath+"/pylib/node")
execfile(pwdPath+"/pylib/simulation")

eprint=customprint(ERROR)
rprint=customprint("Sync start")

# Main program.

Simulation=0;print

rprint("--")

for arg__1 in sys.argv:
	if arg__1=="-sim":
		Simulation=1
	#endIf
#endFor 

nodeConfigFile=utilib().loadConfigFile("Node")
configNodes=nodeConfigFile.getProperty("NODE_NAME").strip()
configSyncType=nodeConfigFile.getProperty("SYNC_TYPE").strip()
if len(configNodes)==0:
	node(Simulation).saveAndNodeSync()
else:
	for configNode in configNodes.split(","):
		configNode=configNode.strip()
		if node(0).checkIfExist(configNode):
			node(Simulation).synchronization(configSyncType, configNode)
		else:
			eprint("%s is not defined or not found under %s cell." %(configNode, waslib().getCellName()))
		#endIfElse
	#endFor
#endIfElse

if Simulation: 
	simulation(1)
#endIf

rprint("")

#end_syncNode.py
