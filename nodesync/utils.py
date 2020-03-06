##################################################################################
# Script Name : utils.py
# $r2d2c3p0
# Explanation : Utility Script.
#
##################################################################################

import java, sys, os, time

global AdminConfig
global AdminControl
from time import strftime

# Methods
##################################################################################

def loadJVMPropertyFile():
      properties = java.util.Properties()
      ioStream = java.io.FileInputStream("rollingRestarts.properties")
      properties.load(ioStream)
      return properties
#endDef

def splitProps(propString):
    propList = [];tempProp = '';inQuotes = 0
    for char in propString[1:-1]:
        if char == ' ':
           if inQuotes == 0:	    
              propList.append(tempProp)
              tempProp = ''
           else:	    
              tempProp = tempProp + char
           #endIf	
        elif char == '"':
            if inQuotes == 0:
	       inQuotes = 1
            else:
               inQuotes = 0
            #endIf
        else:
            tempProp = tempProp + char
        #endIf
    #endFor
    if tempProp != '': 
       propList.append(tempProp)
    #endIf
    return propList
#endDef

def NodesSync(Node):
    Node = Node.strip()		
    if (Node != ""):	
       NodeSync = AdminControl.completeObjectName("type=NodeSync,node="+node+",*")	
       try:
	  _excp_ = 0
	  error = AdminControl.invoke(NodeSync, "sync")				
       except:
	  _type_, _value_, _tbck_ = sys.exc_info()
	  error = `_value_`
	  _excp_ = 1			
       #endTry 	
       if (_excp_):			
	  logError("Synchronization with node:"+Node+"")
	  logInfo("Make sure that node agent has started on node:"+Node+"");print""
       else:
          logInfo("Node:"+Node+" synchronization successfull");print""			
       #endIf
    #endIf
#endDef 

def wsadminToList(inStr):
    outList=[]
    if (len(inStr) > 0 and inStr[0] == '[' and inStr[-1] == ']'):
       inStr = inStr[1:-1]
       tmpList = inStr.split(" ")
    else:
       tmpList = inStr.split("\n")
    #endIf
    for item in tmpList:
        item = item.rstrip();
        if (len(item) > 0):
           outList.append(item)
        #endIf
    #endFor
    return outList
#endDef

def length(Lists):
    val = 0
    for List in Lists:
        val = val + 1
    #endFor
    return val
#endDef

def logMessageValues(message, value):
    format = '%-5s : %-10s'
    Timer = strftime("%m/%d/%Y %H:%M:%S")
    print format % ('['+Timer+'] '+message+'', value)
#endDef

def logInfo(message):
    format = '%-6s : %2s'
    Timer = strftime("%m/%d/%Y %H:%M:%S")
    print format % ('['+Timer+'] INFO', message)
#endDef

def logError(error):
    format = '%-6s : %2s'
    Timer = strftime("%m/%d/%Y %H:%M:%S")
    print format % ('['+Timer+'] ERROR', error)
#endDef

def logWar(warning):
    format = '%-6s : %2s'
    Timer = strftime("%m/%d/%Y %H:%M:%S")
    print format % ('['+Timer+'] WARNING', warning)
#endDef

def logS(Strted):
    format = '%-6s : %2s'
    Timer = strftime("%m/%d/%Y %H:%M:%S")
    print format % ('['+Timer+'] START', Strted)
#endDef

def logC(comp):
    format = '%-6s : %2s'
    Timer = strftime("%m/%d/%Y %H:%M:%S")
    print format % ('['+Timer+'] COMPLETE', comp)
#endDef

def stopJVM(nodeName, serverName):
    logInfo("Issuing STOP.")
    try:
       _excp_ = 0
       result = logInfo(AdminControl.stopServer(serverName, nodeName))
    except:
       _type_, _value_, _tbck_ = sys.exc_info()
       result = `_value_`
       _excp_ = 1
    #endTry
    if (_excp_):
       logError("Stopping the JVM "+serverName+"")
    else:
       time.sleep(10)
       stopID = AdminControl.completeObjectName("type=Server,node="+nodeName+",process="+serverName+",*")
       if len(stopID) > 0:
          logInfo("After issuind STOP, The JVM "+serverName+" is still in START state.")
          logError("The JVM "+serverName+" may be HUNG.")
          logInfo(" --> Issuing terminate command on "+serverName+"")
          stop_result = AdminControl.invoke(na, 'terminate', serverName)
          if stop_result == "true":
             logInfo(""+serverName+" terminated succesfully.")
          else:
             print;logError("The JVM - "+serverName+" CANNOT be STOPPED.");print
          #endElse
       else:
          logInfo("STOPPED successfully.")
       #endElse
    #endElse
#endDef

def startJVM(nodeName, serverName):
    logInfo("Issuing START.")
    try:
       _excp_ = 0
       result = AdminControl.startServer(serverName, nodeName, 5)
    except:
       _type_, _value_, _tbck_ = sys.exc_info()
       result = `_value_`
       _excp_ = 1
    #endTry
    if (_excp_):
       logError("Starting the JVM - "+serverName+"")
    #endIf
    #else:
    #    time.sleep(10)
    #    startID = AdminControl.completeObjectName("type=Server,node="+nodeName+",process="+serverName+",*")
    #    if len(startID) > 0:
    #       logInfo("The JVM "+serverName+" STARTED successfully.")
    #    else:
    #       logError("The JVM "+serverName+" is in STOP state.")
    #    #endElse
    ##endElse
#endDef
