##############################################################################################################
# Script Name   : rollingRestarts.py 
# $r2d2c3p0
# Date of birth : 06/28/2010.
# Explanation   : To performs the JVMs rolling restarts. 
#                 The script stops and starts all the nodeagents and the JVMs under the nodes sequentially 
#                 in the order provided by the property file rolling_restart.properties under the propertes directory.
# Dependencies  : utils.py under same level and the rolling_restart.properties.
# Modified      : Added shell scripts to kill the nodeagent pids on 07/07/2010.
# 
#
##############################################################################################################

# Global imports
##############################################################################################################

import java, sys, os, time

global AdminConfig
global AdminControl
from time import strftime

# Main Method
###############################################################################################################

print;print;print
lineSeparator = java.lang.System.getProperty('line.separator')
UTILS = sys.argv[0];execfile(UTILS);properties = loadJVMPropertyFile()
noOfns = properties.getProperty('Number_of_Nodes').strip()
for i in range(int(noOfns)):
    j = str(i+1)
    NODES_JVM_PAIRS = properties.getProperty('Nodes_JVMS_PAIRS'+j).strip()
    NODES_JVM_PAIRS = NODES_JVM_PAIRS.split('^');nodeName = NODES_JVM_PAIRS[0]
    nodeName = nodeName.strip();cellName = AdminControl.getCell()
    scopeId = AdminConfig.getid("/Cell:"+cellName+"/Node:"+nodeName+"/")
    if len(scopeId) > 0:
       na = AdminControl.queryNames('type=NodeAgent,node='+nodeName+',*')
       FLAG = 0;LOOPFLAG = 0
       logS("Restarts on node "+nodeName+"")
       print;logInfo("Issuing stop, sync and start on the node "+nodeName+"")
       try:
            _excp_ = 0
            na_result = AdminControl.invoke(na,'restart','true false')
       except:
            _type_, _value_, _tbck_ = sys.exc_info()
            na_result = `_value_`
            _excp_ = 1
       #endTry
       if (_excp_):
          logWar("Nodeagent "+nodeName+" STOPPED or HUNG.")
          logInfo(" ---> Issuing KILL -9/STARTNODE on the nodeagent PID.")
          os.system('../bin/killna_start.sh '+nodeName+'')
       #endIf
       time.sleep(10)
       nodeagentId = AdminControl.completeObjectName("type=Server,node="+nodeName+",name=nodeagent,*")
       while len(nodeagentId) == 0:
             nodeagentId = AdminControl.completeObjectName("type=Server,node="+nodeName+",name=nodeagent,*")
             if len(nodeagentId) > 0:
                logInfo("Nodeagent on "+nodeName+" was restarted SUCCESSFULLY.")
                break
             else:
                logInfo(" ---> Nodeagent coming up.")
             #endElse
             if LOOPFLAG > 5:
                logWar("The restart is taking longer time than anticipated on node "+nodeName+"")
                logInfo(" ---> Issuing KILL -9/STARTNODE on the nodeagent PID.")
                os.system('../bin/killna_start.sh '+nodeName+'') 
                break
             else:
                logInfo("Status check in 30 seconds.")
                time.sleep(30)
                LOOPFLAG = LOOPFLAG + 1
             #endElse
       #endWhile
       JVM_PAIRS = NODES_JVM_PAIRS[1];JVMS = JVM_PAIRS.split(':')
       for JVM in JVMS:
           cellName = AdminControl.getCell();serverName = JVM;serverName = serverName.strip()
           serverID = AdminConfig.getid("/Cell:"+cellName+"/Node:"+nodeName+"/Server:"+serverName+"/")
           if len(serverID) > 0:
              print;logInfo("Restarting the JVM "+serverName+" on the node "+nodeName+"")
              runningID = AdminControl.completeObjectName("type=Server,node="+nodeName+",process="+serverName+",*")
              if len(runningID) > 0:
                 logInfo("Initial status ---> STARTED.")
                 stopJVM(nodeName, serverName)
                 time.sleep(5)
                 startJVM(nodeName, serverName)
              else:
                 logInfo("Initial status ---> STOPPED.")
                 startJVM(nodeName, serverName)
              #endElse
           else:
              print;logError("The JVM "+serverName+" is not found.")
           #endElse
       #endFor
       JFLAG = 0;JLOOPFLAG = 0;SERVERFLAG = 0
       numberofJVMS = len(JVMS)
       numberofJVMS = numberofJVMS - SERVERFLAG
       print;logInfo(" ---> JVMs status on node "+nodeName+"")
       while JFLAG < numberofJVMS:
             for serverName in JVMS:
                 serverID = AdminConfig.getid("/Cell:"+cellName+"/Node:"+nodeName+"/Server:"+serverName+"/")
                 if len(serverID) > 0:
                    serverId = AdminControl.completeObjectName("type=Server,node="+nodeName+",name="+serverName+",*")
                    if len(serverId) > 0:
                       try:
                              _excp_ = 0
                              result = AdminControl.getAttribute(serverId, "state")
                       except:
                              _type_, _value_, _tbck_ = sys.exc_info()
                              result = `_value_`
                              _excp_ = 1
                       #endTry
                       if (_excp_):
                          logWar("JVM "+serverName+" seems to be HUNG!")
                          logInfo("Please contact WAS team");print
                       else:
                          if (result == "STARTED"):
                             logInfo(""+serverName+" ---> STARTED.")
                             JFLAG = JFLAG + 1
                          #endIf
                       #endElse
                    else:
                       logInfo(""+serverName+" ---> STOPPED.")
                       SERVERFLAG = SERVERFLAG + 1
                    #endElse
                 else:
                    print;logError("The JVM "+serverName+" is not found.")
                 #endElse   
             #endFor
             if JLOOPFLAG > 10:
                logError("The restart is taking longer time than anticipated on node "+nodeName+"")
                logInfo("URGENT!!! Please contact WAS team");print
                #sys.exit(127)
                break
             #endIf
             if JFLAG < numberofJVMS:
                JFLAG = 0
                logInfo(" ---> Status check in 3 minutes.");print
                sleep(180)
                JLOOPFLAG = JLOOPFLAG + 1
             #endIf
       #endWhile
       print;logC("Restarts on node "+nodeName+"");print;print
    else:
       print;logError("The node "+nodeName+" is not found.")
    #endElse
#endFor
