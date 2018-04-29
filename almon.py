from __future__ import division

# python

#-------------------------------------------------------------------------------------------------------------------#
#                                                                                                                   #
# Author        : r2d2c3p0                                                                                          #
# Script Name   : almon.py.                                                                                         #
# Date of birth : 02/24/2015.                                                                                       #
# Explanation   : Tool to monitor the TCP connections and generate alerts to send email and text \                  #
#                 notifications to the Web-Operations team.                                                         #
# Modifications : 02/24/2015 - Original version ($1.0$).                                                            #
#                 03/17/2015 - Added the appserver name to the alert email ($1.0a$).                                #
#                 03/20/2015 - Added code to lock the program ($1.0b$).                                             #
#                 03/21/2015 - Added code to notify when the connections reach zero ($1.0c$).                       #
#                 03/30/2015 - Major code upgrade from threading module to multiprocessing module ($2.0$).          #
# Dependencies  : 1. Python 2.6 or greater.                                                                         #
#                 2. almon.config [master_config_file].                                                             #
#                 3. Runs on unix machines (developed on Linux machine) only.                                       #
# Version       : 2.0                                                                                               #
# Contact       : r2d2c3p0                                                                                          #
#                                                                                                                   #
#-------------------------------------------------------------------------------------------------------------------#

#-------------------------------------------------------------------------------------------------------------------#
# 0. Global imports.                                                                                                #
#-------------------------------------------------------------------------------------------------------------------#

from os import listdir
from os.path import isfile, join
from time import strftime
from sys import exit

import math
import time
import os
import os.path
import ConfigParser
import smtplib
import hashlib
import gc
import multiprocessing
import sys

#-------------------------------------------------------------------------------------------------------------------#
# 1. Initialization and global variables.                                                                           #
#-------------------------------------------------------------------------------------------------------------------#

suspendTime=0
eLogging=0
tConn=0
mxC=0
pgI=0
appServersPorts='a:0'
tcpNetFile='tcp.file'
suspendFile='suspend.file'

__author__='r2d2c3p0'
__version__='2.0'

#-------------------------------------------------------------------------------------------------------------------#
# 2. Classes and definitions.                                                                                       #
#-------------------------------------------------------------------------------------------------------------------#

class Logging:
      def __init__(self, eflag):
          self.enableL=eflag
          self.logLevel='DEBUG'
      def echo(self, _message):
          if self.enableL:
             get_trace=sys._getframe(1).f_code.co_name, str(sys._getframe(1).f_lineno)
             trace_string=":".join(get_trace)
             format_='%-6s | %2s'
             millisecond = "%s" %(repr(time.time()))
             _timer = strftime("%m/%d/%Y %H:%M:%S:" + millisecond.split('.')[1] + " %Z")
             print format_ %('['+_timer+'] | '+self.logLevel+' | version:'+__version__+' | '+trace_string+'', _message)

class readConfigFile:

      def __init__(self):
          config=ConfigParser.RawConfigParser()
          config.read(master_config_file)
          self.returnArray=[]
          maximumConnections=int(config.get('Connection settings', 'maximum.connections'))
          pingInterval=int(config.get('Connection settings', 'ping.interval'))
          thresholdValue=int(config.get('Connection settings', 'threshold.value'))
          monitorPorts=config.get('Connection settings', 'applicationservers.ports')
          tcpFileName=config.get('Connection settings', 'tcp.filename')
          self.returnArray.append(thresholdValue)
          self.returnArray.append(maximumConnections)
          self.returnArray.append(pingInterval)
          self.returnArray.append(monitorPorts)
          self.returnArray.append(tcpFileName)
          self.enabletool=int(config.get('Alerts settings', 'run.almon'))
          self.suspendFile=config.get('Alerts settings', 'suspend.file')
          self.eLogging=int(config.get('Alerts settings', 'enable.logging'))
          self.logFile=config.get('Alerts settings', 'logfile.name')
          self.debugGC=int(config.get('Alerts settings', 'enable.gcdebug'))
          self.notifySender=config.get('Email settings', 'sender.address')
          
      def getConnectionValues(self):
          return self.returnArray
          
      def enableTool(self):
          return self.enabletool
          
#endClass

class notifyAdmins:
      def __init__(self, eflag):
          self.eLogging=eflag
      def sendAlerts(self, subject_, messageBody, connectionArray, serverName):
          Logging(self.eLogging).echo('[%s] email.' %(serverName))
          configDirectory_os, configFileName_os = os.path.split(master_config_file)
          getEmailValues=readConfigFile()
          senderAddress=getEmailValues.notifySender
          nDirectory=configDirectory_os
          complete_to_address=[]
          emailaddress=""
          configFiles=[file_ for file_ in listdir(nDirectory) if isfile(join(nDirectory,file_))]
          for userFile in configFiles:
              if userFile.find('.email')!=-1:
                 fullpathUserFile=nDirectory+'/'+userFile
                 ufo=open(fullpathUserFile, 'r')
                 for line_ufo in ufo:
                     emailaddress=line_ufo
                     break
                 ufo.close()
                 for email_id in emailaddress.split(','):
                     if email_id.find('@')!=-1:
                        complete_to_address.append(email_id)
          if len(complete_to_address)!=0:
             Logging(self.eLogging).echo('email group: %s' %(complete_to_address))
             message="""Subject: [%s] Connections Alert [%s].
%s
%s""" %(subject_, serverName, messageBody, connectionArray)
             try:
                 smtpObject=smtplib.SMTP('localhost')
                 smtpObject.sendmail(senderAddress, complete_to_address, message)
                 Logging(self.eLogging).echo('email and text sent succesfully.')
             except SMTPException:
                 #print "Error: unable to send email"
                 Logging(self.eLogging).echo('email failed.')
                 pass
          else:
             Logging(self.eLogging).echo('no sender found!')
             Logging(self.eLogging).echo('************* no email files found. **************')
             Logging(self.eLogging).echo('%s %s %s' %(serverName, messageBody, connectionArray))

#endClass

class programUtility:
      def __init__(self):
          self.connectionState={
                             '01':'ESTABLISHED',
                             '02':'SYN_SENT',
                             '03':'SYN_RECV',
                             '04':'FIN_WAIT1',
                             '05':'FIN_WAIT2',
                             '06':'TIME_WAIT',
                             '07':'CLOSE',
                             '08':'CLOSE_WAIT',
                             '09':'LAST_ACK',
                             '0A':'LISTEN',
                             '0B':'CLOSING'
          }
      def parseProcNetTCPFile(self, procNetTCPFile):
          tcpFileObject=open(procNetTCPFile,'r')
          content = tcpFileObject.readlines()
          content.pop(0)
          return content
      def removeEmptySpace(self, array):
          return [x for x in array if x !='']
      def getPort(self, array):
          g_port = array.split(':')[1]
          return str(int(g_port,16))
      def pythonNetstat(self, netPort, tcpFile):
          numberOfConnections=0
          content=self.parseProcNetTCPFile(tcpFile)
          for line in content:
              line_array=self.removeEmptySpace(line.split(' '))
              if self.getPort(line_array[1])==netPort:
                 if self.connectionState[line_array[3]]=="ESTABLISHED":
                    numberOfConnections+=1
          return numberOfConnections
      def getFileChecksum(self, gfileName):
          with open(gfileName, 'rb') as gfileHandle:
               mdfive=hashlib.md5()
               while True:
                     fileData=gfileHandle.read(8192)
                     #Logging(1).echo('md5 %s' %(fileData))
                     if not fileData:
                        break
                     mdfive.update(fileData)
               return mdfive.hexdigest()
      def setFileChecksum(self, sfileName, ckstring):
          try:
               with open(sfileName, 'wb') as sfileHandle:
                    sfileHandle.write(ckstring)
          except IOError:
               print '[almon] can not run multiple instances!'
               exit()
      def readFileChecksum(self, rfileName):
          with open(rfileName, 'rb') as rfileHandle:
               while True:
                     tempCksum=rfileHandle.read()
                     break;
          return tempCksum
      def checkFileExistsAndReadable(self, file_name):
          if os.path.isfile(file_name) and os.access(file_name, os.R_OK):
             return 1
          return 0

#endClass

class __premain__method__(multiprocessing.Process):
      def __init__(self, asNamePortNumber, TCPFile, activeApplicationServersQueue, eflag=0):
          multiprocessing.Process.__init__(self)
          self.asNamePortNumber=asNamePortNumber
          self.portNumber=asNamePortNumber.split(':')[1]
          self.TCPFile=TCPFile
          self.activeApplicationServersQueue=activeApplicationServersQueue
          self.utilObject=programUtility()
          self.eLogging=eflag
      def run(self):
          currentConnections=self.utilObject.pythonNetstat(self.portNumber, self.TCPFile)
          if not currentConnections:
             Logging(self.eLogging).echo('__premain__[reason: zero connections] placing %s under no-monitor list.' %(self.asNamePortNumber.split(':')[0]))
             self.activeApplicationServersQueue.put(0)
          else:
             if currentConnections>=20:
                Logging(self.eLogging).echo('__premain__ placing %s under monitor list.' %(self.asNamePortNumber.split(':')[0]))
                self.activeApplicationServersQueue.put(self.asNamePortNumber)
             else:
                Logging(self.eLogging).echo('__premain__[reason: below pre-main threshold] placing %s under no-monitor list.' %(self.asNamePortNumber.split(':')[0]))
                self.activeApplicationServersQueue.put(0)

class analyzeData:
      def __init__(self, inputArray, eflag):
          self.eLogging=eflag
          self.inputArray=inputArray
          self.sizeOfArray=len(self.inputArray)
      def standardDeviation(self):
          Logging(eLogging).echo('calculating standard deviation.')
          numbersSDTotal=0
          largeValue=0
          arrayMean=self.getMean()
          for numbersd in self.inputArray:
              numbersSDTotal=numbersSDTotal+(int(numbersd)-arrayMean)**2
              if int(numbersd) >= largeValue:
                 largeValue=int(numbersd)
          sdMean=numbersSDTotal/self.sizeOfArray
          sD=math.sqrt(sdMean)
          returnMessage="""
The mean of the connections: %s
Standard Deviation for the connections: %s
Largest connection: %s
          """ %(arrayMean, sD, largeValue)
          Logging(self.eLogging).echo('sd returned %s' %(sD))
          return returnMessage
      def getMean(self):
          Logging(self.eLogging).echo('calculating mean.')
          numbersTotal=0
          for number in self.inputArray:
              numbersTotal=numbersTotal+int(number)
          arrayMean=numbersTotal/self.sizeOfArray
          Logging(self.eLogging).echo('mean returned %s' %(arrayMean))
          return arrayMean
      def rampUp(self):
          returnValue=1
          junkValue=-1
          for i in range(self.sizeOfArray):
              if int(self.inputArray[i])>=junkValue:
                 junkValue=int(self.inputArray[i])
              else:
                 returnValue=0
          Logging(self.eLogging).echo('rampup returned %s' %(returnValue))
          return returnValue
      def seeSaw(self):
          pass
      def rampDown(self):
          returnValue=1
          cmpValue=int(self.inputArray[0])
          for i in range(self.sizeOfArray):
              if cmpValue>=int(self.inputArray[i]):
                 cmpValue=int(self.inputArray[i])
              else:
                 returnValue=0
          Logging(self.eLogging).echo('rampdown returned %s' %(returnValue))
          return returnValue
      def Flat(self):
          returnValue=1
          cmpValue=int(self.inputArray[0])
          for i in range(self.sizeOfArray):
              if cmpValue!=int(self.inputArray[i]):
                 returnValue=0
          Logging(self.eLogging).echo('flat returned %s' %(returnValue))
          return returnValue

class __main__method__(multiprocessing.Process):
      def __init__(self, thresholdConnection, maximumConnections, pingInterval, portNumber, serverName, TCPFile, eflag):
          multiprocessing.Process.__init__(self)
          self.pingInterval=pingInterval
          self.portNumber=portNumber
          self.thresholdConnection=thresholdConnection
          self.maximumConnections=maximumConnections
          self.connectionsArray=[]
          self.eLogging=eflag
          self.serverName=serverName
          self.TCPFile=TCPFile
          self.utilObject=programUtility()
      def run(self):
          Logging(self.eLogging).echo('inside __main__ run()')
          while True:
                time.sleep(0.5)
                currentConnections=self.utilObject.pythonNetstat(self.portNumber, self.TCPFile)
                Logging(self.eLogging).echo('%s: current number of connections: %s' %(self.serverName, currentConnections))
                if int(currentConnections)>=self.thresholdConnection:
                   Logging(self.eLogging).echo('%s-Queue: %s' %(self.serverName, self.connectionsArray))
                   if len(self.connectionsArray)>=(self.maximumConnections):
                      Logging(self.eLogging).echo('reached maximum queue limit.')
                      time.sleep(self.pingInterval)
                      break
                   else:
                      self.connectionsArray.append(int(currentConnections))
                      Logging(self.eLogging).echo('captured %s, sleep for %s seconds...' %(currentConnections, self.pingInterval))
                      time.sleep(self.pingInterval)
                else:
                   Logging(self.eLogging).echo('below threshold: %s' %(currentConnections))
                   time.sleep(self.pingInterval)
                   break
          if (self.maximumConnections)==len(self.connectionsArray):
             aD=analyzeData(self.connectionsArray, self.eLogging)
             Logging(self.eLogging).echo('analyzing data..')
             if aD.Flat():
                Logging(self.eLogging).echo('detected: Flat')
                notifyAdmins(self.eLogging).sendAlerts('Flat', aD.standardDeviation(), self.connectionsArray, self.serverName)
             else:
                if aD.rampDown():
                   Logging(self.eLogging).echo('detected: Ramp-Down')
                   notifyAdmins(self.eLogging).sendAlerts('Ramp-Down', aD.standardDeviation(), self.connectionsArray, self.serverName)
                else:
                   if aD.rampUp():
                      Logging(self.eLogging).echo('detected: Ramp-Up')
                      notifyAdmins(self.eLogging).sendAlerts('Ramp-Up', aD.standardDeviation(), self.connectionsArray, self.serverName)
                   else:
                      Logging(self.eLogging).echo('detected: See-Saw')
                      notifyAdmins(self.eLogging).sendAlerts('See-Saw', aD.standardDeviation(), self.connectionsArray, self.serverName)

#-------------------------------------------------------------------------------------------------------------------#
# 3. Main.                                                                                                          #
#-------------------------------------------------------------------------------------------------------------------#

if __name__=="__main__":
   utilityModule=programUtility()
   master_config_file=os.path.dirname(os.path.abspath(__file__))+'/../config/almon.config'
   if not utilityModule.checkFileExistsAndReadable(master_config_file):
      print '[almon] can not access %s.' %(master_config_file)
      exit()
   configDirectory_os, configFileName_os = os.path.split(master_config_file)
   if not os.path.isdir(configDirectory_os+'/../temp'):
      print '[almon] can not access temp directory.'
      exit()
   configChangeFlagFileName=configDirectory_os+'/../temp/configchange.flag'
   while True:
         time.sleep(0.2);
         tempCSFlag=0;
         unCollectableObjects=gc.collect()
         currentCheckSum=utilityModule.getFileChecksum(master_config_file)
         if not utilityModule.checkFileExistsAndReadable(configChangeFlagFileName):
            tempCheckSum='a'
         else:
            tempCheckSum=utilityModule.readFileChecksum(configChangeFlagFileName)
         if tempCheckSum!=currentCheckSum:
            tempCSFlag=1
            utilityModule.setFileChecksum(configChangeFlagFileName, currentCheckSum)
            getConfigValues=readConfigFile()
            if not getConfigValues.enableTool():
               unCollectableObjects=gc.collect()
               print '[almon] gc complete, uncollectable %d objects.' %(unCollectableObjects)
               print '[almon] received shutdown flag from the config file.'
               exit()
            else:
               eLogging=getConfigValues.eLogging
               tConn=getConfigValues.getConnectionValues()[0]
               mxC=getConfigValues.getConnectionValues()[1]
               pgI=getConfigValues.getConnectionValues()[2]
               appServersPorts=getConfigValues.getConnectionValues()[3]
               tcpNetFile=getConfigValues.getConnectionValues()[4]
               suspendFile=configDirectory_os+'/../temp/'+getConfigValues.suspendFile
         if eLogging:
            logFileName=configDirectory_os+'/../logs/'+getConfigValues.logFile
            originalSTDOUT=sys.stdout
            logFileNameObject=open(logFileName, 'a', 0)
            sys.stdout=logFileNameObject
            if getConfigValues.debugGC:
               gc.set_debug(gc.DEBUG_STATS)
         if utilityModule.checkFileExistsAndReadable(suspendFile):
            suspendFileObject=open(suspendFile, 'r')
            for sLine in suspendFileObject:
                suspendTime=int(sLine)
                break
            suspendFileObject.close()
            Logging(eLogging).echo('alerts and monitoring are suspended!')
            Logging(eLogging).echo('sleep %s' %(suspendTime))
            time.sleep(suspendTime)
            try:
                  os.remove(suspendFile)
                  Logging(eLogging).echo('suspend lock cleared.')
                  Logging(eLogging).echo('alerts and monitoring will resume.')
            except OSError:
                  print "|ERROR| can not release the suspend lock!"
                  exit()
         if not utilityModule.checkFileExistsAndReadable(tcpNetFile):
            Logging(eLogging).echo('can not access %s.' %(tcpNetFile))
            print '[almon] can not access %s.' %(tcpNetFile)
            exit()
         else:
            Logging(eLogging).echo('%s' %(currentCheckSum))
            if tempCSFlag:
               Logging(eLogging).echo('the config file contents are changed, loading the new config file values...')
            Logging(eLogging).echo('updated temp %s.' %(tempCheckSum))
            Logging(eLogging).echo('reading config file: %s...' %(master_config_file))
            Logging(eLogging).echo('tConn:%s mx:%s p:%s appp:%s' %(tConn, mxC, pgI, appServersPorts))
            Logging(eLogging).echo('tcp file: %s' %(tcpNetFile))
            Logging(eLogging).echo('garbage collection completed [number of uncollectable objects %d].' %(unCollectableObjects))
            mtList=[]
            mtListFinal=[]
            activeApplicationServersQueue=multiprocessing.Queue()
            Logging(eLogging).echo('mulqueue activeApplicationServersQueue reset.')
            checkForZeroConnectionFlag=0
            for appServerPort in appServersPorts.split(','):
                appServerPort=appServerPort.replace(',','')
                mtList.append(__premain__method__(appServerPort, tcpNetFile, activeApplicationServersQueue, eLogging))
            map(lambda process_: process_.start(), mtList)
            map(lambda process_: process_.join(), mtList)
            processThreadNumber=0
            while not activeApplicationServersQueue.empty():
                  checkForZeroConnectionFlag=1
                  aServersList=[activeApplicationServersQueue.get() for process_ in mtList]
                  for monitorAppServer in aServersList:
                      if monitorAppServer!=0:
                         checkForZeroConnectionFlag=0
                         processThreadNumber+=1
                         appServerName=monitorAppServer.split(':')[0]
                         appServerPort=monitorAppServer.split(':')[1]
                         mtListFinal.append(__main__method__(tConn, mxC, pgI, appServerPort, appServerName, tcpNetFile, eLogging))
                         Logging(eLogging).echo('[process-%s] %s mul-process sequence initialized.' %(processThreadNumber, appServerName))
            map(lambda process__: process__.start(), mtListFinal)
            map(lambda process__: process__.join(), mtListFinal)
            if checkForZeroConnectionFlag:
               Logging(eLogging).echo('no connections reported by the application servers listed in config file.')
               Logging(eLogging).echo('sending email and text alerts to weboperations team')
               notifyAdmins(eLogging).sendAlerts('Zero-Connections', appServersPorts, [0], 'ZAlert')
               Logging(eLogging).echo('pausing for 3 minutes...')
               time.sleep(180)
         if eLogging:
            sys.stdout=originalSTDOUT
            logFileNameObject.close()
else:
   print '|ERROR| [almon] do not import.'
   exit()

#endMain
