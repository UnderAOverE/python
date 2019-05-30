# /usr/bin/python

#
#
# name: firewallCheck.py
# dob: 05/29/2019
# r2d2c3p0
# v1.0.0
#
#
#

# imports
import socket
import sys  

# global variables
programName = sys.argv[0]
hostName = sys.argv[1]
portNumber = sys.argv[2]

# main.
try: 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    print "socket successfully created"
	try: 
		hostIP = socket.gethostbyname(hostName) 
		s.connect((hostIP, portNumber)) 
		print "successfully connected to %s(%s) at %s" %(hostName, hostIP, portNumber)
	except socket.gaierror: 
		print "there was an error resolving the host"
		sys.exit() 
	#EndTryExcept
except socket.error as err: 
    print "socket creation failed with error %s" %(err) 
#EndTryExcept

#end_firewallCheck.py
