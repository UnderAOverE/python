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


--------- to-do

# first of all import the socket library 
import socket			 

# next create a socket object 
s = socket.socket()		 
print "Socket successfully created"

# reserve a port on your computer in our 
# case it is 12345 but it can be anything 
port = 12345				

# Next bind to the port 
# we have not typed any ip in the ip field 
# instead we have inputted an empty string 
# this makes the server listen to requests 
# coming from other computers on the network 
s.bind(('', port))		 
print "socket binded to %s" %(port) 

# put the socket into listening mode 
s.listen(5)	 
print "socket is listening"			

# a forever loop until we interrupt it or 
# an error occurs 
while True: 

# Establish connection with client. 
c, addr = s.accept()	 
print 'Got connection from', addr 

# send a thank you message to the client. 
c.send('Thank you for connecting') 

# Close the connection with the client 
c.close() 

#end_firewallCheck.py
