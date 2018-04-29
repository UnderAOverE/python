# python

#-------------------------------------------------------------------------------------------------------------------#
#                                                                                                                   #
# Author        : r2d2c3p0.                                                                                         #
# Script Name   : startalmon.py.                                                                                    #
# Date of birth : 03/30/2015.                                                                                       #
# Explanation   : Trigger to start the almon for shell script.                                                      #
# Modifications : 03/30/2015 - Original version ($1.0$).                                                            #
# Dependencies  : 1. Python 2.6 or greater.                                                                         #
#                 2. almon.py.                                                                                      #
# Version       : 1.0                                                                                               #
# Contact       : r2d2c3p0                                                                                          #
#                                                                                                                   #
#-------------------------------------------------------------------------------------------------------------------#

import subprocess
import os

__author__="r2d2c3p0"
__version__="1.0v"

almoncmd="python", os.path.dirname(os.path.abspath(__file__))+"/almon.py" 
almonPID=subprocess.Popen(almoncmd)
print "|INFO| [startalmon] started almon with PID=", almonPID.pid

#end_startalmon.py
