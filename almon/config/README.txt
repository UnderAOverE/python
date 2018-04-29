Date: 03/30/2015
Version: 2.0
Tool: almon.py
                                 ----------------------------------------------------------------------
                                      almon: alerts and monitoring tool for operations team.
                                 ----------------------------------------------------------------------

* Tool is developed to replace the exiting watchapps monitoring, to alert once every interval over selected time period.
* Below are the directories and files required for the tool to work:
    * almon
          -config = Directory, which has the config file required by the master and wrapper scripts. Please see below for more information.
                     Also contains all the administators email files which have the email and the mobile number for alerting.
              -ooo = The directory which houses the administrators email files who are on OOO.
              -egg = libraries and python modules.
          -py = Python directory for master python script, almon.py.
                startalmon.py = fire and forget script for almon.py.
          -logs = logs directory.
          -temp = Required for operations like suspending, disabling and enabling on the fly.
          -almon.sh = The wrapper to manage the master python script.
                       Run '-help' to print usage information for the script.

* The wrapper script almon.sh can be used to manage the operations and also alerting modes. This script is not essential for the tool to perform.

* almon.config.
    * Alerting parameters: 
             enable.logging=option to enable(1) or disable(0) logging.
             run.almon=enable(1) or disable(0) almon.
             logfile.name=log filename.
             enable.gcdebug=verbose gc option - WARNING: turn on only to debug memory leaks. Best practice is to disable this option.
    * Email settings:
             sender.address=host email address.
    * Connection settings:
             threshold.value=the highest connections value to start capturing.
             maximum.connections=maximum connections to monitor before alerting.
             ping.interval=interval between the high connections to pause.
             applicationservers.ports=pair of application servername and the TCP port to monitor. 
                                      ApplicationServername is not important, required only for informational purpose and to identify the server.
             tcp.filename=unix net tcp file.

* Note: Make sure latest version is running. The tool do not run any of the unix based network tools, it monitors the net tcp file for the number fo connections. Tool also has lot of checks in place to run smoothly and efficiently.

Read config files -> Load the options -> check for the active member -> high connection >=20 selected -> monitor >=threshold.value -> queue up -> pause ping.interval -> queue size > maximum.connections -> send alerts. Repeat the cycle. If during the capturing period if number of connections go below threshold.value the queue is discarded.

# end.