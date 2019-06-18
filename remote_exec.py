#!/usr/bin/env python

#stdin, stdout, stderr = ssh.exec_command('python -u /var/tmp/test')
#for line in stdout:
#    print line.rstrip()

from subprocess import Popen, PIPE
import os

os.popen("scp test gtcrd-cblla01q:/var/tmp 2>/dev/null")
stdout, stderr = Popen(['ssh', '-q', 'gtcrd-cblla01q', 'python /var/tmp/test'], stderr=PIPE, stdout=PIPE).communicate()
print(stdout)
