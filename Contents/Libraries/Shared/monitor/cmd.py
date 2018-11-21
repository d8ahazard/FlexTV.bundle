import json
import subprocess
import logging
from time import sleep

log = logging.getLogger(__name__)


def run_command(command, single=True):
    results = []
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    line = p.stdout.readline()
    while line:
        if line.strip() is not "":
            results.append(line.strip())
        line = p.stdout.readline()
    while p.poll() is None:
        sleep(.1)
    # Empty STDERR buffer
    err = p.stderr.read()
    if p.returncode != 0:
        results = ["Error: " + str(err)]
    log.debug("Returning: '%s'" % json.dumps(results))
    return results
