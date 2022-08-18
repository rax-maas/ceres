import subprocess
import argparse
import json
import time
from math import floor

#Coloritos!!!!
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    RESETALL = '\033[0m'

### Sum a list of logicial expresions 
def logocialSum(logicals):
    result = True
    for i in logicals:
        result = result and i
    return result

### Get the status of the pods return true if all containers are up
def getContainerStatus(pod):
    readies = []
    containers = pod["status"]["containerStatuses"] 
    for container in containers:
        if container["ready"]:
            print ("  {}: {}Ready{}".format(container["name"], bcolors.OKGREEN, bcolors.RESETALL))
            readies.append(True)
        else:
            print ("  {}: {}Not ready{}".format(container["name"], bcolors.WARNING, bcolors.RESETALL))
            readies.append(False)
    
    if len(readies) > 1:

        return logocialSum(readies)
    else:
        return readies[0]

def getAllPodStatus(pods):
    podsReady = []
    for pod in pods:
        print("{}{}:{}".format(bcolors.BOLD, pod["metadata"]["name"], bcolors.RESETALL))
        ready = getContainerStatus(pod)
        podsReady.append(ready)

    return logocialSum(podsReady)

parser=argparse.ArgumentParser()

parser.add_argument('-l', '--label', help='label name', required=True)
parser.add_argument('-i', '--interval', help='time between tests, default is 5 secounds', default="5")
parser.add_argument('-t', '--timeout', help='test duration before exiting, defualt is 5 minutes', default="300")
parser.add_argument('-s', '--selector', help='filter selector/label, default is app', default="app")

args=parser.parse_args()

label=args.label
selector=args.selector
interval=int(args.interval)
timeout=int(args.timeout)

totalTries=floor(timeout/interval)

if totalTries <= 0:
    print ("timeout must be equal or higher that interval")
    exit(1)

print (f"Will try a total of {totalTries} times, each {interval} seconds")
allReady = False
for tryIt in range(totalTries):
    print (f"Try {tryIt + 1}/{totalTries}:")
    cmd = ["kubectl", "get", "pods", f"--selector={selector}={label}", "-o", "json"]
    podsProcess= subprocess.run(cmd, stdout=subprocess.PIPE)
    podsText = podsProcess.stdout.decode('utf-8')

    if podsProcess.returncode != 0:
        print (podsText = podsProcess.stdout.decode('utf-8'))
        exit(1)

    podsJSON = json.loads(podsText)
    pods = podsJSON["items"]

    if (len(pods) == 0):
        print("No pods found, exiting process")
        exit(1)
    else:
        print (f"Testins {len(pods)} Pods:")
        if getAllPodStatus(pods):
            allReady = True

    if allReady:
        break

    print ("Waiting all pods get ready...\n")
    time.sleep(interval)

if allReady:
    print ("All pods ready!!!")
    print ("Have a nice day.")
else:
    print(f"Pods not ready in the given time({timeout})")
    print(f"A total of {bcolors.BOLD}{totalTries}{bcolors.RESETALL} tries {bcolors.FAIL}failed{bcolors.RESETALL}")
    exit (1)