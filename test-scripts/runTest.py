# test run script
# Will run a test set in test-plan yaml file
# Will try to load test scripts from local, other wise will try to download from artifactory
# Each test in the test set must have a name, type(python, java...), and argLine(arguments)
# Artifactory url and repository must be specified in the test-plan yaml
# Must configure k8s cluster before run, to be able to load secrets and config maps

import yaml
import argparse
import subprocess
import os
import requests
import json
import base64
import copy

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

def runProcess(cmd, env):
    Process = subprocess.run(cmd, stdout=subprocess.PIPE, env=env)
    Text = Process.stdout.decode('utf-8')
    print (Text)
    
    if Process.returncode == 0:
        return True
    else:
        return False

def runProcessOutput(cmd):
    Process = subprocess.run(cmd, stdout=subprocess.PIPE)
    Text = Process.stdout.decode('utf-8')
    
    if Process.returncode == 0:
        return Text

def removeLastSlash(string):
    length = len(string)
    if string[length - 1] == "/":
        return string[:-1]
    else:
        return string

#Check if the file exist in local, if not downlod it from artifactory 
def fileCheck(localFile, fileName, type):
    if not os.path.isfile(localFile):
        print ("File not in local path...")
        if not Artifactory.skip:
            url = removeLastSlash(Artifactory.url)
            print ("Downloading from Artifactory")
            ArtifactoryURL = url + "/" + Artifactory.repository + "/" + type + "/" + fileName
            try:
                response = requests.get(ArtifactoryURL)
            except:
                print ("Can't connect to artifactory server")
                return False

            if response.status_code == 200:
                open(localFile, "wb").write(response.content)
            else: 
                print ("Test file not found in artifactory")
                return False
        else:
            return False
    return True

def runTest(testName, arguments, testDirectory, rawTestType, environment):
    env = loadEnv(environment)
    fileName = ""
    cmd = []
    if rawTestType == "python" or rawTestType == "py":
        cmd = ["python"]
        testType = "python"
        fileName = testName + ".py"
    if rawTestType == "java" or rawTestType =="jar":
        cmd = ["java", "-jar"]
        testType = "java"
        fileName = testName + ".jar"
    if rawTestType == "ruby" or rawTestType == "rb":
        cmd = ["ruby"]
        testType = "ruby"
        fileName = testName + ".rb"

    if rawTestType not in suportedTypes.supported:
        print ("{}: {} type not suported for test".format(testName, rawTestType))
        print ("Suported types:")
        for appType in suportedTypes.supported:
            print (f"  {appType}")
        return {"name":testName, "result":"Not Supported"}

    localFile = f"{testDirectory}/{fileName}"

    if not fileCheck(localFile, fileName, testType):
        return {"name":testName, "result":"Not Found"}

    cmd.append(localFile)
    
    argumentsList = arguments.split()
    cmd.extend(argumentsList)
    
    print(f"Starting test: {bcolors.BOLD}{testName}{bcolors.RESETALL}\n")
    if runProcess(cmd, env):
        result = "Pass"
    else:
        result = "Failed"
    
    return {"name":testName, "result":result}

def loadEnv(environment):
    newEnv = copy.deepcopy(os.environ)
    variables = environment.envVars
    configMaps = environment.configMaps
    secrets = environment.secrets
    files = environment.files

    for vars in variables:
        keys= list(vars.keys())
        for key in keys:
            newEnv[key]=vars[key]

    for config in configMaps:
        cmd = ["kubectl", "get", "configmaps", config, "-o" "json"]
        configMapsJSON = json.loads(runProcessOutput(cmd))
        data = configMapsJSON["data"]
        keys= list(data.keys())
        for key in keys:
             newEnv[key]=data[key]

    for secret in secrets:
        cmd = ["kubectl", "get", "secrets", secret, "-o" "json"]
        secretsJSON = json.loads(runProcessOutput(cmd))
        data = secretsJSON["data"]
        keys= list(data.keys())
        for key in keys:
            try:
                newEnv[key]=base64.b64decode(data[key]).decode("utf-8")
            except:
                print (f"can't load variable: {key}")

    for file in files:
        cmd = ["kubectl", "get", "secrets", file["k8s-secret"], "-o" "json"]
        
        secretsJSON = json.loads(runProcessOutput(cmd))
        data = secretsJSON["data"]
        keys= list(data.keys())
        
        f = open(file["fileName"], "wb")
        f.write(base64.b64decode(data[file["name"]]))
        f.close()
        
        if 'varValue' in file:
            newEnv[file["varName"]] = file["varValue"]
        else:
            newEnv[file["varName"]] = file["fileName"]

    return newEnv


class EnvironmentArgs:
    def __init__(self, envVars, configMaps, secrets, files):
        self.envVars=envVars
        self.configMaps=configMaps
        self.secrets=secrets
        self.files=files

class Artifactory:
    url = ""
    repository = ""
    skip = True

class suportedTypes:
    supported = ['python', 'java', 'ruby', 'py', 'jar', 'rb']

def main():
    parser=argparse.ArgumentParser()

    parser.add_argument('-l', '--list', help='Test list file path', default="./test-plan.yml" )
    parser.add_argument('-t', '--test_set', help='Test set to be run', required=True)
    parser.add_argument('-d', '--test_directory', help='local test directory', default=".")

    args=parser.parse_args()

    listPath = args.list
    testSet = args.test_set
    testDirectory=args.test_directory

    alltestSet = {}
    with open(listPath) as file:
        alltestSet = yaml.load(file, Loader=yaml.FullLoader)

    if "Artifactory" in alltestSet:
        Artifactory.url = alltestSet["Artifactory"]["url"]
        Artifactory.repository = alltestSet["Artifactory"]["repository"]
        Artifactory.skip = False

    SummaryList = []

    for test in alltestSet[testSet]:
        if "env" in test:
            envVars = envVars=test["env"]
        else:
            envVars = []

        if "k8sConfigMap" in test:
            configMaps =  configMaps=test["k8sConfigMap"]
        else:
            configMaps = []

        if "k8sSecrets" in test:
            secrets = secrets=test["k8sSecrets"]
        else:
            secrets = []

        if "files" in test:
            files = test["files"]
        else: 
            files = []
            
        environment = EnvironmentArgs(envVars=envVars, configMaps=configMaps, secrets=secrets, files=files)
        SummaryList.append(runTest(testName=test["name"], arguments=test["argsLine"], testDirectory=testDirectory, rawTestType=test["type"], environment=environment))
    
    outcome = []
    print ("Test run completed")
    print (bcolors.HEADER + bcolors.BOLD + "Summary:" + bcolors.RESETALL)

    for test in SummaryList:
        printable = ""
        if test["result"] == "Pass":
            printable = bcolors.OKGREEN + test["result"]
            outcome.append(True)
        else:
            printable = bcolors.FAIL + test["result"]
            outcome.append(False)

        print (" {}: {}{}".format(test["name"], printable, bcolors.RESETALL))
    
    if not logocialSum(outcome):
        exit(1)

if __name__ == "__main__":
    main()