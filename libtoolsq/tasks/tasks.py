from celery.task import task
from subprocess import check_output, CalledProcessError
import os
import json
import logging

from celeryconfig import PATH, LIBREPOTOOLS_ROOT_PATH, LIBREPOTOOLS_JAR_PATH
os.environ["PATH"] = PATH + os.pathsep + os.environ["PATH"]

logging.basicConfig(level=logging.INFO)

@task
def awsDissertation(
        data
    ):
    id = str(awsDissertation.request.id)

    jsonData = json.loads(data)
    collectionhandle = jsonData['collection']
    dspaceapiurl = jsonData['rest endpoint']

    with open('/working/data.json', 'w') as outfile:
        json.dump(jsonData, outfile)

    awsDissertationExec(id)

    safPath = getSafPathFromUserInputInfoFile(id, "aws-dissertation")

    importOutput = libtoolsjournalimport(id, safPath, collectionhandle, dspaceapiurl)

    return

def awsDissertationExec(
        id
    ):
    
    cmd_tmp = "java -jar " + LIBREPOTOOLS_JAR_PATH + " \'{0}\' \'aws-dissertation\' \'{{\"json\" : \"{1}\"}}\' "
    cmd = cmd_tmp.format(id, '/working/data.json')
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}
    print "aws resp = " + resp
    # if command returns just the path
    return resp

@task
def safPackageGenerationAndImport(
        csvPath, collectionhandle, dspaceapiurl
    ):

    id = str(safPackageGenerationAndImport.request.id)

    #### Generating the SAF package ####
    safPackageGen(id, csvPath)

    safpath = getSafPathFromUserInputInfoFile(id, "saf-build")

    print '\n safpath = ' + safpath + ' for safPackageGenerationAndImport id = ' + id

    """
        "Import the DSpace SAF package into DSpace repository"
    """
    path = libtoolsjournalimport(id, safpath, collectionhandle, dspaceapiurl)
    return str(path)


@task
def safPackageGeneration(csvPath, zipped="false"):

    """ Generate a SAF package using SAFBuilder for DSpace ingestion
    """
    id = str(safPackageGeneration.request.id)

    #### Generating the SAF package ####
    return safPackageGen(id, csvPath, zipped)
    

def safPackageGen(id, csvPath, zipped="false"):

    cmd_tmp = "java -jar " + LIBREPOTOOLS_JAR_PATH + " \'{0}\' \'saf-build\' \'{{\"csvPath\" : \"{1}\", \"zipped\" : \"{2}\"}}\' "
    cmd = cmd_tmp.format(id, csvPath, zipped)
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}
    print "resp = " + resp
    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]

@task
def runJournalTasks(
        publisher, startdate, enddate,
        collectionhandle, dspaceapiurl,
        affiliate="University of Oklahoma"
    ):
    """ Run the journals-search, journal-saf, and journal-import in one task
    """
    id = str(runJournalTasks.request.id)

    #### Search the article information ####
    journalSearchOutput = libtoolsjournalsearch(id,publisher, startdate, enddate, affiliate)

    jsonPath = os.path.join(LIBREPOTOOLS_ROOT_PATH, "dspace", "commandline", "journal-search", id, startdate+"_"+enddate+".json")
    jsonData = open(jsonPath, 'r').read()
    jsonObjList = json.loads(jsonData)
    dois = ""
    for obj in jsonObjList:
        dois += obj["doi"]+";"
    dois = dois[:-1]

    #### Generating the SAF package ####
    journalSafOutput = libtoolsjournalsaf(id, dois, startdate, enddate)
    
    #### Importing the SAF package ####
    safPath = getSafPathFromUserInputInfoFile(id, "journal-saf")

    importOutput = libtoolsjournalimport(id, safPath, collectionhandle, dspaceapiurl)
    
    return {'searchResults' : journalSearchOutput, 'dois' : dois, 'safPackagePath' : safPath}        

def libtoolsjournalsearch(
        id,publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    
    cmd_tmp = "java -jar " + LIBREPOTOOLS_JAR_PATH + " \'{0}\' \'journal-search\' \'{{\"publisher\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\", \"affiliate\" : \"{4}\"}}\' " 
    cmd = cmd_tmp.format(id, publisher, startdate, enddate, affiliate)
    #try:
    #    resp = check_output(cmd, shell=True)
    #except CalledProcessError as err:
    #    logging.error(err)
    #    logging.error(os.environ)
    #    return {"status": "error catched: {0}".format(err.returncode)}
    resp = check_output(cmd, shell=True)
    return resp

    # else if path is last line in stdout
#    return [line for line in resp.splitlines()][-1]

@task
def runjournalsearch(
        publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """
	Search journal articles in a data range based on the publisher, start date and end date
    """
    id = str(runjournalsearch.request.id)
    path = libtoolsjournalsearch(id,publisher, startdate, enddate, affiliate)
    return str(path)


def libtoolsjournalsaf(id, dois, startdate, enddate):
    cmd_tmp = "java -jar " + LIBREPOTOOLS_JAR_PATH + " \'{0}\' \'journal-saf\' \'{{\"dois\": \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\"}}\' "
    cmd = cmd_tmp.format(id, dois, startdate, enddate)
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}
    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]

@task
def runjournalsaf(
        dois, startdate, enddate
    ):
    """
        "Generate the DSpace SAF package for importing"
    """
    id = str(runjournalsaf.request.id)
    path = libtoolsjournalsaf(id, dois, startdate, enddate)
    return str(path)


def libtoolsjournalimport(id, safpath, collectionhandle, dspaceapiurl):
    cmd_tmp = "java -jar " + LIBREPOTOOLS_JAR_PATH + " \'{0}\' \'journal-import\' \'{{\"safPath\" : \"{1}\", \"collectionHandle\": \"{2}\", \"dspaceApiUrl\" : \"{3}\"}}\' "
    cmd = cmd_tmp.format(id, safpath, collectionhandle, dspaceapiurl)
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}
    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]

@task
def runjournalimport(
        safpath, collectionhandle, dspaceapiurl
    ):
    """
        "Import the DSpace SAF package into DSpace repository"
    """
    id = str(runjournalimport.request.id)
    path = libtoolsjournalimport(id, safpath, collectionhandle, dspaceapiurl)
    return str(path)

def getSafPathFromUserInputInfoFile(id, taskType):
    safPath = ""
    userInputInfoTxtPath = os.path.join(LIBREPOTOOLS_ROOT_PATH, "dspace", "commandline", taskType, id, "userInputInfo.txt")
    with open(userInputInfoTxtPath) as f:
        content = f.readlines()

    for line in content:
        if "safPath=" in line:
            safPath = line.split("=")[1].replace("\n","")
    if "[\"" in safPath:
        safPath = safPath.replace("[\"","")
    if "\"]" in safPath:
        safPath = safPath.replace("\"]","")

    return safPath
