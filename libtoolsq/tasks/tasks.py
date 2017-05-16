from celery.task import task
from subprocess import check_output, CalledProcessError
import os
import json

os.environ["PATH"] = "/usr/local/Cellar/maven/3.3.9/libexec/bin" + os.pathsep + os.environ["PATH"]
LIBREPOTOLLS_ROOT_PATH = "/var/local/librepotools/librepotools-data"

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

    jsonPath = os.path.join(LIBREPOTOLLS_ROOT_PATH, "dspace", "commandline", "journal-search", id, startdate+"_"+enddate+".json")
    jsonData = open(jsonPath, 'r').read()
    jsonObjList = json.loads(jsonData)
    dois = ""
    for obj in jsonObjList:
        dois += obj["doi"]+";"
    dois = dois[:-1]

    #### Generating the SAF package ####
    journalSafOutput = libtoolsjournalsaf(id, dois, startdate, enddate)
    
    #### Importing the SAF package ####
    safPath = os.path.join(LIBREPOTOLLS_ROOT_PATH, "dspace", "commandline", "journal-saf", id, "userInputInfo.txt")
    with open(jsonPath) as f:
        content = f.readlines()

    for line in content:
        if "safPath=" in line:
            safPath = line.split("=")[1]

    libtoolsjournalimport(id, safPath, collectionhandle, dspaceapiurl)

    return {'searchResults' : journalSearchOutput, 'dois' : dois, 'safPackagePath' : safPath}        

def libtoolsjournalsearch(
        id,publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    
    cmd_tmp = "mvn exec:exec@journal-search -DtaskId=\'{0}\' -DtaskType=\'journal-search\' -Ddata=\'{{\"publisher\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\", \"affiliate\" : \"{4}\"}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
    cmd = cmd_tmp.format(id, publisher, startdate, enddate, affiliate)
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}

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
    cmd_tmp = "mvn exec:exec@journal-saf -DtaskId=\'{0}\' -DtaskType=\'journal-saf\' -Ddata=\'{{\"dois\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\"}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
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
        id, dois, startdate, enddate
    ):
    """
        "Generate the DSpace SAF package for importing"
    """
    path = libtoolsjournalsaf(id, dois, startdate, enddate)
    return str(path)


def libtoolsjournalimport(id, safpath, collectionhandle, dspaceapiurl):
    cmd_tmp = "mvn exec:exec@journal-import -DtaskId=\'{0}\' -DtaskType=\'journal-search\' -Ddata=\'{{\"safPath\" : \"{1}\", \"collectionHandle\": \"{2}\", \"dspaceApiUrl\" : \"{3}\"}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
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
        id, safpath, collectionhandle, dspaceapiurl
    ):
    """
        "Import the DSpace SAF package into DSpace repository"
    """
    path = libtoolsjournalimport(id, safpath, collectionhandle, dspaceapiurl)
    return str(path)

