from celery.task import task
from subprocess import check_output, CalledProcessError
import os
import json

os.environ["PATH"] = "/usr/local/Cellar/maven/3.3.9/libexec/bin" + os.pathsep + os.environ["PATH"]
LIBREPOTOLLS_ROOT_PATH = "/var/local/librepotools/librepotools-data"

@task
def runJournalTasks(
        publisher, startdate, enddate,
        affiliate="Universitoy of Oklahoma",
        collectionhandle, dspaceapiurl
    ):
    """ Run the journals-search, journal-saf, and journal-import in one task
    """
    journalSearchOutput = libtoolsjournalsearch(publisher, startdate, enddate, affiliate)

    jsonPath = os.path.join(LIBREPOTOLLS_ROOT_PATH, "dspace", "commandline", "journal-search", id, startdate+"_"+enddate+".json")
    jsonData = open(jsonPath, 'r').read()
    jsonObjList = json.loads(jsonData)
    for obj in jsonObjList:
        doi += obj["doi"]+";"
    doi = doi[:-1]
    return doi        

def libtoolsjournalsearch(
        publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    id = str(runjournalsearch.request.id)
    cmd_tmp = "mvn exec:exec@journal-search -DtaskId=\'{0}\' -DtaskType=\'journal-search\' -Ddata=\'{{\"publisher\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\", \"affiliate\" : \"{4}\"}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
    cmd = cmd_tmp.format(id, publisher, startdate, enddate, affiliate)
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}

    return resp, test_results

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
    path = libtoolsjournalsearch(publisher, startdate, enddate, affiliate)
    return str(path)


def libtoolsjournalsaf(id, dois, startdate, enddate):
    cmd_tmp = "mvn exec:exec@journal-saf -Ddata=\'{{\"journal-saf\": {{\"id\" : \"{0}\", \"dois\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\"}}}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
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
    cmd_tmp = "mvn exec:exec@journal-import -Ddata=\'{{\"journal-import\": {{\"id\" : \"{0}\", \"safPath\" : \"{1}\", \"collectionHandle\": \"{2}\", \"dspaceApiUrl\" : \"{3}\"}}}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
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

def test():
    return "This is a simple test"

