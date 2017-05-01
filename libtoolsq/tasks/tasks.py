from celery.task import task
from subprocess import check_output, CalledProcessError
import os

os.environ["PATH"] = "/usr/local/Cellar/maven/3.3.9/libexec/bin" + os.pathsep + os.environ["PATH"]

def libtoolsjournalsearch(
        id, publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    cmd_tmp = "mvn exec:exec@journal-search -Ddata=\'{{\"journal-search\": {{\"id\" : \"{0}\", \"publisher\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\", \"affiliate\" : \"{4}\"}}}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
    cmd = cmd_tmp.format(id, publisher, startdate, enddate, affiliate)
    try:
        resp = check_output(cmd, shell=True)
    except CalledProcessError:
        return {"status": "error catched"}

    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]

@task
def runjournalsearch(
        id, publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """
	Search journal articles in a data range based on the publisher, start date and end date
    """
    path = libtoolsjournalsearch(id, publisher, startdate, enddate, affiliate)
    return str(path)


def libtoolsjournalsaf(id, dois, startdate, enddate):
    cmd_tmp = "mvn exec:exec@journal-search -Ddata=\'{{\"journal-saf\": {{\"id\" : \"{0}\", \"dois\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\"}}}}\' -f /Users/zhao0677/Projects/shareokdata/kernel-api/pom.xml"
    cmd = cmd_tmp.format(id, dois, startdate, enddate)
    try:
        resp = check_output()
    except CalledProcessError
        return {"status": "error catched"}
    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]

@task
def runjournalsaf(
        id, dois, startdate, enddate
    ):
    """"
        "Generate the DSpace SAF package for importing"
    """"
    path = libtoolsjournalsaf(id, dois, startdate, enddate)
    return str(path)
