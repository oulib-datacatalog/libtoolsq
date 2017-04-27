from celery.task import task
from subprocess import check_output, CalledProcessError


def libtooljournalpath(
        id, publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
#    cmd_tmp = "mvn exec:exec@journal-search -Ddata=\'{{\"journal-search\": {{\"id\" : \"{0}\", \"publisher\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\", \"affiliate\" : \"{4}\"}}}}\' -f ./kernal-api/pom.xml"
#    cmd = cmd_tmp.format(id, publisher, startdate, enddate, affiliate)
#    try:
#        resp = check_output(cmd, shell=True)
#    except CalledProcessError:
#        return {"status": "error catched"}
    cmd_tmp = "mvn -v"
    resp = check_output(cmd, shell=True)
    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]


def processjournal(path):
    return str(path)


@task
def runall(
        id, publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """
	test string
    """
    path = libtooljournalpath(id, publisher, startdate, enddate, affiliate)
    return str(path)
