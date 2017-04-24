from celery.task import task
from subprocess import check_output


def libtooljournalpath(
        id, publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    cmd_tmp = "mvn exec:exec@journal-search -Ddata=\'{{\"journal-search\": {{\"id\" : \"{0}\", \"publisher\" : \"{1}\", \"startDate\": \"{2}\", \"endDate\" : \"{3}\", \"affiliate\" : \"{4}\"}}}}\' -f ./kernel-api/pom.xml"

    cmd = cmd_tmp.format(id, publisher, startdate, enddate, affiliate)

    resp = check_output(cmd, shell=True)

    # if command returns just the path
    return resp

    # else if path is last line in stdout
    return [line for line in resp.splitlines()][-1]


def processjournal(path):
    with open(path, "r") as f:
        return f.read()



@task
def runall(
        id, publisher, startdate, enddate,
        affiliate="University of Oklahoma"
    ):
    path = libtooljournalpath(id, publisher, startdate, enddate, affiliate)
    return processjournal(path)
