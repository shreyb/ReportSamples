#!/usr/bin/python

import re
from ast import literal_eval

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

num_successes = 0
num_failures = 0


def establish_client():
    """Initialize and return the elasticsearch client"""
    client = Elasticsearch(['https://gracc.opensciencegrid.org/q'],
                           use_ssl=True,
                           timeout=60)
    return client


def query():
    """Method that actually queries elasticsearch"""
    # Set up our search parameters
    voq = '/fermilab/nova/Role=Production/Capability=NULL'
    productioncheck = '*Role=Production*'

    starttimeq = '2016-11-28T00:00Z'
    endtimeq = '2016-11-29T00:00Z'

    # Elasticsearch query
    resultset = Search(using=establish_client(), index='gracc.osg.raw-2016.11') \
        .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
        .filter("term", ResourceType="Payload")

    prodcheck = True

    if prodcheck:
        resultset = resultset.query("wildcard", VOName=productioncheck)\
            .filter("term", VOName=voq)
    else:
        resultset = resultset.query("wildcard", VOName=voq)

    resultset = resultset[:500] # Give us pages of size 500 records

    return resultset


def prod_success_rate():
    global num_failures, num_successes
    while True:
        exitcode = yield
        if not literal_eval(exitcode):      # Exit code 0 is a success
            num_successes += 1
        else:
            num_failures += 1


def generate_result_array(resultset):
    # Compile results into array

    usermatch_CILogon = re.compile('.+CN=UID:(\w+)')
    usermatch_FNAL = re.compile('.+/(\w+\.fnal\.gov)')
    globaljobparts = re.compile('\w+\.(fifebatch\d\.fnal\.gov)#(\d+\.\d+)#.+')
    realhost_pattern = re.compile('\s\(primary\)')

    psr_calc = prod_success_rate()
    psr_calc.send(None)

    for hit in resultset.scan():
        try:
            # Parse userid
            try:
                # Grabs the first parenthesized subgroup in the hit['CommonName'] string, where that subgroup comes
                # after "CN=UID:"
                userid = usermatch_CILogon.match(hit['CommonName']).group(1)
            except AttributeError:
                try:
                    userid = usermatch_FNAL.match(hit['CommonName']).group(1)  # If this doesn't match CILogon standard, just grab the *.fnal.gov string at the end
                except AttributeError:
                    userid = hit['CommonName']  # Just print the CN string, move on
            # Parse jobid
            try:
                # Parse the GlobalJobId string to grab the cluster number and schedd
                jobparts = globaljobparts.match(hit['GlobalJobId']).group(2, 1)
                # Put these together to create the jobid (e.g. 123.0@fifebatch1.fnal.gov)
                jobid = '{0}@{1}'.format(*jobparts)
            except AttributeError:
                jobid = hit['GlobalJobId']  # If for some reason a probe gives us a bad jobid string, just keep going
            realhost = realhost_pattern.sub('', hit['Host'])  # Parse to get the real hostname

            psr_calc.send(hit['Resource_ExitCode'])

            outstr = '{starttime}\t{endtime}\t{CN}\t{JobID}\t{hostdescription}\t{host}\t{exitcode}'.format(
                starttime=hit['StartTime'],
                endtime=hit['EndTime'],
                CN=userid,
                JobID=jobid,
                hostdescription=hit['Host_description'],
                host=realhost,
                exitcode=hit['Resource_ExitCode']
            )
            yield outstr
        except KeyError:
            # We want to ignore records where one of the above keys isn't listed in the ES document.
            # This is consistent with how the old MySQL report behaved.
            pass


def printsuccessrate():
    global num_successes, num_failures
    print "The job success rate in this time period is {0}% for {1} jobs.".format(round(float(num_successes)/(num_successes + num_failures) * 100, 1),
                                                                                  num_failures + num_successes)
    return


def main():
    resultset = query()  # Generate Search object for ES
    response = resultset.execute()  # Execute that Search
    return_code_success = response.success()  # True if the elasticsearch query completed without errors

    if not return_code_success:
        raise Exception('Error accessing ElasticSearch')

    for line in generate_result_array(resultset):
        print line

    printsuccessrate()

if __name__ == '__main__':
    main()

