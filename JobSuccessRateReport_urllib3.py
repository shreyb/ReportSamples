#!/usr/bin/python

import re
import sys
from ast import literal_eval
import json
import urllib3

num_successes = 0
num_failures = 0


def prod_success_rate():
    global num_failures, num_successes
    while True:
        exitcode = yield
        if not literal_eval(exitcode):      # Exit code 0 is a success
            num_successes += 1
        else:
            num_failures += 1


def generate_result_array():
    # Compile results into array

    usermatch_CILogon = re.compile('.+CN=UID:(\w+)')
    usermatch_FNAL = re.compile('.+/(\w+\.fnal\.gov)')
    globaljobparts = re.compile('\w+\.(fifebatch\d\.fnal\.gov)#(\d+\.\d+)#.+')
    realhost_pattern = re.compile('\s\(primary\)')

    psr_calc = prod_success_rate()
    psr_calc.send(None)

    while True:
        hit = yield
        hit = hit['_source']
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
            print outstr
        except KeyError:
            # We want to ignore records where one of the above keys isn't listed in the ES document.
            # This is consistent with how the old MySQL report behaved.
            pass


def query():
    """Method that actually queries elasticsearch"""
    # Set up our search parameters
    voq = '/fermilab/nova/Role=Production/Capability=NULL'
    productioncheck = '*Role=Production*'

    starttimeq = '2016-11-28T00:00Z'
    endtimeq = '2016-11-29T00:00Z'

    # Elasticsearch query

    prodcheck = True

    if prodcheck:
        q = '{{"query": {{"bool": {{"filter": [' \
            '{{"range": ' \
            '{{"EndTime": {{"lt": "{0}", "gte": "{1}"}}}}' \
            '}},' \
            '{{"term": {{"ResourceType": "Payload"}}}},' \
            '{{"term": {{"VOName": "{2}"}}}}' \
            '],' \
            '"must": [{{"wildcard": {{"VOName": "{3}"}}}}]' \
            '}}}},' \
            '"sort": ["_doc"],' \
            '"size": 1000}}'.format(endtimeq, starttimeq, voq, productioncheck)
        # Note - need to double the { and } to use these and .format
    else:
        q = '{{"query": {{"bool": {{"filter": [' \
            '{{"range": ' \
            '{{"EndTime": {{"lt": "{0}", "gte": "{1}"}}}}' \
            '}},' \
            '{{"term": {{"ResourceType": "Payload"}}}},' \
            '{{"term": {{"VOName": "{2}"}}}}' \
            ']' \
            '}}}}}}'.format(endtimeq, starttimeq, voq)

    return q


def getdata():
    resultarray = generate_result_array()
    resultarray.send(None)
    # Get first scroll id
    try:
        http = urllib3.PoolManager()
        r = http.request('POST', 'https://gracc.opensciencegrid.org/q/gracc.osg.raw*/_search?search_type=scan&scroll=1m', body=query(),
        headers={'Content-Type': 'application/json'})
    except Exception as e:
        print e
        sys.exit(1)
    scroll_id = json.loads(r.data)['_scroll_id']
    while True:
        try:
            r = http.request('POST','https://gracc.opensciencegrid.org/q/_search/scroll?scroll=1m', body=scroll_id)
        except Exception as e:
            print e
            sys.exit(1)
        # Get the next scroll ID
        data = json.loads(r.data)
        scroll_id = data['_scroll_id']
        for hit in data['hits']['hits']:
            resultarray.send(hit)
        if not data['hits']['hits']:
            break
    return


def printsuccessrate():
    global num_successes, num_failures
    print "The job success rate in this time period is {0}% for {1} jobs.".format(round(float(num_successes)/(num_successes + num_failures) * 100, 1),
                                                                                  num_failures + num_successes)
    return


def main():
    getdata()
    printsuccessrate()

if __name__ == '__main__':
    main()

