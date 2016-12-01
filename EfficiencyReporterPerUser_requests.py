#!/usr/bin/python

import re
import sys
import requests


vo = "uboone"
cilogon_match = re.compile('.+CN=UID:(\w+)')
non_cilogon_match = re.compile('/CN=([\w\s]+)/?.+?')


def query():
    """Method to query Elasticsearch cluster for EfficiencyReport
    information"""
    # Gather parameters, format them for the query
    starttimeq = "2016-11-29T09:30:00Z"
    endtimeq = "2016-11-30T09:30:00Z"
    wildcardVOq = '*{}*'.format(vo)
    wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'
    queryargs = (wildcardVOq, wildcardProbeNameq, starttimeq, endtimeq)

    q = ' {{' \
        '   "size": 0,' \
        '   "query": {{' \
        '     "constant_score": {{' \
        '       "filter": {{ ' \
        '         "bool":{{' \
        '           "must": [ ' \
        '             {{"wildcard": {{"VOName": "{0}"}}}},' \
        '             {{"wildcard": {{"ProbeName":"{1}"}}}},' \
        '             {{"range": {{' \
        '               "EndTime": {{' \
        '                 "gte": "{2}",' \
        '                 "lt": "{3}"' \
        '               }}' \
        '             }}}},' \
        '             {{"range": {{' \
        '               "WallDuration" : {{"gt": 0}}' \
        '             }}}},' \
        '             {{"term": {{"Host_description": "GPGrid"}}}},' \
        '             {{"term": {{"ResourceType": "Payload"}}}}' \
        '           ]' \
        '         }}' \
        '       }}' \
        '     }}' \
        '   }},' \
        '   "aggs": {{' \
        '     "group_VOName": {{' \
        '       "terms": {{"field": "ReportableVOName"}},' \
        '       "aggs": {{' \
        '         "group_HostDescription": {{' \
        '           "terms": {{"field": "Host_description"}},' \
        '           "aggs": {{' \
        '             "group_CommonName": {{' \
        '               "terms": {{"field": "CommonName"}},' \
        '               "aggs": {{' \
        '                 "WallHours": {{' \
        '                   "sum": {{' \
        '                     "script": {{' \
        '                       "inline": "(doc[\'WallDuration\'].value*doc[\'Processors\'].value)/3600"' \
        '                     }}' \
        '                   }}' \
        '                 }},' \
        '                 "CPU_Duration_sec": {{' \
        '                   "sum": {{"field": "CpuDuration"}}' \
        '                 }}' \
        '               }}' \
        '             }}' \
        '           }}' \
        '         }}' \
        '       }}' \
        '     }}' \
        '   }}' \
        ' }}'.format(*queryargs)

    return q


def parseCN(cn):
    """Parse the CN to grab the email address and user"""
    m = cilogon_match.match(cn)      # CILogon certs
    if m:
        pass
    else:
        m = non_cilogon_match.match(cn)
    user = m.group(1)
    return user


def calc_eff(wallhours, cpusec):
    """Calculate the efficiency given the wall hours and cputime in seconds.  Returns percentage"""
    return round(((cpusec / 3600) / wallhours) * 100, 1)


def printline():
    """Coroutine to print each line to stdout"""
    print "{0}\t{1}\t{2}\t{3}%".format("User", "NJobs", "Wall Hours", "Efficiency (%)")
    while True:
        dn, njobs, wallhours, cputime = yield
        user = parseCN(dn)
        eff = calc_eff(wallhours, cputime)
        print "{0}\t{1}\t{2}\t{3}%".format(user, njobs, wallhours, eff)


def run_query():
    """Execute the query and check the status code before returning the response"""
    try:
        r = requests.post("https://gracc.opensciencegrid.org/q/gracc.osg.raw*/_search", data=query())
        r.raise_for_status()
        return r
    except Exception as e:
        print e
        sys.exit(1)


def run_report():
    """Takes data from query response and parses it to send to other functions for processing"""
    pline = printline()
    pline.send(None)

    r = run_query()
    rawdict = r.json()
    entries = (line for line in rawdict["aggregations"]["group_VOName"]["buckets"][0]
        ["group_HostDescription"]["buckets"][0]
        ["group_CommonName"]["buckets"])

    # The above works in this case.  If we had more than one VO, Host Description, etc, we'd need to do the following:
    # vos = (x for x in rawdict["aggregations"]["group_VOName"]["buckets"])
    # hostdescs = (x for vo in vos for x in vo["group_HostDescription"]["buckets"])
    # entries = (x for hd in hostdescs for x in hd["group_CommonName"]["buckets"])

    for entry in entries:
        if entry["WallHours"]["value"] > 1000:
            pline.send((entry["key"],   # DN
                        entry["doc_count"],  # NJobs
                        entry["WallHours"]["value"],    # Wall hours
                        entry["CPU_Duration_sec"]["value"]))    # CPU secs

    return

def main():
    run_report()


if __name__ == "__main__":
    main()

