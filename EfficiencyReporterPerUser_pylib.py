#!/usr/bin/python

import re
import sys

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


vo = "uboone"
cilogon_match = re.compile('.+CN=UID:(\w+)')
non_cilogon_match = re.compile('/CN=([\w\s]+)/?.+?')


def establish_client():
    """Initialize and return the elasticsearch client"""
    client = Elasticsearch(['https://gracc.opensciencegrid.org/q'],
                           use_ssl=True,
                           verify_certs=False,
                           timeout=60)
    return client


def query():
    """Method to query Elasticsearch cluster for EfficiencyReport
    information"""
    # Gather parameters, format them for the query
    indexpattern = 'gracc.osg.raw-2016*'
    starttimeq = "2016-11-29T09:30:00Z"
    endtimeq = "2016-11-30T09:30:00Z"
    wildcardVOq = '*' + vo + '*'
    wildcardProbeNameq = 'condor:fifebatch?.fnal.gov'

    # Elasticsearch query and aggregations
    s = Search(using=establish_client(), index=indexpattern) \
            .query("wildcard", VOName=wildcardVOq) \
            .query("wildcard", ProbeName=wildcardProbeNameq) \
            .filter("range", EndTime={"gte": starttimeq, "lt": endtimeq}) \
            .filter("range", WallDuration={"gt": 0}) \
            .filter("term", Host_description="GPGrid") \
            .filter("term", ResourceType="Payload")[0:0]
    # Size 0 to return only aggregations

    # Bucket aggs
    Bucket = s.aggs.bucket('group_VOName', 'terms', field='ReportableVOName') \
        .bucket('group_HostDescription', 'terms', field='Host_description') \
        .bucket('group_CommonName', 'terms', field='CommonName')

    # Metric aggs
    Bucket.metric('WallHours', 'sum',
                script="(doc['WallDuration'].value*doc['Processors'].value)/3600") \
        .metric('CPUDuration_sec', 'sum', field='CpuDuration')
    return s


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


def runquery():
    """Execute the query and check the status code before returning the response"""
    try:
        response = query().execute()
        if not response.success():
            raise
        results = response.aggregations
        return results
    except Exception as e:
        print e, "Error accessing Elasticsearch"
        sys.exit(1)


def run_report():
    """Takes data from query response and parses it to send to other functions for processing"""
    pline = printline()
    pline.send(None)

    results = runquery()
    cns = (cn for cn in results.group_VOName.buckets[0]
        .group_HostDescription.buckets[0]
        .group_CommonName.buckets)

    # In this particular case, we're lucky that the top two buckets only have one item.  Otherwise we'd have
    # to do the following:
    # vos = (vo for vo in results.group_VOName.buckets)
    # hostdesc = (hd for vo in vos for hd in vo.group_HostDescription.buckets)
    # cns = (cn for hd in hostdesc for cn in hd.group_CommonName.buckets)

    for cn in cns:
        if cn.WallHours.value > 1000:
            pline.send((cn.key, cn.doc_count, cn.WallHours.value, cn.CPUDuration_sec.value))


def main():
    run_report()


if __name__ == "__main__":
    main()

