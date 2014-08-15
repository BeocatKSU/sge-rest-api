#!/usr/bin/env python

import subprocess
from lxml import etree
import copy

#import sgerestapi.config

class SGE_qstat(object):
    @staticmethod
    def _merge_jobs(orig, new):
        job = copy.deepcopy(orig)
        if not isinstance(new['master_queue'], bool):
            job['master_queue'] = new['master_queue']
        if 'queues' in job.keys() and 'queues' in new.keys():
            job['queues'] = set(job['queues'])
            new['queues'] = set(new['queues'])
            job['queues'].update(new['queues'])
            job['queues'] = list(job['queues'])
        if 'hosts' in new.keys():
            for host, s in new['hosts'].items():
                if host not in job['hosts'].keys():
                    job['hosts'][host] = 0
                job['hosts'][host] += s
        if 'slots' in orig.keys() and 'slots' in new.keys():
            job['slots'] += new['slots']
        return job
    @staticmethod
    def _parse_job_info(item):
        temp_jobs = {}
        for i in item.iterchildren():
            if i.tag == 'queue_info':
                for job in SGE_qstat._parse_queue_info(i):
                    k = '{}.{}'.format(job['jobid'], job['taskid'])
                    if k in temp_jobs.keys():
                        temp_jobs[k] = SGE_qstat._merge_jobs(temp_jobs[k], job)
                    else:
                        temp_jobs[k] = job
            elif i.tag == 'job_info':
                for job in SGE_qstat._parse_job_info(i):
                    k = '{}.{}'.format(job['jobid'], job['taskid'])
                    if k in temp_jobs.keys():
                        temp_jobs[k] = SGE_qstat._merge_jobs(temp_jobs[k], job)
                    else:
                        temp_jobs[k] = job
            elif i.tag == 'job_list':
                for job in SGE_qstat._parse_job_list(i):
                    k = '{}.{}'.format(job['jobid'], job['taskid'])
                    if k in temp_jobs.keys():
                        temp_jobs[k] = SGE_qstat._merge_jobs(temp_jobs[k], job)
                    else:
                        temp_jobs[k] = job
        for k in temp_jobs.keys():
            if temp_jobs[k]['state'] == 'running':
                if 'pe' in temp_jobs[k].keys() and temp_jobs[k]['slots'] - 1 == temp_jobs[k]['pe'][1]:
                    temp_jobs[k]['hosts'][temp_jobs[k]['master_queue'].split('@', 1)[1]] -= 1
                    temp_jobs[k]['slots'] -= 1
        return list(temp_jobs.values())
    @staticmethod
    def _parse_queue_info(item):
        temp_jobs = {}
        for i in item.iterfind('Queue-List'):
            for job in SGE_qstat._parse_queue_list(i):
                k = '{}.{}'.format(job['jobid'], job['taskid'])
                if k in temp_jobs.keys():
                    temp_jobs[k] = SGE_qstat._merge_jobs(temp_jobs[k], job)
                else:
                    temp_jobs[k] = job
        return list(temp_jobs.values())
    @staticmethod
    def _parse_queue_list(item):
        temp_jobs = {}
        queue_name, host_name = item.find('name').text.split('@', 1)
        for i in item.iterfind('job_list'):
            for job in SGE_qstat._parse_job_list(i):
                job['hosts'] = {}
                job['hosts'][host_name] = job['slots']
                k = '{}.{}'.format(job['jobid'], job['taskid'])
                if k in temp_jobs.keys():
                    temp_jobs[k] = SGE_qstat._merge_jobs(temp_jobs[k], job)
                else:
                    temp_jobs[k] = job
                if job['master_queue']:
                    temp_jobs[k]['master_queue'] = '{}@{}'.format(queue_name, host_name)
        return list(temp_jobs.values())
    @staticmethod
    def _parse_job_list(item):
        jobs = []
        job = {'taskid': None}
        job['state'] = item.get('state')
        for i in item.iterchildren():
            if i.tag == "JB_job_number":
                job['jobid'] = int(i.text)
            elif i.tag == 'JB_name':
                job['name'] = i.text if 'name' not in job.keys() else job['name']
            elif i.tag == 'JB_owner':
                job['user'] = i.text
            elif i.tag == 'JB_project':
                job['project'] = i.text
            elif i.tag == 'JB_department':
                job['department'] = i.text
            elif i.tag == 'master':
                job['master_queue'] = True if i.text == 'MASTER' else False
            elif i.tag == 'queue_name':
                if 'queues' not in job.keys():
                    job['queues'] = set()
                job['queues'].add(i.text)
            elif i.tag == 'hard_request':
                if 'requests' not in job.keys():
                    job['requests'] = {}
                job['requests'][i.get('name')] = i.text
            elif i.tag == 'hard_req_queue':
                if 'queue_requests' not in job.keys():
                    job['queue_requests'] = set()
                job['queue_requests'].add(i.text)
            elif i.tag == 'slots':
                job['slots'] = int(i.text)
            elif i.tag == 'requested_pe':
                if 'pe' not in job.keys():
                    job['pe'] = (i.get('name'), i.text)
            elif i.tag == 'granted_pe':
                job['pe'] = (i.get('name'), int(i.text))
        if 'queues' in job.keys():
            job['queues'] = list(job['queues'])
        if 'queue_requests' in job.keys():
            job['queue_requests'] = list(job['queue_requests'])
        if item.find('tasks') is not None:
            tasks = item.find('tasks').text
            if ',' in tasks:
                tasks = tasks.split(',')
            else:
                tasks = [tasks]
            for t in tasks:
                if t.isnumeric():
                    j = job.copy()
                    j['taskid'] = int(t)
                    jobs.append(j)
                else:
                    r, step = t.split(':', 1) if ':' in t else (t, "1")
                    start, stop = r.split('-', 1) if '-' in r else (r, r)
                    for i in range(int(start), int(stop)+1, int(step)):
                        j = job.copy()
                        j['taskid'] = i
                        jobs.append(j)
        else:
            job['taskid'] = 1
            jobs.append(job)
        return jobs
    def __init__(self):
        qstat_data = subprocess.Popen(['qstat', '-xml', '-g', 't', '-f', '-ext', '-r'], stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()[0].decode('UTF-8')
        xml = etree.fromstring(qstat_data)
        if xml.tag != 'job_info':
            raise Exception("Not Qstat data")
        self.job_info = self._parse_job_info(xml)
    def get_jobs(self, jobid, taskid):
        if jobid is not None:
            if taskid is not None:
                return [i for i in self.job_info if jobid == i['jobid'] and taskid == i['taskid']]
            return [i for i in self.job_info if jobid == i['jobid']]
        return self.job_info
    def get_hosts(self, hostid):
        jobs = {}
        for job in self.job_info:
            if 'hosts' in job.keys():
                for host in job['hosts'].keys():
                    if host == hostid or hostid is None:
                        if host not in jobs.keys():
                            jobs[host] = {'jobs': []}
                        jobs[host]['jobs'].append(job)
        return jobs
    def get_users(self, userid):
        jobs = {}
        for job in self.job_info:
            if job['user'] == userid or userid is None:
                if job['user'] not in jobs.keys():
                    jobs[job['user']] = {'jobs': []}
                jobs[job['user']]['jobs'].append(job)
        return jobs
