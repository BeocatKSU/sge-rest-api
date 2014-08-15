#!/usr/bin/env python

from flask import Blueprint, jsonify, url_for, abort, make_response

from sgerestapi import model

api_beta = Blueprint('api_beta', __name__)

@api_beta.errorhandler(404)
def not_found(error):
    return make_response(jsonify( { 'error': 'Not found' } ), 404)

@api_beta.route('/', methods = ['GET'])
def index():
    return jsonify( {
            'version': '0.5',
            'endpoints': [ url_for('.get_user'), url_for('.get_job'), url_for('.get_host')]
        })

@api_beta.route('/users', defaults={'userid': None}, methods = ['GET'])
@api_beta.route('/users/', defaults={'userid': None}, methods = ['GET'])
@api_beta.route('/users/<userid>', methods = ['GET'])
def get_user(userid):
    qstat = model.SGE_qstat()
    retval = {
            'users': qstat.get_users(userid)
        }
    if len(retval['users']) == 0:
        abort(404)
    for user in retval['users'].keys():
        retval['users'][user]['url'] = url_for('.get_user', userid=user)
        for job in retval['users'][user]['jobs']:
            job['url'] = url_for('.get_job', jobid=job['jobid'], taskid=job['taskid'])
    return jsonify(retval)

@api_beta.route('/jobs', defaults={'jobid': None, 'taskid': None}, methods = ['GET'])
@api_beta.route('/jobs/', defaults={'jobid': None, 'taskid': None}, methods = ['GET'])
@api_beta.route('/jobs/<int:jobid>', defaults={'taskid': None}, methods = ['GET'])
@api_beta.route('/jobs/<int:jobid>.<int:taskid>', methods = ['GET'])
def get_job(jobid, taskid):
    qstat = model.SGE_qstat()
    retval = {
            'jobs': qstat.get_jobs(jobid, taskid)
        }
    if len(retval['jobs']) == 0:
        abort(404)
    for job in retval['jobs']:
        job['url'] = url_for('.get_job', jobid=job['jobid'], taskid=job['taskid'])
    return jsonify(retval)

@api_beta.route('/hosts', defaults={'hostid': None}, methods = ['GET'])
@api_beta.route('/hosts/', defaults={'hostid': None}, methods = ['GET'])
@api_beta.route('/hosts/<hostid>', methods = ['GET'])
def get_host(hostid):
    qstat = model.SGE_qstat()
    retval = {
            'hosts': qstat.get_hosts(hostid)
        }
    if len(retval['hosts']) == 0:
        abort(404)
    for host in retval['hosts'].keys():
        retval['hosts'][host]['url'] = url_for('.get_host', hostid=host)
        for job in retval['hosts'][host]['jobs']:
            job['url'] = url_for('.get_job', jobid=job['jobid'], taskid=job['taskid'])
    return jsonify(retval)
