#!/usr/bin/env python
'''Functions to populate a database to store information on nodes in
   a compute cluster with a PBS torque resource manager'''

import sys

from vsc.pbs.utils import compute_features, compute_partition
from vsc.utils import hostname2rackinfo

NO_CONFIG_FILE_ERROR = 1
NO_PBSNODES_FILE_ERROR = 2
NO_SHOWQ_FILE_ERROR = 3
PBSNODES_CMD_ERROR = 4
SHOWQ_CMD_ERROR = 5

def insert_partitions(conn, partition_list):
    '''insert partitions, and return a dictionary of partition names
       and IDs'''
    cursor = conn.cursor()
    partition_insert_cmd = '''INSERT INTO partitions
                                  (partition_name) VALUES (?)'''
    partitions = {}
    for partition_name in partition_list:
        cursor.execute(partition_insert_cmd, (partition_name, ))
        partitions[partition_name] = cursor.lastrowid
    cursor.close()
    conn.commit()
    return partitions

def insert_qos_levels(conn, qos_levels):
    '''insert QOS levels'''
    cursor = conn.cursor()
    qos_insert_cmd = '''INSERT INTO qos_levels
                                  (qos) VALUES (?)'''
    for qos in qos_levels:
        cursor.execute(qos_insert_cmd, (qos, ))
    cursor.close()
    conn.commit()

def insert_node_info(conn, nodes, partitions, do_jobs=False):
    '''insert node information, including properties and features'''
    cursor = conn.cursor()
    node_insert_cmd = '''INSERT INTO nodes
                             (hostname, partition_id, rack, iru, np, mem)
                         VALUES
                             (?, ?, ?, ?, ?, ?)'''
    prop_insert_cmd = '''INSERT INTO properties
                             (node_id, property) VALUES
                             (?, ?)'''
    feature_insert_cmd = '''INSERT INTO features
                                (node_id, feature) VALUES
                                (?, ?)'''
    running_job_insert_cmd = '''INSERT INTO running_jobs
                                    (job_id, node_id) VALUES
                                    (?, ?)'''
    for node in nodes:
        partition_id = compute_partition(node, partitions)
        rack, iru, _ = hostname2rackinfo(node.hostname)
        if partition_id:
            if node.status:
                cursor.execute(node_insert_cmd, (node.hostname,
                                                 partition_id,
                                                 rack,
                                                 iru,
                                                 node.np,
                                                 node.memory))
                node_id = cursor.lastrowid
                for property in node.properties:
                    cursor.execute(prop_insert_cmd, (node_id, property))
                for feature in compute_features(node):
                    cursor.execute(feature_insert_cmd, (node_id, feature))
                if do_jobs:
                    for job_id in node.job_ids:
                        cursor.execute(running_job_insert_cmd,
                                       (job_id, node_id))
            else:
                msg = 'E: node {0} has no status\n'.format(node.hostname)
                sys.stderr.write(msg)
    cursor.close()
    conn.commit()

def insert_jobs(conn, jobs):
    '''insert information on jobs, active and non-active'''
    cursor = conn.cursor()
    active_jobs_insert_cmd = '''INSERT INTO jobs
                                    (job_id, user, state, procs,
                                     remaining, starttime) VALUES
                                    (?, ?, ?, ?, ?, ?)'''
    nonactive_jobs_insert_cmd = '''INSERT INTO jobs
                                       (job_id, user, state, procs,
                                        wclimit, queuetime) VALUES
                                       (?, ?, ?, ?, ?, ?)'''
    for job_state in jobs:
        if job_state == 'active':
            for job in jobs[job_state]:
                cursor.execute(active_jobs_insert_cmd,
                               (job.id, job.username, job.state,
                                job.procs, job.remaining, job.starttime))
        else:
            for job in jobs[job_state]:
                cursor.execute(nonactive_jobs_insert_cmd,
                               (job.id, job.username, job.state,
                                job.procs, job.wclimit, job.queuetime))
    conn.commit()

if __name__ == '__main__':
    from argparse import ArgumentParser
    import json, sqlite3, subprocess

    from vsc.pbs.pbsnodes import PbsnodesParser
    from vsc.moab.showq import ShowqParser

    arg_parser = ArgumentParser(description=('loads a database with node '
                                             'information'))
    arg_parser.add_argument('--conf', help='JSON configuration file')
    arg_parser.add_argument('--pbsnodes_file', help='pbsnodes file')
    arg_parser.add_argument('--showq_file', help='showq file')
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    arg_parser.add_argument('--partitions', default='thinking,gpu,phi',
                            help='partitions defined for the cluster')
    arg_parser.add_argument('--qos_levels', default='debugging,normal',
                            help='QOS defined for the cluster')
    arg_parser.add_argument('--jobs', action='store_true',
                            help='create job-related tables')
    arg_parser.add_argument('--pbsnodes', default='/usr/local/bin/pbsnodes',
                            help='pbsnodes command to use')
    arg_parser.add_argument('--showq', default='/opt/moab/bin/showq',
                            help='showq command to use')
    options = arg_parser.parse_args()
    if options.conf:
        try:
            with open(options.conf, 'r') as config_file:
                config = json.load(config_file)
        except IOError as error:
            msg = '### error reading config file:  {0}'.format(str(error))
            sys.stderr.write(msg)
            sys.exit(NO_CONFIG_FILE_ERROR)
    else:
        config = None
    partition_list = options.partitions.split(',')
    qos_levels = options.qos_levels.split(',')
    if options.pbsnodes_file:
        try:
            with open(options.pbsnodes_file, 'r') as node_file:
                pbsnodes_parser = PbsnodesParser()
                nodes = pbsnodes_parser.parse_file(node_file)
        except IOError as error:
            msg = '### error reading pbsnodes file:  {0}'.format(str(error))
            sys.stderr.write(msg)
            sys.exit(NO_PBSNODES_FILE_ERROR)
    else:
        try:
            node_output = subprocess.check_output([options.pbsnodes])
            nodes = pbsnodes_parser.parse(node_output)
            if options.verbose:
                print '{0:d} nodes found'.format(len(nodes))
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute pbsnodes\n')
            sys.exit(PBSNODES_CMD_ERROR)
    if options.jobs:
        if options.showq_file:
            try:
                showq_parser = ShowqParser()
                with open(options.showq_file, 'r') as job_file:
                    jobs = showq_parser.parse_file(job_file)
            except IOError as error:
                msg = '### error reading showq file:  {0}'.format(str(error))
                sys.stderr.write(msg)
                sys.exit(NO_SHOWQ_FILE_ERROR)
        else:
            try:
                job_output = subprocess.check_output([options.showq])
                jobs = showq_parser.parse(job_output)
                if options.verbose:
                    print '{0:d} nodes found'.format(len(jobs))
            except subprocess.CalledProcessError:
                sys.stderr.write('### error: could not execute showq\n')
                sys.exit(SHOWQ_CMD_ERROR)
    with sqlite3.connect(options.db) as conn: 
        partitions = insert_partitions(conn, partition_list)
        insert_qos_levels(conn, qos_levels)
        insert_node_info(conn, nodes, partitions, options.jobs)
        if options.jobs:
            insert_jobs(conn, jobs)
