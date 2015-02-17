#!/usr/bin/env python
'''Functions to populate a database to store information on nodes in
   a compute cluster with a PBS torque resource manager'''

import json, sys

from vsc.pbs.utils import compute_features, compute_partition
from vsc.utils import hostname2rackinfo
from vsc.pbs.pbsnodes import PbsnodesParser
from vsc.moab.showq import ShowqParser

NO_CONFIG_FILE_ERROR = 1
NO_PBSNODES_FILE_ERROR = 2
NO_SHOWQ_FILE_ERROR = 3
PBSNODES_CMD_ERROR = 4
SHOWQ_CMD_ERROR = 5
NO_PARTITIONS_ERROR = 6
NO_QOS_LEVELS_ERROR = 7
NO_PBSNODES_CMD_ERROR = 8
NO_SHOWQ_CMD_ERROR = 9
DB_EXISTS_ERROR = 10

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
                for node_property in node.properties:
                    cursor.execute(prop_insert_cmd,
                                   (node_id, node_property))
                for node_feature in compute_features(node):
                    cursor.execute(feature_insert_cmd,
                                   (node_id, node_feature))
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

def read_config(config_file_name, is_verbose=False):
    '''Read configuration file and return dictionary, or None when no
       file name was given'''
    config = None
    if config_file_name:
        try:
            with open(config_file_name, 'r') as config_file:
                config = json.load(config_file)
            if is_verbose:
                print 'configuration file read'
        except IOError as error:
            msg = '### error reading config file:  {0}'.format(str(error))
            sys.stderr.write(msg)
            sys.exit(NO_CONFIG_FILE_ERROR)
    return config

def get_nodes(pbsnodes_cmd, pbsnodes_file_name=None, is_verbose=False):
    '''Retrieve node information, either by running the pbsnodes command,
       or reading the information from a file'''
    pbsnodes_parser = PbsnodesParser()
    if pbsnodes_file_name:
        try:
            with open(pbsnodes_file_name, 'r') as node_file:
                nodes = pbsnodes_parser.parse_file(node_file)
        except IOError as error:
            msg = '### error reading pbsnodes file:  {0}'
            sys.stderr.write(msg.format(str(error)))
            sys.exit(NO_PBSNODES_FILE_ERROR)
    else:
        try:
            pbsnodes_cmd = config['pbsnodes_cmd']
            node_output = subprocess.check_output([pbsnodes_cmd])
            nodes = pbsnodes_parser.parse(node_output)
            if is_verbose:
                print '{0:d} nodes found'.format(len(nodes))
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute pbsnodes\n')
            sys.exit(PBSNODES_CMD_ERROR)
    return nodes

def get_jobs(showq_cmd, showq_file_name=None, is_verbose=False):
    '''Retrieve job information, either by running the showq command,
       or reading the information from a file'''
    if showq_file_name:
        try:
            showq_parser = ShowqParser()
            with open(showq_file_name, 'r') as job_file:
                jobs = showq_parser.parse_file(job_file)
        except IOError as error:
            msg = '### error reading showq file:  {0}'
            sys.stderr.write(msg.format(str(error)))
            sys.exit(NO_SHOWQ_FILE_ERROR)
    else:
        try:
            job_output = subprocess.check_output([showq_cmd])
            jobs = showq_parser.parse(job_output)
            if is_verbose:
                print '{0:d} nodes found'.format(len(jobs))
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute showq\n')
            sys.exit(SHOWQ_CMD_ERROR)
    return jobs

def get_partitions(partition_str, config):
    '''Get partitions, either from command line options, or from
       configuration file'''
    if partition_str:
        return partition_str.split(',')
    elif config and 'partitions' in config:
        return config['partitions']
    else:
        sys.stderr.write('### error: no partitions specified\n')
        sys.exit(NO_PARTITIONS_ERROR)

def get_qos_levels(qos_str, config):
    '''Get QOS levels, either from command line options, or from
       configuration file'''
    if qos_str:
        return qos_levels.split(',')
    elif config and 'qos_levels' in config:
        return config['qos_levels']
    else:
        sys.stderr.write('### error: no QOS levels specified\n')
        sys.exit(NO_QOS_LEVELS_ERROR)

def get_pbsnodes_cmd(pbsnodes_str, config):
    '''Get pbsnodes command, either from command line options, or from
       configuration file'''
    if pbsnodes_str:
        return pbsnodes_str
    elif config and 'pbsnodes_cmd' in config:
        return config['pbsnodes_cmd']
    else:
        sys.stderr.write('### error: no pbsnodes command specified\n')
        sys.exit(NO_PBSNODES_CMD_ERROR)

def get_showq_cmd(showq_str, config):
    '''Get showq command, either from command line options, or from
       configuration file'''
    if showq_str:
        return showq_str
    elif config and 'showq_cmd' in config:
        return config['showq_cmd']
    else:
        sys.stderr.write('### error: no showq command specified\n')
        sys.exit(NO_SHOWQ_CMD_ERROR)

if __name__ == '__main__':
    from argparse import ArgumentParser
    import os.path, sqlite3, subprocess
    import create_node_db

    arg_parser = ArgumentParser(description=('loads a database with node '
                                             'information'))
    arg_parser.add_argument('--conf', help='JSON configuration file')
    arg_parser.add_argument('--pbsnodes_file', help='pbsnodes file')
    arg_parser.add_argument('--showq_file', help='showq file')
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    arg_parser.add_argument('--partitions',
                            help='partitions defined for the cluster')
    arg_parser.add_argument('--qos_levels',
                            help='QOS defined for the cluster')
    arg_parser.add_argument('--jobs', action='store_true',
                            help='create job-related tables')
    arg_parser.add_argument('--force', action='store_true',
                            help='force to create a new DB')
    arg_parser.add_argument('--verbose', action='store_true',
                            help='show information for debugging')
    arg_parser.add_argument('--pbsnodes', help='pbsnodes command to use')
    arg_parser.add_argument('--showq', help='showq command to use')
    options = arg_parser.parse_args()
    config = read_config(options.conf, options.verbose)
    partition_list = get_partitions(options.partitions, config)
    qos_levels = get_qos_levels(options.qos_levels, config)
    pbs_nodes_cmd = get_pbsnodes_cmd(options.pbsnodes, config)
    nodes = get_nodes(options.pbsnodes, options.pbsnodes_file,
                      options.verbose)
    if not os.path.isfile(options.db):
        with sqlite3.connect(options.db) as conn:
            create_node_db.init_db(conn, create_node_db.DB_DESC)
    elif not options.force:
        msg = "### error: DB '{0}' already exists"
        sys.stderr.write(msg.format(options.db))
        sys.exit(DB_EXISTS_ERROR)
    else:
        with sqlite3.connect(self._file_name) as conn:
            create_node_db.init_db(conn, create_node_db.DB_DESC, force=True)
    with sqlite3.connect(options.db) as conn:
        partitions = insert_partitions(conn, partition_list)
        insert_qos_levels(conn, qos_levels)
        insert_node_info(conn, nodes, partitions, options.jobs)
        if options.jobs:
            showq_cmd = get_showq_cmd(options.showq, config)
            jobs = get_jobs(showq_cmd, options.showq_file,
                            options.verbose)
            insert_jobs(conn, jobs)
