#!/usr/bin/env python
'''module to test the loading of a cluster database'''

import json, os, sqlite3, StringIO, sys, unittest
import create_node_db
import load_node_db
from vsc.pbs.pbsnodes import PbsnodesParser

class LoadNodeDbTest(unittest.TestCase):
    '''Tests the loading of a cluster database'''

    def setUp(self):
        self._file_name = 'data/nodes.db'
        try:
            os.remove(self._file_name)
        except OSError:
            pass
        with sqlite3.connect(self._file_name) as conn:
            create_node_db.init_db(conn, create_node_db.DB_DESC)
        config_file_name = '../../../vsc-tools-lib/conf/config.json'
        with open(config_file_name, 'r') as config_file:
            self._config = json.load(config_file)

    def tearDown(self):
        try:
            os.remove(self._file_name)
        except OSError:
            pass

    def test_load(self):
        pbsnodes_file_name = 'data/pbsnodes.txt'
        qos_levels = self._config['qos_levels']
        partitions = self._config['partitions']
        with open(pbsnodes_file_name, 'r') as pbsnode_file:
            pbsnodes_parser = PbsnodesParser()
            nodes = pbsnodes_parser.parse_file(pbsnode_file)
        with sqlite3.connect(self._file_name) as conn:
            load_node_db.insert_partitions(conn, partitions)
            load_node_db.insert_qos_levels(conn, qos_levels)
            error = sys.stderr
            sys.stderr = StringIO.StringIO()
            load_node_db.insert_node_info(conn, nodes, partitions)
            print 'my error', sys.stderr.get_value()
            sys.stderr = error
            cursor = conn.cursor()
