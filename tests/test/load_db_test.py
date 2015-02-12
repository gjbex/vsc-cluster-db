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
        error_msg = 'E: node r3i0n2 has no status\n'
        nr_nodes = 163
        nr_128gb_nodes = 32
        nr_ivybridge_nodes = 143
        with open(pbsnodes_file_name, 'r') as pbsnode_file:
            pbsnodes_parser = PbsnodesParser()
            nodes = pbsnodes_parser.parse_file(pbsnode_file)
        with sqlite3.connect(self._file_name) as conn:
            load_node_db.insert_partitions(conn, partitions)
            load_node_db.insert_qos_levels(conn, qos_levels)
            stderr_tmp = sys.stderr
            sys.stderr = StringIO.StringIO()
            load_node_db.insert_node_info(conn, nodes, partitions)
            self.assertEquals(error_msg, sys.stderr.getvalue())
            sys.stderr = stderr_tmp
            cursor = conn.cursor()
            result = cursor.execute('''SELECT qos FROM qos_levels''')
            nr_qos_levels = 0
            for row in result:
                self.assertIn(row[0], qos_levels)
                nr_qos_levels += 1
            self.assertEquals(len(qos_levels), nr_qos_levels)
            result = cursor.execute('''SELECT partition_name
                                           FROM partitions''')
            nr_partitions = 0
            for row in result:
                self.assertIn(row[0], partitions)
                nr_partitions += 1
            self.assertEquals(len(partitions), nr_partitions)
            result = cursor.execute("""SELECT count(*) FROM features
                                           WHERE feature = 'mem128'""")
            self.assertEquals(nr_128gb_nodes, result.fetchone()[0])
            result = cursor.execute("""SELECT count(*) FROM properties
                                           WHERE property = 'ivybridge'""")
            self.assertEquals(nr_ivybridge_nodes, result.fetchone()[0])
