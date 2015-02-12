#!/usr/bin/env python
'''module to test the creation of a cluster database'''

import os, sqlite3, unittest
import create_node_db

class CreateNodeDbTest(unittest.TestCase):
    '''Tests the creation of a cluster database'''

    def setUp(self):
        self._file_name = 'data/nodes.db'
        try:
            os.remove(self._file_name)
        except OSError:
            pass

    def tearDown(self):
        try:
            os.remove(self._file_name)
        except OSError:
            pass

    def test_create(self):
        tables = {'partitions', 'nodes', 'properties', 'features',
                  'qos_levels'}
        with sqlite3.connect(self._file_name) as conn:
            create_node_db.init_db(conn, create_node_db.DB_DESC)
            cursor = conn.cursor()
            result = cursor.execute("""SELECT name FROM sqlite_master
                                           WHERE type = 'table'""")
            nr_rows = 0
            for row in result:
                if not row[0].startswith('sqlite_'):
                    self.assertIn(row[0], tables)
                    nr_rows += 1
            self.assertEquals(len(tables), nr_rows)
