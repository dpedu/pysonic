import os
import json
import sqlite3
from itertools import chain
from contextlib import closing


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class PysonicDatabase(object):
    def __init__(self, path):
        self.sqlite_opts = dict(check_same_thread=False, cached_statements=0, isolation_level=None)
        self.path = path
        self.db = None

        self.open()
        self.migrate()

    def open(self):
        self.db = sqlite3.connect(self.path, **self.sqlite_opts)
        self.db.row_factory = dict_factory

    def migrate(self):
        # Create db
        queries = ["""CREATE TABLE 'meta' (
                        'key' TEXT PRIMARY KEY NOT NULL,
                        'value' TEXT);""",
                   """INSERT INTO meta VALUES ('db_version', '0');""",
                   """CREATE TABLE 'nodes' (
                        'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        'parent' INTEGER NOT NULL,
                        'isdir' BOOLEAN NOT NULL,
                        'name' TEXT NOT NULL,
                        'title' TEXT,
                        'album' TEXT,
                        'artist' TEXT,
                        'metadata' TEXT
                        )""",
                   """INSERT INTO nodes (parent, isdir, name, metadata)
                        VALUES (-1, 1, 'Main Library', '{"fspath": "/home/dave/Code/pysonic/music/"}');"""]

        with closing(self.db.cursor()) as cursor:
            cursor.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='meta';")

            # Initialize DB
            if len(cursor.fetchall()) == 0:
                print("Initializing database")
                for query in queries:
                    cursor.execute(query)
            else:
                # Migrate if old db exists
                version = int(cursor.execute("SELECT * FROM meta WHERE key='db_version';").fetchone()['value'])
                print("db schema is version {}".format(version))

    # Virtual file tree
    def getnode(self, node_id):
        with closing(self.db.cursor()) as cursor:
            return cursor.execute("SELECT * FROM nodes WHERE id=?;", (node_id, )).fetchone()

    def getnodes(self, *parent_ids):
        with closing(self.db.cursor()) as cursor:
            return list(chain(*[cursor.execute("SELECT * FROM nodes WHERE parent=?;", (parent_id, )).fetchall()
                              for parent_id in parent_ids]))

    def addnode(self, parent, fspath, name):
        fullpath = os.path.join(fspath, name)
        print("Adding ", fullpath)
        is_dir = os.path.isdir(fullpath)
        with closing(self.db.cursor()) as cursor:
            cursor.execute("INSERT INTO nodes (parent, isdir, name) VALUES (?, ?, ?);",
                           (parent["id"], 1 if is_dir else 0, name))
            return self.getnode(cursor.lastrowid)

    def delnode(self, node_id):
        deleted = 1
        for child in self.getnodes(node_id):
            deleted += self.delnode(child["id"])
        with closing(self.db.cursor()) as cursor:
            cursor.execute("DELETE FROM nodes WHERE id=?;", (node_id, ))
        return deleted

    def update_metadata(self, node_id, mergedict=None, **kwargs):
        mergedict = mergedict if mergedict else {}
        keys_in_table = ["title", "album", "artist"]
        mergedict.update(kwargs)
        with closing(self.db.cursor()) as cursor:
            for table_key in keys_in_table:
                if table_key in mergedict:
                    cursor.execute("UPDATE nodes SET {}=? WHERE id=?;".format(table_key),
                                   (mergedict[table_key], node_id))
            other_meta = {k: v for k, v in mergedict.items() if k not in keys_in_table}
            if other_meta:
                metadata = self.get_metadata(node_id)
                metadata.update(other_meta)
                cursor.execute("UPDATE nodes SET metadata=? WHERE id=?;", (json.dumps(metadata), node_id, ))

    def get_metadata(self, node_id):
        return self.decode_metadata(self.getnode(node_id)["metadata"])

    def decode_metadata(self, metadata):
        if metadata:
            return json.loads(metadata)
        return {}
