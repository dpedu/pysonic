import os
import json
import sqlite3
import logging
from hashlib import sha512
from itertools import chain
from contextlib import closing


logging = logging.getLogger("database")


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class NotFoundError(Exception):
    pass


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
                        'type' TEXT,
                        'title' TEXT,
                        'album' TEXT,
                        'artist' TEXT,
                        'metadata' TEXT
                        )"""]

        with closing(self.db.cursor()) as cursor:
            cursor.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='meta';")

            # Initialize DB
            if len(cursor.fetchall()) == 0:
                logging.warning("Initializing database")
                for query in queries:
                    cursor.execute(query)
            else:
                # Migrate if old db exists
                version = int(cursor.execute("SELECT * FROM meta WHERE key='db_version';").fetchone()['value'])
                if version < 1:
                    logging.warning("migrating database to v1 from %s", version)
                    users_table = """CREATE TABLE 'users' (
                                        'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                        'username' TEXT UNIQUE NOT NULL,
                                        'password' TEXT NOT NULL,
                                        'admin' BOOLEAN DEFAULT 0,
                                        'email' TEXT)"""
                    cursor.execute(users_table)
                    version = 1
                if version < 2:
                    logging.warning("migrating database to v2 from %s", version)
                    users_table = """CREATE TABLE 'stars' (
                                        'userid' INTEGER,
                                        'nodeid' INTEGER,
                                        primary key ('userid', 'nodeid'))"""
                    cursor.execute(users_table)
                    version = 2

                cursor.execute("""UPDATE meta SET value=? WHERE key="db_version";""", (str(version), ))
                logging.warning("db schema is version {}".format(version))

    # Virtual file tree
    def getnode(self, node_id):
        return self.getnodes(node_id=node_id)[0]

    def _populate_meta(self, node):
        node['metadata'] = self.decode_metadata(node['metadata'])
        return node

    def getnodes(self, *parent_ids, node_id=None, types=None, limit=None, order=None):
        """
        Find nodes that match the passed paramters.
        :param parent_ids: one or more parents to find children of
        :type parent_ids: int
        :param node_id: single node id to return
        :type node_id: int
        :param types: filter by type column
        :type types: list
        :param limit: number of records to limit to
        :param order: one of ("rand") to select ordering mode
        """
        query = "SELECT * FROM nodes WHERE "
        qargs = []

        def add_filter(name, values):
            nonlocal query
            nonlocal qargs
            query += "{} in (".format(name)
            for value in (values if type(values) in [list, tuple] else [values]):
                query += "?, "
                qargs += [value]
            query = query.rstrip(", ")
            query += ") AND"

        if node_id:
            add_filter("id", node_id)
        if parent_ids:
            add_filter("parent", parent_ids)
        if types:
            add_filter("type", types)

        query = query.rstrip(" AND")

        if order:
            query += "ORDER BY "
            if order == "rand":
                query += "RANDOM()"

        if limit:  # TODO 2-item tuple limit
            query += " limit {}".format(limit)

        with closing(self.db.cursor()) as cursor:
            return list(map(self._populate_meta, cursor.execute(query, qargs).fetchall()))

    def addnode(self, parent_id, fspath, name):
        fullpath = os.path.join(fspath, name)
        is_dir = os.path.isdir(fullpath)
        return self._addnode(parent_id, name, is_dir)

    def _addnode(self, parent_id, name, is_dir=True):
        with closing(self.db.cursor()) as cursor:
            cursor.execute("INSERT INTO nodes (parent, isdir, name) VALUES (?, ?, ?);",
                           (parent_id, 1 if is_dir else 0, name))
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
        keys_in_table = ["title", "album", "artist", "type"]
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
        keys_in_table = ["title", "album", "artist", "type"]
        node = self.getnode(node_id)
        meta = node["metadata"]
        meta.update({item: node[item] for item in keys_in_table})
        return meta

    def decode_metadata(self, metadata):
        if metadata:
            return json.loads(metadata)
        return {}

    def hashit(self, unicode_string):
        return sha512(unicode_string.encode('UTF-8')).hexdigest()

    def validate_password(self, realm, username, password):
        with closing(self.db.cursor()) as cursor:
            users = cursor.execute("SELECT * FROM users WHERE username=? AND password=?;",
                                   (username, self.hashit(password))).fetchall()
            return bool(users)

    def add_user(self, username, password, is_admin=False):
        with closing(self.db.cursor()) as cursor:
            cursor.execute("REPLACE INTO users (username, password, admin) VALUES (?, ?, ?)",
                           (username, self.hashit(password), is_admin)).fetchall()

    def get_user(self, user):
        with closing(self.db.cursor()) as cursor:
            try:
                column = "id" if type(user) is int else "username"
                return cursor.execute("SELECT * FROM users WHERE {}=?;".format(column), (user, )).fetchall()[0]
            except IndexError:
                raise NotFoundError("User doesn't exist")

    def set_starred(self, user_id, node_id, starred=True):
        with closing(self.db.cursor()) as cursor:
            if starred:
                query = "INSERT INTO stars (userid, nodeid) VALUES (?, ?);"
            else:
                query = "DELETE FROM stars WHERE userid=? and nodeid=?;"
            try:
                cursor.execute(query, (user_id, node_id))
            except sqlite3.IntegrityError:
                pass

    def get_starred_items(self, for_user_id=None):
        with closing(self.db.cursor()) as cursor:
            q = """SELECT n.* FROM nodes as n INNER JOIN stars as s ON s.nodeid = n.id"""
            qargs = []
            if for_user_id:
                q += """ AND userid=?"""
                qargs += [int(for_user_id)]
            return list(map(self._populate_meta,
                            cursor.execute(q, qargs).fetchall()))
