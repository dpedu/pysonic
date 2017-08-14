import logging
import cherrypy
from bs4 import BeautifulSoup
import sqlite3
import os
from contextlib import closing
import json
from threading import Thread
from itertools import chain
import sys

# import pdb
# from pprint import pprint

LETTER_GROUPS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
                 "u", "v", "w", "x-z", "#"]


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class PysonicDatabase(object):
    def __init__(self):
        self.sqlite_opts = dict(check_same_thread=False, cached_statements=0, isolation_level=None)
        self.db = None

        self.open()
        self.migrate()

        self.scanner = Thread(target=self.rescan, daemon=True)
        self.scanner.start()

    def open(self):
        self.db = sqlite3.connect("db.sqlite", **self.sqlite_opts)
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

    def rescan(self):
        # Perform directory scan
        with closing(self.db.cursor()) as cursor:

            # Find top level dirs, parent=-1
            for parent in cursor.execute("SELECT id, name, metadata FROM nodes WHERE parent=-1;").fetchall():
                meta = json.loads(parent["metadata"])
                # print("Scanning {}".format(meta["fspath"]))

                def recurse_dir(path, parent):
                    # print("Scanning {} with parent {}".format(path, parent))
                    # create or update the database of nodes by comparing sets of names
                    fs_entries = set(os.listdir(path))
                    db_entires = self.getnodes(parent["id"])
                    db_entires_names = set([i['name'] for i in db_entires])
                    to_delete = db_entires_names - fs_entries
                    to_create = fs_entries - db_entires_names

                    # Create any nodes not found in the db
                    for create in to_create:
                        new_node = self.addnode(parent, path, create)
                        db_entires.append(new_node)

                    # Delete any db nodes not found on disk
                    for delete in to_delete:
                        print("Prune ", delete, "in parent", path)
                        node = [i for i in db_entires if i["name"] == delete]
                        if node:
                            deleted = self.delnode(node[0]["id"])
                            print("Pruned {}, deleting total of {}".format(node, deleted))

                    for entry in db_entires:
                        if entry["name"] in to_delete:
                            continue

                        if int(entry['isdir']):  # 1 means dir
                            recurse_dir(os.path.join(path, entry["name"]), entry)
                # Populate all files for this top-level root
                recurse_dir(meta["fspath"], parent)
                #
                #
                #
                # Add simple metadata
                for artist_dir in self.getnodes(parent["id"]):
                    artist = artist_dir["name"]
                    for album_dir in self.getnodes(artist_dir["id"]):
                        album = album_dir["name"]
                        album_meta = self.get_metadata(album_dir["id"])
                        for track_file in self.getnodes(album_dir["id"]):
                            title = track_file["name"]
                            if not track_file["title"]:
                                self.update_metadata(track_file["id"], artist=artist, album=album, title=title)
                                print("Adding simple metadata for {}/{}/{} #{}".format(artist, album,
                                                                                       title, track_file["id"]))
                            if not album_dir["album"]:
                                self.update_metadata(album_dir["id"], artist=artist, album=album)
                                print("Adding simple metadata for {}/{} #{}".format(artist, album, album_dir["id"]))
                            if not artist_dir["artist"]:
                                self.update_metadata(artist_dir["id"], artist=artist)
                                print("Adding simple metadata for {} #{}".format(artist, artist_dir["id"]))
                            if title == "cover.jpg" and 'cover' not in album_meta:
                                # // add cover art
                                self.update_metadata(album_dir["id"], cover=track_file["id"])
                                print("added cover for {}".format(album_dir['id']))
                print("Metadata scan complete.")


def memoize(function):
    memo = {}

    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv
    return wrapper


class NoDataException(Exception):
    pass


class PysonicLibrary(object):
    def __init__(self, database):
        self.db = database
        print("library ready")

    @memoize
    def get_libraries(self):
        """
        Libraries are top-level nodes
        """
        return self.db.getnodes(-1)

    @memoize
    def get_artists(self):
        # Assume artists are second level dirs
        return self.db.getnodes(*[item["id"] for item in self.get_libraries()])

    def get_dir(self, dirid):
        return self.db.getnode(dirid)

    def get_dir_children(self, dirid):
        return self.db.getnodes(dirid)

    @memoize
    def get_filepath(self, fileid):
        parents = [self.db.getnode(fileid)]
        while parents[-1]['parent'] != -1:
            parents.append(self.db.getnode(parents[-1]['parent']))
        root = parents.pop()
        parents.reverse()
        return os.path.join(json.loads(root['metadata'])['fspath'], *[i['name'] for i in parents])

    def get_artist_info(self, item_id):
        # artist = self.db.getnode(item_id)
        return {"biography": "placeholder biography",
                "musicBrainzId": "playerholder",
                "lastFmUrl": "https://www.last.fm/music/Placeholder",
                "smallImageUrl": "",
                "mediumImageUrl": "",
                "largeImageUrl": "",
                "similarArtists": []}


class PysonicApi(object):
    def __init__(self):
        self.db = PysonicDatabase()
        self.library = PysonicLibrary(self.db)

        print("Libraries:", [i["name"] for i in self.library.get_libraries()])
        print("Artists:", [i["name"] for i in self.library.get_artists()])

    def response(self, status="ok"):
        doc = BeautifulSoup('', features='lxml-xml')
        root = doc.new_tag("subsonic-response", xmlns="http://subsonic.org/restapi", status=status, version="1.15.0")
        doc.append(root)
        return doc, root

    @cherrypy.expose
    def ping_view(self, **kwargs):
        # Called when the app hits the "test connection" server option
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        yield doc.prettify()

    @cherrypy.expose
    def getLicense_view(self, **kwargs):
        # Called after ping.view
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        root.append(doc.new_tag("license",
                                valid="true",
                                email="admin@localhost",
                                licenseExpires="2018-06-22T10:31:49.921Z",
                                trialExpires="2016-06-29T03:03:58.200Z"))
        yield doc.prettify()

    @cherrypy.expose
    def getMusicFolders_view(self, **kwargs):
        # Get list of configured dirs
        # {'c': 'DSub', 's': 'bfk9mir8is02u3m5as8ucsehn0', 'v': '1.2.0',
        #  't': 'e2b09fb9233d1bfac9abe3dc73017f1e', 'u': 'dave'}
        # Access-Control-Allow-Origin:*
        # Content-Encoding:gzip
        # Content-Type:text/xml; charset=utf-8
        # Server:Jetty(6.1.x)
        # Transfer-Encoding:chunked
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'

        doc, root = self.response()
        folder_list = doc.new_tag("musicFolders")
        root.append(folder_list)

        for folder in self.library.get_libraries():
            entry = doc.new_tag("musicFolder", id=folder["id"])
            entry.attrs["name"] = folder["name"]
            folder_list.append(entry)
        yield doc.prettify()

    @cherrypy.expose
    def getIndexes_view(self, **kwargs):
        # Get listing of top-level dir
        # /rest/getIndexes.view?u=dave&s=bfk9mir8is02u3m5as8ucsehn0
        # &t=e2b09fb9233d1bfac9abe3dc73017f1e&v=1.2.0&c=DSub HTTP/1.1
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        indexes = doc.new_tag("indexes", lastModified="1502310831000", ignoredArticles="The El La Los Las Le Les")
        doc.append(indexes)

        for letter in LETTER_GROUPS:
            index = doc.new_tag("index")
            index.attrs["name"] = letter.upper()
            indexes.append(index)
            for artist in self.library.get_artists():
                if artist["name"][0].lower() == letter:
                    artist_tag = doc.new_tag("artist")
                    artist_tag.attrs.update({"id": artist["id"], "name": artist["name"]})
                    index.append(artist_tag)
        yield doc.prettify()

    @cherrypy.expose
    def getMusicDirectory_view(self, id, **kwargs):
        """
        List an artist dir
        """
        dir_id = int(id)

        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()

        dirtag = doc.new_tag("directory")

        directory = self.library.get_dir(dir_id)
        dir_meta = self.db.decode_metadata(directory["metadata"])
        children = self.library.get_dir_children(dir_id)
        dirtag.attrs.update(name=directory['name'], id=directory['id'],
                            parent=directory['parent'], playCount=10)
        root.append(dirtag)

        for item in children:
            child = doc.new_tag("child",
                                id=item["id"],
                                parent=directory["id"],
                                isDir="true" if item['isdir'] else "false",
                                title=item["name"],
                                album=item["name"],
                                artist=directory["name"],
                                # playCount="5",
                                # created="2016-04-25T07:31:33.000Z"
                                # track="3",
                                # year="2012",
                                # genre="Other",
                                # coverArt="12835",
                                # contentType="audio/mpeg"
                                # suffix="mp3"
                                # size="15838864"
                                # duration="395"
                                # bitRate="320"
                                # path="Cosmic Gate/Sign Of The Times/03 Flatline (featuring Kyler England).mp3"
                                # albumId="933"
                                # artistId="353"
                                # type="music"/>
                                )
            item_meta = self.db.decode_metadata(item['metadata'])
            if 'cover' in item_meta:
                child.attrs["coverArt"] = item_meta["cover"]
            elif 'cover' in dir_meta:
                child.attrs["coverArt"] = dir_meta["cover"]
            dirtag.append(child)
        yield doc.prettify()

    @cherrypy.expose
    def stream_view(self, id, **kwargs):
        # /rest/stream.view?u=dave&s=rid5h452ag6nmb153r8sjtctk8
        # &t=dad1e6f7331160ea7f04120c7fbab1c8&v=1.2.0&c=DSub&id=167&maxBitRate=256
        fpath = self.library.get_filepath(id)
        cherrypy.response.headers['Content-Type'] = 'audio/mpeg'

        def content():
            total = 0
            with open(fpath, "rb") as f:
                while True:
                    data = f.read(8192)
                    if not data:
                        break
                    total += len(data)
                    yield data
                    sys.stdout.write('.')
                    sys.stdout.flush()
            print("\nSent {} bytes for {}".format(total, fpath))
        return content()
    stream_view._cp_config = {'response.stream': True}

    @cherrypy.expose
    def getCoverArt_view(self, id, **kwargs):
        # /rest/getCoverArt.view?u=dave&s=bfk9mir8is02u3m5as8ucsehn0
        # &t=e2b09fb9233d1bfac9abe3dc73017f1e&v=1.2.0&c=DSub&id=12833
        fpath = self.library.get_filepath(id)
        cherrypy.response.headers['Content-Type'] = 'image/jpeg'

        def content():
            total = 0
            with open(fpath, "rb") as f:
                while True:
                    data = f.read(8192)
                    if not data:
                        break
                    total += len(data)
                    yield data
                    sys.stdout.write('.')
                    sys.stdout.flush()
            print("\nSent {} bytes for {}".format(total, fpath))
        return content()

    getCoverArt_view._cp_config = {'response.stream': True}

    @cherrypy.expose
    def getArtistInfo_view(self, id, includeNotPresent="true", **kwargs):
        #/rest/getArtistInfo.view?
        # u=dave
        # s=gqua9i6c414aomjok8f6b0kdp1
        # t=ed1d31850bbd27690687305d9ccbdabf
        # v=1.2.0
        # c=DSub
        # id=7
        # includeNotPresent=true
        info = self.library.get_artist_info(id)
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()

        dirtag = doc.new_tag("artistInfo")
        root.append(dirtag)

        for key, value in info.items():
            if key == "similarArtists":
                continue
            tag = doc.new_tag(key)
            tag.append(str(value))
            # print(dir(tag))
            # print(value)
            dirtag.append(tag)
        yield doc.prettify()

    @cherrypy.expose
    def getUser_view(self, u, username, **kwargs):
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        user = doc.new_tag("user",
                           username="admin",
                           email="admin@localhost",
                           scrobblingEnabled="false",
                           adminRole="false",
                           settingsRole="false",
                           downloadRole="true",
                           uploadRole="false",
                           playlistRole="true",
                           coverArtRole="false",
                           commentRole="false",
                           podcastRole="false",
                           streamRole="true",
                           jukeboxRole="false",
                           shareRole="true",
                           videoConversionRole="false",
                           avatarLastChanged="2017-08-07T20:16:24.596Z")
        root.append(user)
        folder = doc.new_tag("folder")
        folder.append("0")
        user.append(folder)
        yield doc.prettify()


def main():

    logging.basicConfig(level=logging.INFO)

    cherrypy.tree.mount(PysonicApi(), '/rest/', {'/': {}})

    cherrypy.config.update({
        'sessionFilter.on': True,
        'tools.sessions.on': True,
        'tools.sessions.locking': 'explicit',
        'tools.sessions.timeout': 525600,
        'request.show_tracebacks': True,
        'server.socket_port': 3000,
        'server.thread_pool': 25,
        'server.socket_host': '0.0.0.0',
        'server.show_tracebacks': True,
        'server.socket_timeout': 5,
        'log.screen': False,
        'engine.autoreload.on': True
    })

    try:
        cherrypy.engine.start()
        cherrypy.engine.block()
    finally:
        print("API has shut down")
        cherrypy.engine.exit()

if __name__ == '__main__':
    main()
