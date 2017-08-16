import os
import json
import logging
from pysonic.scanner import PysonicFilesystemScanner


LETTER_GROUPS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
                 "u", "v", "w", "xyz", "0123456789"]


logging = logging.getLogger("library")


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


class DuplicateRootException(Exception):
    pass


class PysonicLibrary(object):
    def __init__(self, database):
        self.db = database
        self.scanner = PysonicFilesystemScanner(self)
        logging.info("library ready")

    def update(self):
        self.scanner.init_scan()

    def add_dir(self, dir_path):
        dir_path = os.path.abspath(os.path.normpath(dir_path))
        libraries = [self.db.decode_metadata(i['metadata'])['fspath'] for i in self.db.getnodes(-1)]
        if dir_path in libraries:
            raise DuplicateRootException("Dir already in library")
        else:
            new_root = self.db._addnode(-1, 'New Library', is_dir=True)
            self.db.update_metadata(new_root['id'], fspath=dir_path)

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
    def get_albums(self):
        return self.db.getnodes(*[item["id"] for item in self.get_artists()])

    @memoize
    def get_filepath(self, nodeid):
        parents = [self.db.getnode(nodeid)]
        while parents[-1]['parent'] != -1:
            parents.append(self.db.getnode(parents[-1]['parent']))
        root = parents.pop()
        parents.reverse()
        return os.path.join(json.loads(root['metadata'])['fspath'], *[i['name'] for i in parents])

    def get_file_metadata(self, nodeid):
        return self.db.get_metadata(nodeid)

    def get_artist_info(self, item_id):
        # artist = self.db.getnode(item_id)
        return {"biography": "placeholder biography",
                "musicBrainzId": "playerholder",
                "lastFmUrl": "https://www.last.fm/music/Placeholder",
                "smallImageUrl": "",
                "mediumImageUrl": "",
                "largeImageUrl": "",
                "similarArtists": []}

    def set_starred(self, username, node_id, starred):
        self.db.set_starred(self.db.get_user(username)["id"], node_id, starred)

    def get_user(self, user):
        if type(user) is int:
            return self.db.get_user(username)
