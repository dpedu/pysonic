import os
import json
from pysonic.scanner import PysonicFilesystemScanner


LETTER_GROUPS = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
                 "u", "v", "w", "x-z", "#"]


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
        self.scanner = PysonicFilesystemScanner(self)
        print("library ready")

    def update(self):
        self.scanner.init_scan()

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
