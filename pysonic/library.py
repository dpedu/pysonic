import os
import logging
from pysonic.scanner import PysonicFilesystemScanner
from pysonic.types import MUSIC_TYPES


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


class PysonicLibrary(object):
    def __init__(self, database):
        self.db = database

        self.get_libraries = self.db.get_libraries
        self.get_artists = self.db.get_artists
        self.get_albums = self.db.get_albums
        # self.get_song = self.db.get_song
        # self.get_cover = self.db.get_cover

        self.scanner = PysonicFilesystemScanner(self)
        logging.info("library ready")

    def update(self):
        """
        Start the library media scanner ands
        """
        self.scanner.init_scan()

    def add_root_dir(self, path):
        """
        The music library consists of a number of root dirs. This adds a new root
        """
        path = os.path.abspath(os.path.normpath(path))
        self.db.add_root(path)

    # def get_artists(self, *args, **kwargs):
    #     artists = self.db.get_artists(*args, **kwargs)
    #     for item in artists:
    #         item["parent"] = item["libraryid"]
    #     return artists

    # def get_albums(self, *args, **kwargs):
    #     albums = self.db.get_albums(*args, **kwargs)
    #     for item in albums:
    #         item["parent"] = item["artistid"]
    #     return albums

    def get_artist_info(self, item_id):
        #TODO
        return {"biography": "placeholder biography",
                "musicBrainzId": "playerholder",
                "lastFmUrl": "https://www.last.fm/music/Placeholder",
                "smallImageUrl": "",
                "mediumImageUrl": "",
                "largeImageUrl": "",
                "similarArtists": []}

    def get_cover(self, cover_id):
        cover = self.db.get_cover(cover_id)
        library = self.db.get_libraries(cover["library"])[0]
        cover['_fullpath'] = os.path.join(library["path"], cover["path"])
        return cover

    def get_song(self, song_id):
        song = self.db.get_songs(id=song_id)[0]
        library = self.db.get_libraries(song["library"])[0]
        song['_fullpath'] = os.path.join(library["path"], song["file"])
        return song

    def get_playlist(self, playlist_id):
        playlist_info = self.db.get_playlist(playlist_id)
        songs = self.db.get_playlist_songs(playlist_id)
        return (playlist_info, songs)

    def delete_playlist(self, playlist_id):
        self.db.empty_playlist(playlist_id)
        self.db.delete_playlist(playlist_id)
