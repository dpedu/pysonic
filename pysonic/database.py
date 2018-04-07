import sqlite3
import logging
from hashlib import sha512
from time import time
from contextlib import closing
from collections import Iterable

logging = logging.getLogger("database")
keys_in_table = ["title", "album", "artist", "type", "size"]


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class NotFoundError(Exception):
    pass


class DuplicateRootException(Exception):
    pass


def hash_password(unicode_string):
        return sha512(unicode_string.encode('UTF-8')).hexdigest()


def readcursor(func):
    """
    Provides a cursor to the wrapped method as the first arg.
    """
    def wrapped(*args, **kwargs):
        self = args[0]
        if len(args) >= 2 and isinstance(args[1], sqlite3.Cursor):
            return func(*args, **kwargs)
        else:
            with closing(self.db.cursor()) as cursor:
                return func(*[self, cursor], *args[1:], **kwargs)
    return wrapped


class PysonicDatabase(object):
    def __init__(self, path):
        self.sqlite_opts = dict(check_same_thread=False)
        self.path = path
        self.db = None
        self.open()
        self.migrate()

    def open(self):
        self.db = sqlite3.connect(self.path, **self.sqlite_opts)
        self.db.row_factory = dict_factory

    def migrate(self):
        # Create db
        queries = ["""CREATE TABLE 'libraries' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'name'      TEXT,
                        'path'      TEXT UNIQUE);""",
                   """CREATE TABLE 'dirs' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'library'   INTEGER,
                        'parent'    INTEGER,
                        'name'      TEXT,
                        UNIQUE(parent, name)
                        )""",
                   """CREATE TABLE 'genres' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'name'      TEXT UNIQUE)""",
                   """CREATE TABLE 'artists' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'libraryid' INTEGER,
                        'dir'       INTEGER UNIQUE,
                        'name'      TEXT)""",
                   """CREATE TABLE 'albums' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'artistid'  INTEGER,
                        'coverid'   INTEGER,
                        'dir'       INTEGER,
                        'name'      TEXT,
                        'added'     INTEGER NOT NULL DEFAULT -1,
                         UNIQUE (artistid, dir));""",
                   """CREATE TABLE 'songs' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'library'   INTEGER,
                        'albumid'   BOOLEAN,
                        'genre'     INTEGER DEFAULT NULL,
                        'file'      TEXT UNIQUE,  -- path from the library root
                        'size'      INTEGER NOT NULL DEFAULT -1,
                        'title'     TEXT NOT NULL,
                        'lastscan'  INTEGER NOT NULL DEFAULT -1,
                        'format'    TEXT,
                        'length'    INTEGER,
                        'bitrate'   INTEGER,
                        'track'     INTEGER,
                        'year'      INTEGER
                        )""",
                   """CREATE TABLE 'covers' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT,
                        'library'   INTEGER,
                        'type'      TEXT,
                        'size'      TEXT,
                        'path'      TEXT UNIQUE);""",
                   """CREATE TABLE 'users' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        'username'  TEXT UNIQUE NOT NULL,
                        'password'  TEXT NOT NULL,
                        'admin'     BOOLEAN DEFAULT 0,
                        'email'     TEXT)""",
                   """CREATE TABLE 'stars' (
                        'userid'    INTEGER,
                        'songid'    INTEGER,
                        primary key ('userid', 'songid'))""",
                   """CREATE TABLE 'playlists' (
                        'id'        INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        'ownerid'   INTEGER,
                        'name'      TEXT,
                        'public'    BOOLEAN,
                        'created'   INTEGER,
                        'changed'   INTEGER,
                        'cover'     INTEGER,
                        UNIQUE ('ownerid', 'name'))""",
                   """CREATE TABLE 'playlist_entries' (
                        'playlistid'    INTEGER,
                        'songid'        INTEGER,
                        'order'         FLOAT)""",
                   """CREATE TABLE 'meta' (
                        'key' TEXT PRIMARY KEY NOT NULL,
                        'value' TEXT);""",
                   """INSERT INTO meta VALUES ('db_version', '1');"""]

        with closing(self.db.cursor()) as cursor:
            cursor.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='meta'")

            # Initialize DB
            if len(cursor.fetchall()) == 0:
                logging.warning("Initializing database")
                for query in queries:
                    cursor.execute(query)
                cursor.execute("COMMIT")
            else:
                # Migrate if old db exists
                # cursor.execute("""UPDATE meta SET value=? WHERE key="db_version";""", (str(version), ))
                # logging.warning("db schema is version {}".format(version))
                pass

    @readcursor
    def get_stats(self, cursor):
        songs = cursor.execute("SELECT COUNT(*) as cnt FROM songs").fetchone()['cnt']
        artists = cursor.execute("SELECT COUNT(*) as cnt FROM artists").fetchone()['cnt']
        albums = cursor.execute("SELECT COUNT(*) as cnt FROM albums").fetchone()['cnt']
        return dict(songs=songs, artists=artists, albums=albums)

    # Music related
    @readcursor
    def add_root(self, cursor, path, name="Library"):
        """
        Add a new library root. Returns the root ID or raises on collision
        :param path: normalized absolute path to add to the library
        :type path: str:
        :return: int
        :raises: sqlite3.IntegrityError
        """
        assert path.startswith("/")
        try:
            cursor.execute("INSERT INTO libraries ('name', 'path') VALUES (?, ?)", (name, path, ))
            cursor.execute("COMMIT")
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            raise DuplicateRootException("Root '{}' already exists".format(path))

    @readcursor
    def get_libraries(self, cursor, id=None):
        libs = []
        q = "SELECT * FROM libraries"
        params = []
        conditions = []
        if id:
            conditions.append("id = ?")
            params.append(id)
        if conditions:
            q += " WHERE " + " AND ".join(conditions)
        cursor.execute(q, params)
        for row in cursor:
            libs.append(row)
        return libs

    @readcursor
    def get_artists(self, cursor, id=None, dirid=None, sortby=None, order=None):
        assert order in ["asc", "desc", None]
        artists = []
        q = "SELECT * FROM artists"
        params = []
        conditions = []
        if id:
            conditions.append("id = ?")
            params.append(id)
        if dirid:
            conditions.append("dir = ?")
            params.append(dirid)
        if conditions:
            q += " WHERE " + " AND ".join(conditions)
        if sortby:
            q += " ORDER BY {} {}".format(sortby, order.upper() if order else "ASC")
        cursor.execute(q, params)
        for row in cursor:
            artists.append(row)
        return artists

    @readcursor
    def get_albums(self, cursor, id=None, artist=None, sortby=None, order=None, limit=None):
        """
        :param limit: int or tuple of int, int. translates directly to sql logic.
        """
        if order:
            order = {"asc": "ASC", "desc": "DESC"}[order]

        if sortby and sortby == "random":
            sortby = "RANDOM()"

        albums = []

        q = """
            SELECT
                alb.*,
                art.name as artistname,
                dirs.parent as artistdir
            FROM albums as alb
                INNER JOIN artists as art
                    on alb.artistid = art.id
                INNER JOIN dirs
                    on dirs.id = alb.dir
            """
        params = []

        conditions = []
        if id:
            conditions.append("id = ?")
            params.append(id)
        if artist:
            conditions.append("artistid = ?")
            params.append(artist)
        if conditions:
            q += " WHERE " + " AND ".join(conditions)

        if sortby:
            q += " ORDER BY {}".format(sortby)
            if order:
                q += " {}".format(order)

        if limit:
            q += " LIMIT {}".format(limit) if isinstance(limit, int) \
                else " LIMIT {}, {}".format(*limit)

        cursor.execute(q, params)
        for row in cursor:
            albums.append(row)
        return albums

    @readcursor
    def get_songs(self, cursor, id=None, genre=None, sortby=None, order=None, limit=None):
        # TODO make this query massively uglier by joining albums and artists so that artistid etc can be a filter
        # or maybe lookup those IDs in the library layer?
        if order:
            order = {"asc": "ASC", "desc": "DESC"}[order]

        if sortby and sortby == "random":
            sortby = "RANDOM()"

        songs = []

        q = """
            SELECT
                s.*,
                alb.name as albumname,
                alb.coverid as albumcoverid,
                art.name as artistname,
                g.name as genrename
            FROM songs as s
                INNER JOIN albums as alb
                    on s.albumid == alb.id
                INNER JOIN artists as art
                    on alb.artistid = art.id
                LEFT JOIN genres as g
                    on s.genre == g.id
            """

        params = []

        conditions = []
        if id and isinstance(id, int):
            conditions.append("s.id = ?")
            params.append(id)
        elif id and isinstance(id, Iterable):
            conditions.append("s.id IN ({})".format(",".join("?" * len(id))))
            params += id
        if genre:
            conditions.append("g.name = ?")
            params.append(genre)
        if conditions:
            q += " WHERE " + " AND ".join(conditions)

        if sortby:
            q += " ORDER BY {}".format(sortby)
            if order:
                q += " {}".format(order)

        if limit:
            q += " LIMIT {}".format(limit)  # TODO support limit pagination

        cursor.execute(q, params)
        for row in cursor:
            songs.append(row)
        return songs

    @readcursor
    def get_genres(self, cursor, genre_id=None):
        genres = []
        q = "SELECT * FROM genres"
        params = []
        conditions = []
        if genre_id:
            conditions.append("id = ?")
            params.append(genre_id)
        if conditions:
            q += " WHERE " + " AND ".join(conditions)
        cursor.execute(q, params)
        for row in cursor:
            genres.append(row)
        return genres

    @readcursor
    def get_cover(self, cursor, coverid):
        cover = None
        for cover in cursor.execute("SELECT * FROM covers WHERE id = ?", (coverid, )):
            return cover

    @readcursor
    def get_subsonic_musicdir(self, cursor, dirid):
        """
        The world is a harsh place.
        Again, this bullshit exists only to serve subsonic clients. Given a directory ID it returns a dict containing:
        - the directory itself
        - its parent
        - its child dirs
        - its child media

        that's a lie, it's a tuple and it's full of BS. read the code
        """
        # find directory
        dirinfo = None
        for dirinfo in cursor.execute("SELECT * FROM dirs WHERE id = ?", (dirid, )):
            pass
        assert dirinfo

        ret = None

        # see if it matches the artists or albums table
        artist = None
        for artist in cursor.execute("SELECT * FROM artists WHERE dir = ?", (dirid, )):
            pass

        # if artist:
        #   get child albums
        if artist:
            ret = ("artist", dirinfo, artist)
            children = []
            for album in cursor.execute("SELECT * FROM albums WHERE artistid = ?", (artist["id"], )):
                children.append(("album", album))
            ret[2]['children'] = children
            return ret

        # else if album:
        #   get child tracks
        album = None
        for album in cursor.execute("SELECT * FROM albums WHERE dir = ?", (dirid, )):
            pass
        if album:
            ret = ("album", dirinfo, album)

            artist_info = cursor.execute("SELECT * FROM artists WHERE id = ?", (album["artistid"], )).fetchall()[0]

            children = []
            for song in cursor.execute("SELECT * FROM songs WHERE albumid = ?", (album["id"], )):
                song["_artist"] = artist_info
                children.append(("song", song))
            ret[2]['children'] = children
            return ret

    # Playlist related
    @readcursor
    def add_playlist(self, cursor, ownerid, name, song_ids, public=False):
        """
        Create a playlist
        """
        now = time()
        cursor.execute("INSERT INTO playlists (ownerid, name, public, created, changed) VALUES (?, ?, ?, ?, ?)",
                       (ownerid, name, public, now, now))
        plid = cursor.lastrowid
        for song_id in song_ids:
            self.add_to_playlist(cursor, plid, song_id)
        cursor.execute("COMMIT")

    @readcursor
    def add_to_playlist(self, cursor, playlist_id, song_id):
        # TODO deal with order column
        cursor.execute("INSERT INTO playlist_entries (playlistid, songid) VALUES (?, ?)", (playlist_id, song_id))

    @readcursor
    def get_playlist(self, cursor, playlist_id):
        return cursor.execute("SELECT * FROM playlists WHERE id=?", (playlist_id, )).fetchone()

    @readcursor
    def get_playlist_songs(self, cursor, playlist_id):
        songs = []
        q = """
            SELECT
                s.*,
                alb.name as albumname,
                alb.coverid as albumcoverid,
                art.name as artistname,
                art.name as artistid,
                g.name as genrename
            FROM playlist_entries as pe
                INNER JOIN songs as s
                    on pe.songid == s.id
                INNER JOIN albums as alb
                    on s.albumid == alb.id
                INNER JOIN artists as art
                    on alb.artistid = art.id
                LEFT JOIN genres as g
                    on s.genre == g.id
            WHERE pe.playlistid = ?
            ORDER BY pe.'order' ASC;
        """
        for row in cursor.execute(q, (playlist_id, )):
            songs.append(row)
        return songs

    @readcursor
    def get_playlists(self, cursor, user_id):
        playlists = []
        for row in cursor.execute("SELECT * FROM playlists WHERE ownerid=? or public=1", (user_id, )):
            playlists.append(row)
        return playlists

    @readcursor
    def remove_index_from_playlist(self, cursor, playlist_id, index):
        cursor.execute("DELETE FROM playlist_entries WHERE playlistid=? LIMIT ?, 1", (playlist_id, index, ))
        cursor.execute("COMMIT")

    @readcursor
    def empty_playlist(self, cursor, playlist_id):
        #TODO combine with # TODO combine with
        cursor.execute("DELETE FROM playlist_entries WHERE playlistid=?", (playlist_id, ))
        cursor.execute("COMMIT")

    @readcursor
    def delete_playlist(self, cursor, playlist_id):
        cursor.execute("DELETE FROM playlists WHERE id=?", (playlist_id, ))
        cursor.execute("COMMIT")

    # User related
    @readcursor
    def add_user(self, cursor, username, password, is_admin=False):
        cursor.execute("INSERT INTO users (username, password, admin) VALUES (?, ?, ?)",
                       (username, hash_password(password), is_admin))
        cursor.execute("COMMIT")

    @readcursor
    def update_user(self, cursor, username, password, is_admin=False):
        cursor.execute("UPDATE users SET password=?, admin=? WHERE username=?;",
                       (hash_password(password), is_admin, username))
        cursor.execute("COMMIT")

    @readcursor
    def get_user(self, cursor, user):
        try:
            column = "id" if type(user) is int else "username"
            return cursor.execute("SELECT * FROM users WHERE {}=?;".format(column), (user, )).fetchall()[0]
        except IndexError:
            raise NotFoundError("User doesn't exist")
