import os
import re
import logging
from contextlib import closing
import mimetypes
from time import time
from threading import Thread
from pysonic.types import KNOWN_MIMES, MUSIC_TYPES, MPX_TYPES, FLAC_TYPES, WAV_TYPES, MUSIC_EXTENSIONS, IMAGE_EXTENSIONS, IMAGE_TYPES
from mutagen.id3 import ID3
from mutagen import MutagenError
from mutagen.id3._util import ID3NoHeaderError
from mutagen.flac import FLAC
from mutagen.mp3 import MP3


logging = logging.getLogger("scanner")
RE_NUMBERS = re.compile(r'^([0-9]+)')


class PysonicFilesystemScanner(object):
    def __init__(self, library):
        self.library = library

    def init_scan(self):
        self.scanner = Thread(target=self.rescan, daemon=True)
        self.scanner.start()

    def rescan(self):
        """
        Perform a full scan of the media library's files
        """
        start = time()
        logging.warning("Beginning library rescan")
        for parent in self.library.db.get_libraries():
            logging.info("Scanning {}".format(parent["path"]))
            self.scan_root(parent["id"], parent["path"])
        logging.warning("Rescan complete in %ss", round(time() - start, 3))

    def scan_root(self, pid, root):
        """
        Scan a single root the library
        :param pid: parent ID
        :param root: absolute path to scan
        """
        logging.warning("Beginning file scan for library %s", pid)
        root_depth = len(self.split_path(root))
        for path, dirs, files in os.walk(root):
            child = self.split_path(path)[root_depth:]
            # dirid = self.create_or_get_dbdir_tree(pid, child)  # dumb table for Subsonic
            self.scan_dir(pid, root, child, dirs, files)

        logging.warning("Beginning metadata scan for library %s", pid)
        self.scan_metadata(pid, root, freshonly=True)

        logging.warning("Finished scan for library %s", pid)

    def create_or_get_dbdir_tree(self, cursor, pid, path):
        """
        Return the ID of the directory specified by `path`. The path will be created as necessary. This bullshit exists
        only to serve Subsonic, and can easily be lopped off.
        :param pid: root parent the path resides in
        :param path: single-file tree as a list of dir names under the root parent
        :type path list
        """
        assert path
        # with closing(self.library.db.db.cursor()) as cursor:
        parent_id = 0  # 0 indicates a top level item in the library
        for name in path:
            parent_id = self.create_or_get_dbdir(cursor, pid, parent_id, name)
        return parent_id

    def create_or_get_dbdir(self, cursor, pid, parent_id, name):
        for row in cursor.execute("SELECT * FROM dirs WHERE library=? and parent=? and name=?",
                                  (pid, parent_id, name, )):
            return row['id']
        cursor.execute("INSERT INTO dirs (library, parent, name) VALUES (?, ?, ?)", (pid, parent_id, name))
        return cursor.lastrowid

    def scan_dir(self, pid, root, path, dirs, files):
        """
        Scan a single directory in the library. Actually, this ignores all dirs that don't contain files. Dirs are
        interpreted as follows:
        - The library root is ignored
        - Empty dirs are ignored
        - Dirs containing files are assumed to be an album
        - Top level dirs in the library are assumed to be artists
        - Any dirs not following the above rules are transparently ignored
        - Files placed in an artist dir is an unhandled edge case TODO
        - Any files with an image extension in an album dir will be assumed to be the cover regardless of naming
        - TODO ignore dotfiles/dirs
        TODO remove all file scanning / statting etc from paths where a db transaction is active (gather data then open)
        :param pid: parent id
        :param root: library root path
        :param path: scan location path, as a list of subdirs within the root
        :param dirs: dirs in the current path
        :param files: files in the current path
        """
        # If this is the library root or an empty dir just bail
        if not path or not files:
            return
        # If it is the library root just bail
        if len(path) == 0:
            return

        logging.info("In library %s scanning %s", pid, os.path.join(*path))

        # Guess an album from the dir, if possible
        album = None
        if len(path) > 1:
            album = path[-1]

        with closing(self.library.db.db.cursor()) as cursor:
            artist_id, artist_dirid = self.create_or_get_artist(cursor, pid, path[0])

            album_id = None
            album_dirid = None
            if album:
                album_id, album_dirid = self.create_or_get_album(cursor, pid, path, artist_id)

            libpath = os.path.join(*path)

            new_files = False
            for fname in files:
                if not any([fname.endswith(".{}".format(i)) for i in MUSIC_EXTENSIONS]):
                    continue
                new_files = self.add_music_if_new(cursor, pid, root, album_id, libpath, fname) or new_files

            # Create cover entry TODO we can probably skip this if there were no new audio files?
            if album_id:
                for file in files:
                    if not any([file.endswith(".{}".format(i)) for i in IMAGE_EXTENSIONS]):
                        continue
                    fpath = os.path.join(libpath, file)
                    cursor.execute("SELECT id FROM covers WHERE path=?", (fpath, ))
                    if not cursor.fetchall():
                        # We leave most fields blank now and return later
                        cursor.execute("INSERT INTO covers (library, path) VALUES (?, ?);", (pid, fpath, ))
                        cursor.execute("UPDATE albums SET coverid=? WHERE id=?", (cursor.lastrowid, album_id))
                    break

            if new_files:  # Commit after each dir IF audio files were found. no audio == dump the artist
                cursor.execute("COMMIT")

    def add_music_if_new(self, cursor, pid, root_dir, album_id, fdir, fname):
        fpath = os.path.join(fdir, fname)
        cursor.execute("SELECT id FROM songs WHERE file=?", (fpath, ))
        if not cursor.fetchall():
            # We leave most fields blank now and return later
            # TODO probably not here but track file sizes and mark them for rescan on change
            cursor.execute("INSERT INTO songs (library, albumid, file, size, title) "
                           "VALUES (?, ?, ?, ?, ?)",
                           (pid,
                            album_id,
                            fpath,
                            os.stat(os.path.join(root_dir, fpath)).st_size,
                            fname, ))
            return True
        return False

    def create_or_get_artist(self, cursor, pid, dirname):
        """
        Retrieve, creating if necessary, directory information about an artist. Return tuple contains the artist's ID
        and the dir id associated with the artist.
        :param cursor: sqlite cursor to use
        :param pid: root parent id we're working int
        :param dirname: name of the artist dir
        :return tuple:
        """
        artist_dirid = self.create_or_get_dbdir_tree(cursor, pid, [dirname])
        cursor.execute("SELECT * FROM artists WHERE dir = ?", (artist_dirid, ))
        row = cursor.fetchone()
        artist_id = None
        if row:
            artist_id = row['id']
        else:
            cursor.execute("INSERT INTO artists (libraryid, dir, name) VALUES (?, ?, ?)",
                           (pid, artist_dirid, dirname))
            artist_id = cursor.lastrowid
        return artist_id, artist_dirid

    def create_or_get_album(self, cursor, pid, dirnames, artist_id):
        """
        Retrieve, creating if necessary, directory information about an album. Return tuple contains the albums's ID
        and the dir id associated with the album.
        :param cursor: sqlite cursor to use
        :param pid: root parent id we're working int
        :param dirnames: list of directories from the root to the album dir
        :param artist_id: id of the artist the album belongs to
        :return tuple:
        """
        album_dirid = self.create_or_get_dbdir_tree(cursor, pid, dirnames)
        cursor.execute("SELECT * FROM albums WHERE artistid = ? AND dir = ?", (artist_id, album_dirid, ))
        row = cursor.fetchone()
        if row:
            album_id = row['id']
        else:
            cursor.execute("INSERT INTO albums (artistid, dir, name, added) VALUES (?, ?, ?, ?)",
                           (artist_id, album_dirid, dirnames[-1], int(time())))
            album_id = cursor.lastrowid

        return album_id, album_dirid

    def split_path(self, path):
        """
        Given a path like /foo/bar, return ['foo', 'bar']
        """
        parts = []
        head = path
        while True:
            head, tail = os.path.split(head)
            if tail:
                parts.append(tail)
            else:
                break
        parts.reverse()
        return parts

    def scan_metadata(self, pid, root, freshonly=False):
        """
        Iterate through files in the library and update metadata
        :param freshonly: only update metadata on files that have never been scanned before
        """
        q = "SELECT * FROM songs "
        if freshonly:
            q += "WHERE lastscan = -1 "
        q += "ORDER BY albumid"

        #TODO scraping ID3 etc from the media files can be parallelized
        with closing(self.library.db.db.cursor()) as reader, \
                closing(self.library.db.db.cursor()) as writer:
            processed = 0  # commit batching counter
            for row in reader.execute(q):
                # Find meta, bail if the file was unreadable
                # TODO file metadata scanning could be done in parallel
                meta = self.scan_file_metadata(os.path.join(root, row['file']))
                if not meta:
                    continue
                # Meta may have additional keys that arent in the songs table, omit them
                song_attrs = ["title", "lastscan", "format", "length", "bitrate", "track", "year"]
                song_meta = {k: v for k, v in meta.items() if k in song_attrs}

                # Update the song row
                q = "UPDATE songs SET "
                params = []
                for key, value in song_meta.items():
                    q += "{}=?, ".format(key)
                    params.append(value)
                q += "lastscan=? WHERE id=?"
                params += [int(time()), row["id"]]
                writer.execute(q, params)

                # If the metadata has an artist or album name, update the relevant items
                # TODO ignore metadata if theyre blank
                if "album" in meta:
                    writer.execute("UPDATE albums SET name=? WHERE id=?", (meta["album"], row["albumid"]))
                if "artist" in meta:
                    album = writer.execute("SELECT artistid FROM albums WHERE id=?", (row['albumid'], )).fetchone()
                    if album:
                        writer.execute("UPDATE artists SET name=? WHERE id=?", (meta["artist"], album["artistid"]))
                if "genre" in meta:
                    genre_name = meta["genre"].strip()
                    if genre_name:
                        genre_id = self.get_genre_id(writer, meta["genre"])
                        writer.execute("UPDATE songs SET genre=? WHERE id=?", (genre_id, row['id']))

                # Commit every 50 items
                processed += 1
                if processed > 50:
                    writer.execute("COMMIT")
                    processed = 0

            if processed != 0:
                writer.execute("COMMIT")

    def get_genre_id(self, cursor, genre_name):
        genre_name = genre_name.title().strip()  # normalize
        for row in cursor.execute("SELECT * FROM genres WHERE name=?", (genre_name, )):
            return row['id']
        cursor.execute("INSERT INTO genres (name) VALUES (?)", (genre_name, ))
        return cursor.lastrowid

    def scan_file_metadata(self, fpath):
        """
        Scan the file for metadata.
        :param fpath: path to the file to scan
        """
        ftype, extra = mimetypes.guess_type(fpath)

        if ftype in MUSIC_TYPES:
            return self.scan_mutagen_metadata(fpath, ftype)

    def scan_mutagen_metadata(self, fpath, ftype):
        meta = {"format": ftype}
        try:
            # Open file with mutagen
            if ftype in MPX_TYPES:
                audio = MP3(fpath)
                if audio.info.sketchy:
                    logging.warning("media reported as sketchy: %s", fpath)
            elif ftype in FLAC_TYPES:
                audio = FLAC(fpath)
            else:
                audio = ID3(fpath)
        except ID3NoHeaderError:
            return
        except MutagenError as m:
            logging.error("failed to read audio information: %s", m)
            return

        try:
            meta["length"] = int(audio.info.length)
        except (ValueError, AttributeError):
            pass
        try:
            bitrate = int(audio.info.bitrate)
            meta["bitrate"] = bitrate
            # meta["kbitrate"] = int(bitrate / 1024)
        except (ValueError, AttributeError):
            pass
        try:
            meta["track"] = int(RE_NUMBERS.findall(''.join(audio['TRCK'].text))[0])
        except (KeyError, IndexError):
            pass
        try:
            meta["artist"] = ''.join(audio['TPE1'].text)
        except KeyError:
            pass
        try:
            meta["album"] = ''.join(audio['TALB'].text)
        except KeyError:
            pass
        try:
            meta["title"] = ''.join(audio['TIT2'].text)
        except KeyError:
            pass
        try:
            meta["year"] = audio['TDRC'].text[0].year
        except (KeyError, IndexError):
            pass
        try:
            meta["genre"] = audio['TCON'].text[0]
        except (KeyError, IndexError):
            pass
        logging.info("got all media info from %s", fpath)

        return meta
