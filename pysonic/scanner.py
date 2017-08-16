import os
import re
import logging
import mimetypes
from time import time
from threading import Thread
from pysonic.types import KNOWN_MIMES, MUSIC_TYPES
from mutagen.id3 import ID3
from mutagen import MutagenError
from mutagen.id3._util import ID3NoHeaderError


logging = logging.getLogger("scanner")
RE_NUMBERS = re.compile(r'^([0-9]+)')


class PysonicFilesystemScanner(object):
    def __init__(self, library):
        self.library = library

    def init_scan(self):
        self.scanner = Thread(target=self.rescan, daemon=True)
        self.scanner.start()

    def rescan(self):
        # Perform directory scan
        logging.warning("Beginning library rescan")
        start = time()
        for parent in self.library.get_libraries():
            meta = parent["metadata"]
            logging.info("Scanning {}".format(meta["fspath"]))

            def recurse_dir(path, parent):
                logging.info("Scanning {} with parent {}".format(path, parent))
                # create or update the database of nodes by comparing sets of names
                fs_entries = set(os.listdir(path))
                db_entires = self.library.db.getnodes(parent["id"])
                db_entires_names = set([i['name'] for i in db_entires])
                to_delete = db_entires_names - fs_entries
                to_create = fs_entries - db_entires_names

                # Create any nodes not found in the db
                for create in to_create:
                    new_node = self.library.db.addnode(parent["id"], path, create)
                    logging.info("Added {}".format(os.path.join(path, create)))
                    db_entires.append(new_node)

                # Delete any db nodes not found on disk
                for delete in to_delete:
                    logging.info("Prune ", delete, "in parent", path)
                    node = [i for i in db_entires if i["name"] == delete]
                    if node:
                        deleted = self.library.db.delnode(node[0]["id"])
                        logging.info("Pruned {}, deleting total of {}".format(node, deleted))

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
            for artist_dir in self.library.db.getnodes(parent["id"]):
                artist = artist_dir["name"]
                for album_dir in self.library.db.getnodes(artist_dir["id"]):
                    album = album_dir["name"]
                    album_meta = album_dir["metadata"]
                    for track_file in self.library.db.getnodes(album_dir["id"]):
                        title = track_file["name"]
                        if not track_file["title"]:
                            self.library.db.update_metadata(track_file["id"], artist=artist, album=album, title=title)
                            logging.info("Adding simple metadata for {}/{}/{} #{}".format(artist, album,
                                                                                          title, track_file["id"]))
                        if not album_dir["album"]:
                            self.library.db.update_metadata(album_dir["id"], artist=artist, album=album)
                            logging.info("Adding simple metadata for {}/{} #{}".format(artist, album, album_dir["id"]))
                        if not artist_dir["artist"]:
                            self.library.db.update_metadata(artist_dir["id"], artist=artist)
                            logging.info("Adding simple metadata for {} #{}".format(artist, artist_dir["id"]))
                        if title in ["cover.jpg", "cover.png"] and 'cover' not in album_meta:
                            # // add cover art
                            self.library.db.update_metadata(album_dir["id"], cover=track_file["id"])
                            logging.info("added cover for {}".format(album_dir['id']))

                        if track_file["type"] is None:
                            fpath = self.library.get_filepath(track_file['id'])
                            ftype, extra = mimetypes.guess_type(fpath)

                            if ftype in KNOWN_MIMES:
                                self.library.db.update_metadata(track_file["id"], type=ftype)
                                logging.info("added type {} for {}".format(ftype, track_file['id']))
                            else:
                                logging.warning("Ignoring unreadable file at {}, unknown ftype ({}, {})"
                                                .format(fpath, ftype, extra))
            #
            #
            #
            # Add advanced id3 metadata
            for artist_dir in self.library.db.getnodes(parent["id"]):
                artist = artist_dir["name"]
                for album_dir in self.library.db.getnodes(artist_dir["id"]):
                    album = album_dir["name"]
                    album_meta = album_dir["metadata"]
                    for track_file in self.library.db.getnodes(album_dir["id"]):
                        track_meta = track_file['metadata']
                        title = track_file["name"]
                        fpath = self.library.get_filepath(track_file["id"])
                        if track_meta.get('id3_done', False) or track_file.get("type", "x") not in MUSIC_TYPES:
                            continue
                        print("Mutagening", fpath)
                        tags = {'id3_done': True}
                        try:
                            id3 = ID3(fpath)
                            # print(id3.pprint())
                            try:
                                tags["track"] = int(RE_NUMBERS.findall(''.join(id3['TRCK'].text))[0])
                            except (KeyError, IndexError):
                                pass
                            try:
                                tags["id3_artist"] = ''.join(id3['TPE1'].text)
                            except KeyError:
                                pass
                            try:
                                tags["id3_album"] = ''.join(id3['TALB'].text)
                            except KeyError:
                                pass
                            try:
                                tags["id3_title"] = ''.join(id3['TIT2'].text)
                            except KeyError:
                                pass
                            try:
                                tags["id3_year"] = id3['TDRC'].text[0].year
                            except (KeyError, IndexError):
                                pass
                        except ID3NoHeaderError:
                            pass
                        except MutagenError as m:
                            logging.error(m)
                        self.library.db.update_metadata(track_file["id"], **tags)

            logging.warning("Library scan complete in {}s".format(int(time() - start)))
