import os
import json
import logging
import mimetypes
from time import time
from threading import Thread


KNOWN_MIMES = ["audio/mpeg", "audio/flac", "audio/x-wav", "image/jpeg", "image/png"]
logging = logging.getLogger("scanner")


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
            meta = json.loads(parent["metadata"])
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
                    logging.info("Added", os.path.join(path, create))
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
                    album_meta = self.library.db.get_metadata(album_dir["id"])
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

            logging.warning("Library scan complete in {}s".format(int(time() - start)))
