import os
import json
from threading import Thread


class PysonicFilesystemScanner(object):
    def __init__(self, library):
        self.library = library

    def init_scan(self):
        self.scanner = Thread(target=self.rescan, daemon=True)
        self.scanner.start()

    def rescan(self):
        # Perform directory scan
        for parent in self.library.get_libraries():
            meta = json.loads(parent["metadata"])
            # print("Scanning {}".format(meta["fspath"]))

            def recurse_dir(path, parent):
                # print("Scanning {} with parent {}".format(path, parent))
                # create or update the database of nodes by comparing sets of names
                fs_entries = set(os.listdir(path))
                db_entires = self.library.db.getnodes(parent["id"])
                db_entires_names = set([i['name'] for i in db_entires])
                to_delete = db_entires_names - fs_entries
                to_create = fs_entries - db_entires_names

                # Create any nodes not found in the db
                for create in to_create:
                    new_node = self.library.db.addnode(parent, path, create)
                    db_entires.append(new_node)

                # Delete any db nodes not found on disk
                for delete in to_delete:
                    print("Prune ", delete, "in parent", path)
                    node = [i for i in db_entires if i["name"] == delete]
                    if node:
                        deleted = self.library.db.delnode(node[0]["id"])
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
            for artist_dir in self.library.db.getnodes(parent["id"]):
                artist = artist_dir["name"]
                for album_dir in self.library.db.getnodes(artist_dir["id"]):
                    album = album_dir["name"]
                    album_meta = self.library.db.get_metadata(album_dir["id"])
                    for track_file in self.library.db.getnodes(album_dir["id"]):
                        title = track_file["name"]
                        if not track_file["title"]:
                            self.library.db.update_metadata(track_file["id"], artist=artist, album=album, title=title)
                            print("Adding simple metadata for {}/{}/{} #{}".format(artist, album,
                                                                                   title, track_file["id"]))
                        if not album_dir["album"]:
                            self.library.db.update_metadata(album_dir["id"], artist=artist, album=album)
                            print("Adding simple metadata for {}/{} #{}".format(artist, album, album_dir["id"]))
                        if not artist_dir["artist"]:
                            self.library.db.update_metadata(artist_dir["id"], artist=artist)
                            print("Adding simple metadata for {} #{}".format(artist, artist_dir["id"]))
                        if title == "cover.jpg" and 'cover' not in album_meta:
                            # // add cover art
                            self.library.db.update_metadata(album_dir["id"], cover=track_file["id"])
                            print("added cover for {}".format(album_dir['id']))
            print("Metadata scan complete.")
