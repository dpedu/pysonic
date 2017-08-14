import sys
import logging
import cherrypy
import subprocess
from time import time
from random import shuffle
from bs4 import BeautifulSoup
from pysonic.library import LETTER_GROUPS
from pysonic.types import MUSIC_TYPES


logging = logging.getLogger("api")


class PysonicApi(object):
    def __init__(self, db, library, options):
        self.db = db
        self.library = library
        self.options = options

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
                                licenseExpires="2100-01-01T00:00:00.000Z",
                                trialExpires="2100-01-01T01:01:00.000Z"))
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
                if artist["name"][0].lower() in letter:
                    artist_tag = doc.new_tag("artist")
                    artist_tag.attrs.update({"id": artist["id"], "name": artist["name"]})
                    index.append(artist_tag)
        yield doc.prettify()

    @cherrypy.expose
    def savePlayQueue_view(self, id, current, position, **kwargs):
        # /rest/savePlayQueue.view?
        # u=dave&
        # s=h7vcg97gm2vbb7m4133pavs1ot&
        # t=355f45124d9d3a75fe681c11d94ed066&
        # v=1.2.0&
        # c=DSub&
        # id=296&
        # id=289&
        # id=292&id=287&id=288&id=290&id=293&id=294&id=297&id=298&id=291&
        # current=297&
        # position=0
        print("TODO save playlist with items {} current {} position {}".format(id, current, position))

    @cherrypy.expose
    def getAlbumList_view(self, type, size=50, offset=0, **kwargs):
        albums = self.library.get_albums()
        if type == "random":
            shuffle(albums)
        elif type == "alphabeticalByName":
            albums.sort(key=lambda item: item.get("id3_album", item["album"]))
        else:
            raise NotImplemented()
        albumset = albums[0 + int(offset):int(size) + int(offset)]

        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        albumlist = doc.new_tag("albumList")
        doc.append(albumlist)

        for album in albumset:
            album_meta = self.library.db.decode_metadata(album['metadata'])
            tag = doc.new_tag("album",
                              id=album["id"],
                              parent=album["parent"],
                              isDir="true" if album['isdir'] else "false",
                              title=album_meta.get("id3_title", album["name"]),
                              album=album_meta.get("id3_album", album["album"]),
                              artist=album_meta.get("id3_artist", album["artist"]),
                              # X year="2014"
                              # X coverArt="3228"
                              # playCount="0"
                              # created="2016-05-08T05:31:31.000Z"/>
                              )
            if 'cover' in album_meta:
                tag.attrs["coverArt"] = album_meta["cover"]
            if 'id3_year' in album_meta:
                tag.attrs["year"] = album_meta['id3_year']
            albumlist.append(tag)
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
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = self.db.decode_metadata(item['metadata'])
            child = doc.new_tag("child",
                                id=item["id"],
                                parent=directory["id"],
                                isDir="true" if item['isdir'] else "false",
                                title=item_meta.get("id3_title", item["name"]),
                                album=item_meta.get("id3_album", item["album"]),
                                artist=item_meta.get("id3_artist", item["artist"]),
                                # playCount="5",
                                # created="2016-04-25T07:31:33.000Z"
                                # X track="3",
                                # X year="2012",
                                # X coverArt="12835",
                                # X contentType="audio/mpeg"
                                # X suffix="mp3"
                                # genre="Other",
                                # size="15838864"
                                # duration="395"
                                # bitRate="320"
                                # path="Cosmic Gate/Sign Of The Times/03 Flatline (featuring Kyler England).mp3"
                                albumId=directory["id"],
                                artistId=directory["parent"],
                                type="music")
            if "." in item["name"]:
                child.attrs["suffix"] = item["name"].split(".")[-1]
            if item["type"]:
                child.attrs["contentType"] = item["type"]
            if 'cover' in item_meta:
                child.attrs["coverArt"] = item_meta["cover"]
            elif 'cover' in dir_meta:
                child.attrs["coverArt"] = dir_meta["cover"]
            if 'track' in item_meta:
                child.attrs["track"] = item_meta['track']
            if 'id3_year' in item_meta:
                child.attrs["year"] = item_meta['id3_year']
            dirtag.append(child)
        yield doc.prettify()

    @cherrypy.expose
    def stream_view(self, id, maxBitRate="256", **kwargs):
        maxBitRate = int(maxBitRate)
        assert maxBitRate >= 32 and maxBitRate <= 320
        fpath = self.library.get_filepath(id)
        meta = self.library.get_file_metadata(id)
        cherrypy.response.headers['Content-Type'] = 'audio/mpeg'
        if self.options.skip_transcode and meta["type"] == "audio/mpeg":
            def content():
                with open(fpath, "rb") as f:
                    while True:
                        data = f.read(16 * 1024)
                        if not data:
                            break
                        yield data
        else:
            def content():
                transcode_args = ["ffmpeg", "-i", fpath, "-map", "0:0", "-b:a",
                                  "{}k".format(min(maxBitRate, self.options.max_bitrate)),
                                  "-v", "0", "-f", "mp3", "-"]
                logging.info(' '.join(transcode_args))
                start = time()
                proc = subprocess.Popen(transcode_args, stdout=subprocess.PIPE)
                while True:
                    data = proc.stdout.read(16 * 1024)
                    if not data:
                        break
                    yield data
                logging.warning("transcoded {} in {}s".format(id, int(time() - start)))
        return content()
    stream_view._cp_config = {'response.stream': True}

    @cherrypy.expose
    def getCoverArt_view(self, id, **kwargs):
        # /rest/getCoverArt.view?u=dave&s=bfk9mir8is02u3m5as8ucsehn0
        # &t=e2b09fb9233d1bfac9abe3dc73017f1e&v=1.2.0&c=DSub&id=12833
        fpath = self.library.get_filepath(id)
        type2ct = {
            'jpg': 'image/jpeg',
            'png': 'image/png'
        }
        cherrypy.response.headers['Content-Type'] = type2ct[fpath[-3:]]

        def content():
            total = 0
            with open(fpath, "rb") as f:
                while True:
                    data = f.read(8192)
                    if not data:
                        break
                    total += len(data)
                    yield data
            logging.info("\nSent {} bytes for {}".format(total, fpath))
        return content()

    getCoverArt_view._cp_config = {'response.stream': True}

    @cherrypy.expose
    def getArtistInfo_view(self, id, includeNotPresent="true", **kwargs):
        # /rest/getArtistInfo.view?
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
