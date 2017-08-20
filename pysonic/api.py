import json
import logging
import subprocess
from time import time
from random import shuffle
from threading import Thread
import cherrypy
from collections import defaultdict
from bs4 import BeautifulSoup
from pysonic.library import LETTER_GROUPS
from pysonic.types import MUSIC_TYPES


logging = logging.getLogger("api")


class ApiResponse(object):
    def __init__(self, status="ok", version="1.15.0", top=None, **kwargs):
        self.status = status
        self.version = version
        self.data = {}
        self.top = top
        if self.top:
            self.data[self.top] = kwargs ## kwargs unused TODO

    def add_child(self, _type, **kwargs):
        if not self.top:
            raise Exception("You can't do this?")
        if _type not in self.data[self.top]:
            self.data[self.top][_type] = []
        self.data[self.top][_type].append(kwargs)

    def render_json(self):
        return json.dumps({"subsonic-response": dict(status=self.status, version="1.15.0", **self.data)}, indent=4)

    def render_xml(self):
        doc = BeautifulSoup('', features='lxml-xml')
        root = doc.new_tag("subsonic-response", xmlns="http://subsonic.org/restapi",
                           status=self.status,
                           version=self.version)
        doc.append(root)

        if self.top:
            top = doc.new_tag(self.top)
            root.append(top)
            # TODO top_attrs ?
            for top_child_type, top_child_instances in self.data[self.top].items():
                for top_child_attrs in top_child_instances:
                    child = doc.new_tag(top_child_type)
                    child.attrs.update(top_child_attrs)
                    top.append(child)

        albumlist = doc.new_tag("albumList")
        doc.append(albumlist)
        return doc.prettify()


response_formats = defaultdict(lambda: "render_xml")
response_formats["json"] = "render_json"

response_headers = defaultdict(lambda: "text/xml; charset=utf-8")
response_headers["json"] = "text/json" #TODO is this right?


def formatresponse(func):
    """
    Decorator for rendering ApiResponse responses
    """
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        cherrypy.response.headers['Content-Type'] = response_headers[kwargs.get("f", "xml")]
        return getattr(response, response_formats[kwargs.get("f", "xml")])()
    return wrapper


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
    @formatresponse
    def getAlbumList_view(self, type, size=50, offset=0, **kwargs):
        albums = self.library.get_albums()
        if type == "random":
            shuffle(albums)
        elif type == "alphabeticalByName":
            albums.sort(key=lambda item: item.get("id3_album", item["album"] if item["album"] else "zzzzzUnsortable"))
        else:
            raise NotImplemented()
        albumset = albums[0 + int(offset):int(size) + int(offset)]

        response = ApiResponse(top="albumList")

        for album in albumset:
            album_meta = album['metadata']
            album_kw = dict(id=album["id"],
                            parent=album["parent"],
                            isDir="true" if album['isdir'] else "false",
                            title=album_meta.get("id3_title", album["name"]),  #TODO these cant be blank or dsub gets mad
                            album=album_meta.get("id3_album", album["album"]),
                            artist=album_meta.get("id3_artist", album["artist"]),
                            # X year="2014"
                            # X coverArt="3228"
                            # playCount="0"
                            # created="2016-05-08T05:31:31.000Z"/>)
                            )
            if 'cover' in album_meta:
                album_kw["coverArt"] = album_meta["cover"]
            if 'id3_year' in album_meta:
                album_kw["year"] = album_meta['id3_year']
            response.add_child("album", **album_kw)
        return response

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
        dir_meta = directory["metadata"]
        children = self.library.get_dir_children(dir_id)
        dirtag.attrs.update(name=directory['name'], id=directory['id'],
                            parent=directory['parent'], playCount=10)
        root.append(dirtag)

        for item in children:
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = item['metadata']
            dirtag.append(self.render_node(doc, item, item_meta, directory, dir_meta))
        yield doc.prettify()

    def render_node(self, doc, item, item_meta, directory, dir_meta, tagname="child"):
        child = doc.new_tag(tagname,
                            id=item["id"],
                            parent=item["id"],
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
                            type="music")
        if item["size"] != -1:
            child.attrs["size"] = item["size"]
        if "media_length" in item_meta:
            child.attrs["duration"] = item_meta["media_length"]
        if "albumId" in directory:
            child.attrs["albumId"] = directory["id"]
        if "artistId" in directory:
            child.attrs["artistId"] = directory["parent"]
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
        return child

    @cherrypy.expose
    def stream_view(self, id, maxBitRate="256", **kwargs):
        maxBitRate = int(maxBitRate)
        assert maxBitRate >= 32 and maxBitRate <= 320
        fpath = self.library.get_filepath(id)
        meta = self.library.get_file_metadata(id)
        to_bitrate = min(maxBitRate, self.options.max_bitrate, meta.get("media_kbitrate", 320))
        cherrypy.response.headers['Content-Type'] = 'audio/mpeg'
        if "media_length" in meta:
            cherrypy.response.headers['X-Content-Duration'] = str(int(meta['media_length']))
        cherrypy.response.headers['X-Content-Kbitrate'] = str(to_bitrate)
        if (self.options.skip_transcode or meta.get("media_kbitrate", -1) == to_bitrate) \
           and meta["type"] == "audio/mpeg":
            def content():
                with open(fpath, "rb") as f:
                    while True:
                        data = f.read(16 * 1024)
                        if not data:
                            break
                        yield data
            return content()
        else:
            transcode_meta = "transcoded_{}_size".format(to_bitrate)
            if transcode_meta in meta:
                cherrypy.response.headers['Content-Length'] = str(int(meta[transcode_meta]))

            transcode_args = ["ffmpeg", "-i", fpath, "-map", "0:0", "-b:a",
                              "{}k".format(to_bitrate),
                              "-v", "0", "-f", "mp3", "-"]
            logging.info(' '.join(transcode_args))
            proc = subprocess.Popen(transcode_args, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            def content(proc):
                length = 0
                completed = False
                start = time()
                try:
                    while True:
                        data = proc.stdout.read(16 * 1024)
                        if not data:
                            completed = True
                            break
                        yield data
                        length += len(data)
                finally:
                    proc.poll()
                    if proc.returncode is None or proc.returncode == 0:
                        logging.warning("transcoded {} in {}s".format(id, int(time() - start)))
                        if completed:
                            self.library.report_transcode(id, to_bitrate, length)
                    else:
                        logging.error("transcode of {} exited with code {} after {}s".format(id, proc.returncode,
                                                                                             int(time() - start)))

            def stopit(proc):
                try:
                    proc.wait(timeout=90)
                except subprocess.TimeoutExpired:
                    logging.warning("killing timed-out transcoder")
                    proc.kill()
                    proc.wait()

            Thread(target=stopit, args=(proc, )).start()

            return content(proc)
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

        user = {} if self.options.disable_auth else self.library.db.get_user(cherrypy.request.login)
        tag = doc.new_tag("user",
                          username=user["username"],
                          email=user["email"],
                          scrobblingEnabled="false",
                          adminRole="true" if user["admin"] else "false",
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
        root.append(tag)
        folder = doc.new_tag("folder")
        folder.append("0")
        tag.append(folder)
        yield doc.prettify()

    @cherrypy.expose
    def star_view(self, id, **kwargs):
        self.library.set_starred(cherrypy.request.login, int(id), starred=True)
        yield self.response()[0].prettify()

    @cherrypy.expose
    def unstar_view(self, id, **kwargs):
        self.library.set_starred(cherrypy.request.login, int(id), starred=False)
        yield self.response()[0].prettify()

    @cherrypy.expose
    def getStarred_view(self, **kwargs):
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        tag = doc.new_tag("starred")
        root.append(tag)

        children = self.library.get_starred(cherrypy.request.login)
        for item in children:
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = item['metadata']
            itemtype = "song" if item["type"] in MUSIC_TYPES else "album"
            tag.append(self.render_node(doc, item, item_meta, {}, {}, tagname=itemtype))
        yield doc.prettify()

    @cherrypy.expose
    def getRandomSongs_view(self, size=50, genre=None, fromYear=0, toYear=0, **kwargs):
        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        doc, root = self.response()
        tag = doc.new_tag("randomSongs")
        root.append(tag)

        children = self.library.get_songs(size, shuffle=True)
        for item in children:
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = item['metadata']
            itemtype = "song" if item["type"] in MUSIC_TYPES else "album"
            tag.append(self.render_node(doc, item, item_meta, {}, self.db.getnode(item["parent"])["metadata"], tagname=itemtype))
        yield doc.prettify()
