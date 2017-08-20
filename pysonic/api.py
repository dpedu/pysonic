import re
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


CALLBACK_RE = re.compile(r'^[a-zA-Z0-9_]+$')
logging = logging.getLogger("api")


response_formats = defaultdict(lambda: "render_xml")
response_formats["json"] = "render_json"
response_formats["jsonp"] = "render_jsonp"

response_headers = defaultdict(lambda: "text/xml; charset=utf-8")
response_headers["json"] = "application/json; charset=utf-8"
response_headers["jsonp"] = "text/javascript; charset=utf-8"


def formatresponse(func):
    """
    Decorator for rendering ApiResponse responses
    """
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        response_format = kwargs.get("f", "xml")
        callback = kwargs.get("callback", None)
        cherrypy.response.headers['Content-Type'] = response_headers[response_format]
        renderer = getattr(response, response_formats[response_format])
        if response_format == "jsonp":
            if callback is None:
                return response.render_xml().encode('UTF-8')  # copy original subsonic behavior
            else:
                return renderer(callback).encode('UTF-8')
        return renderer().encode('UTF-8')
    return wrapper


class ApiResponse(object):
    def __init__(self, status="ok", version="1.15.0"):
        """
        ApiResponses are python data structures that can be converted to other formats. The response has a status and a
        version. The response data structure is stored in self.data and follows these rules:
        - self.data is a dict
        - the dict's values become either child nodes or attributes, named by the key
        - lists become many oner one child
        - dict values are not allowed
        - all other types (str, int, NoneType) are attributes
        :param status:
        :param version:
        """
        self.status = status
        self.version = version
        self.data = defaultdict(lambda: list())

    def add_child(self, _type, _parent="", _real_parent=None, **kwargs):
        parent = _real_parent if _real_parent else self.get_child(_parent)
        m = defaultdict(lambda: list())
        m.update(dict(kwargs))
        parent[_type].append(m)
        return m

    def get_child(self, _path):
        parent_path = _path.split(".")
        parent = self.data
        for item in parent_path:
            if not item:
                continue
            parent = parent.get(item)[0]
        return parent

    def set_attrs(self, _path, **attrs):
        parent = self.get_child(_path)
        if type(parent) not in (dict, defaultdict):
            raise Exception("wot")
        parent.update(attrs)

    def render_json(self):
        def _flatten_json(item):
            """
            Convert defaultdicts to dicts and remove lists where node has 1 or no child
            """
            listed_attrs = ["folder"]
            d = {}
            for k, v in item.items():
                if type(v) is list:
                    if len(v) > 1:
                        d[k] = []
                        for subitem in v:
                            d[k].append(_flatten_json(subitem))
                    elif len(v) == 1:
                        d[k] = _flatten_json(v[0])
                    else:
                        d[k] = {}
                else:
                    d[k] = [v] if k in listed_attrs else v
            return d

        data = _flatten_json(self.data)
        return json.dumps({"subsonic-response": dict(status=self.status, version=self.version, **data)}, indent=4)

    def render_jsonp(self, callback):
        assert CALLBACK_RE.match(callback), "Invalid callback"
        return "{}({});".format(callback, self.render_json())

    def render_xml(self):
        text_attrs = ['largeImageUrl', 'musicBrainzId', 'smallImageUrl', 'mediumImageUrl', 'lastFmUrl', 'biography',
                      'folder']
        # These attributes will be placed in <hello>{{ value }}</hello> tags instead of hello="{{ value }}" on parent
        doc = BeautifulSoup('', features='lxml-xml')
        root = doc.new_tag("subsonic-response", xmlns="http://subsonic.org/restapi",
                           status=self.status,
                           version=self.version)
        doc.append(root)

        def _render_xml(node, parent):
            """
            For every key in the node dict, the parent gets a new child tag with name == key
            If the value is a dict, it becomes the new tag's attrs
            If the value is a list, the parent gets many new tags with each dict as attrs
            If the value is str int etc, parent gets attrs
            """
            for key, value in node.items():
                if type(value) in (dict, defaultdict):
                    tag = doc.new_tag(key)
                    parent.append(tag)
                    tag.attrs.update(value)
                elif type(value) is list:
                    for item in value:
                        tag = doc.new_tag(key)
                        parent.append(tag)
                        _render_xml(item, tag)
                else:
                    if key in text_attrs:
                        tag = doc.new_tag(key)
                        parent.append(tag)
                        tag.append(str(value))
                    else:
                        parent.attrs[key] = value
        _render_xml(self.data, root)
        return doc.prettify()


class PysonicApi(object):
    def __init__(self, db, library, options):
        self.db = db
        self.library = library
        self.options = options

    @cherrypy.expose
    @formatresponse
    def ping_view(self, **kwargs):
        # Called when the app hits the "test connection" server option
        return ApiResponse()

    @cherrypy.expose
    @formatresponse
    def getLicense_view(self, **kwargs):
        # Called after ping.view
        response = ApiResponse()
        response.add_child("license",
                           valid="true",
                           email="admin@localhost",
                           licenseExpires="2100-01-01T00:00:00.000Z",
                           trialExpires="2100-01-01T01:01:00.000Z")
        return response

    @cherrypy.expose
    @formatresponse
    def getMusicFolders_view(self, **kwargs):
        response = ApiResponse()
        response.add_child("musicFolders")
        for folder in self.library.get_libraries():
            response.add_child("musicFolder", _parent="musicFolders", id=folder["id"], name=folder["name"])
        return response

    @cherrypy.expose
    @formatresponse
    def getIndexes_view(self, **kwargs):
        # Get listing of top-level dir
        response = ApiResponse()
        response.add_child("indexes", lastModified="1502310831000", ignoredArticles="The El La Los Las Le Les")
        for letter in LETTER_GROUPS:
            index = response.add_child("index", _parent="indexes", name=letter.upper())
            for artist in self.library.get_artists():
                if artist["name"][0].lower() in letter:
                    response.add_child("artist", _real_parent=index, id=artist["id"], name=artist["name"])
        return response

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

        response = ApiResponse()

        response.add_child("albumList")

        for album in albumset:
            album_meta = album['metadata']
            album_kw = dict(id=album["id"],
                            parent=album["parent"],
                            isDir="true" if album['isdir'] else "false",
                            title=album_meta.get("id3_title", album["name"]),  #TODO these cant be blank or dsub gets mad
                            album=album_meta.get("id3_album", album["album"]),
                            artist=album_meta.get("id3_artist", album["artist"]),
                            # playCount="0"
                            # created="2016-05-08T05:31:31.000Z"/>)
                            )
            if 'cover' in album_meta:
                album_kw["coverArt"] = album_meta["cover"]
            if 'id3_year' in album_meta:
                album_kw["year"] = album_meta['id3_year']
            response.add_child("album", _parent="albumList", **album_kw)
        return response

    @cherrypy.expose
    @formatresponse
    def getMusicDirectory_view(self, id, **kwargs):
        """
        List an artist dir
        """
        dir_id = int(id)

        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'

        response = ApiResponse()
        response.add_child("directory")

        directory = self.library.get_dir(dir_id)
        dir_meta = directory["metadata"]
        children = self.library.get_dir_children(dir_id)
        response.set_attrs(_path="directory", name=directory['name'], id=directory['id'],
                           parent=directory['parent'], playCount=10)

        for item in children:
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = item['metadata']
            response.add_child("child", _parent="directory", **self.render_node2(item, item_meta, directory, dir_meta))

        return response

    def render_node2(self, item, item_meta, directory, dir_meta):
        """
        Given a node and it's parent directory, and meta, return a dict with the keys formatted how the subsonic clients
        expect them to be
        :param item:
        :param item_meta:
        :param directory:
        :param dir_meta:
        """
        child = dict(id=item["id"],
                     parent=item["id"],
                     isDir="true" if item['isdir'] else "false",
                     title=item_meta.get("id3_title", item["name"]),
                     album=item_meta.get("id3_album", item["album"]),
                     artist=item_meta.get("id3_artist", item["artist"]),
                     # playCount="5",
                     # created="2016-04-25T07:31:33.000Z"
                     # genre="Other",
                     # path="Cosmic Gate/Sign Of The Times/03 Flatline (featuring Kyler England).mp3"
                     type="music")
        if 'kbitrate' in item_meta:
            child["bitrate"] = item_meta["kbitrate"]
        if item["size"] != -1:
            child["size"] = item["size"]
        if "media_length" in item_meta:
            child["duration"] = item_meta["media_length"]
        if "albumId" in directory:
            child["albumId"] = directory["id"]
        if "artistId" in directory:
            child["artistId"] = directory["parent"]
        if "." in item["name"]:
            child["suffix"] = item["name"].split(".")[-1]
        if item["type"]:
            child["contentType"] = item["type"]
        if 'cover' in item_meta:
            child["coverArt"] = item_meta["cover"]
        elif 'cover' in dir_meta:
            child["coverArt"] = dir_meta["cover"]
        if 'track' in item_meta:
            child["track"] = item_meta['track']
        if 'id3_year' in item_meta:
            child["year"] = item_meta['id3_year']
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
            'png': 'image/png',
            'gif': 'image/gif'
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

    def response(self, status="ok"):
        doc = BeautifulSoup('', features='lxml-xml')
        root = doc.new_tag("subsonic-response", xmlns="http://subsonic.org/restapi", status=status, version="1.15.0")
        doc.append(root)
        return doc, root

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
                            # genre="Other",
                            # path="Cosmic Gate/Sign Of The Times/03 Flatline (featuring Kyler England).mp3"
                            type="music")
        if 'kbitrate' in item_meta:
            child.attrs["bitrate"] = item_meta["kbitrate"]
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
    @formatresponse
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

        response = ApiResponse()
        response.add_child("artistInfo")
        response.set_attrs("artistInfo", **info)

        return response

    @cherrypy.expose
    @formatresponse
    def getUser_view(self, u, username, **kwargs):
        user = {} if self.options.disable_auth else self.library.db.get_user(cherrypy.request.login)
        response = ApiResponse()
        response.add_child("user",
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
                           avatarLastChanged="2017-08-07T20:16:24.596Z",
                           folder=0)
        return response

    @cherrypy.expose
    @formatresponse
    def star_view(self, id, **kwargs):
        self.library.set_starred(cherrypy.request.login, int(id), starred=True)
        return ApiResponse()

    @cherrypy.expose
    @formatresponse
    def unstar_view(self, id, **kwargs):
        self.library.set_starred(cherrypy.request.login, int(id), starred=False)
        return ApiResponse()

    @cherrypy.expose
    @formatresponse
    def getStarred_view(self, **kwargs):
        children = self.library.get_starred(cherrypy.request.login)
        response = ApiResponse()
        response.add_child("starred")
        for item in children:
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = item['metadata']
            itemtype = "song" if item["type"] in MUSIC_TYPES else "album"
            response.add_child(itemtype, _parent="starred", **self.render_node2(item, item_meta, {}, {}))
        return response

    @cherrypy.expose
    @formatresponse
    def getRandomSongs_view(self, size=50, genre=None, fromYear=0, toYear=0, **kwargs):
        response = ApiResponse()
        response.add_child("randomSongs")
        children = self.library.get_songs(size, shuffle=True)
        for item in children:
            # omit not dirs and media in browser
            if not item["isdir"] and item["type"] not in MUSIC_TYPES:
                continue
            item_meta = item['metadata']
            itemtype = "song" if item["type"] in MUSIC_TYPES else "album"
            response.add_child(itemtype, _parent="randomSongs",
                               **self.render_node2(item, item_meta, {}, self.db.getnode(item["parent"])["metadata"]))
        return response
