import logging
import subprocess
from time import time
from threading import Thread
from pysonic.library import LETTER_GROUPS
from pysonic.types import MUSIC_TYPES
from pysonic.apilib import formatresponse, ApiResponse
import cherrypy

logging = logging.getLogger("api")


class PysonicSubsonicApi(object):
    def __init__(self, db, library, options):
        self.db = db
        self.library = library
        self.options = options

    @cherrypy.expose
    @formatresponse
    def index(self):
        response = ApiResponse()
        response.add_child("totals", **self.library.db.get_stats())
        return response

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
        # TODO real lastmodified date
        # TODO deal with ignoredArticles
        response.add_child("indexes", lastModified="1502310831000", ignoredArticles="The El La Los Las Le Les")
        artists = self.library.get_artists(sortby="name", order="asc")
        for letter in LETTER_GROUPS:
            index = response.add_child("index", _parent="indexes", name=letter.upper())
            for artist in artists:
                if artist["name"][0].lower() in letter:
                    response.add_child("artist", _real_parent=index, id=artist["dir"], name=artist["name"])
        return response

    @cherrypy.expose
    @formatresponse
    def getAlbumList_view(self, type, size=250, offset=0, **kwargs):
        qargs = {}
        if type == "random":
            qargs.update(sortby="random")
        elif type == "alphabeticalByName":
            qargs.update(sortby="name", order="asc")
        elif type == "newest":
            qargs.update(sortby="added", order="desc")
        elif type == "recent":
            qargs.update(sortby="played", order="desc")
        elif type == "frequent":
            qargs.update(sortby="plays", order="desc")

        qargs.update(limit=(offset, size))

        albums = self.library.get_albums(**qargs)

        response = ApiResponse()

        response.add_child("albumList")

        for album in albums:
            album_kw = dict(id=album["dir"],
                            parent=album["artistdir"],
                            isDir="true",
                            title=album["name"],
                            album=album["name"],
                            artist=album["artistname"],
                            coverArt=album["coverid"]
                            #year=TODO
                            # playCount="0"
                            # created="2016-05-08T05:31:31.000Z"/>)
                            )
            response.add_child("album", _parent="albumList", **album_kw)
        return response

    @cherrypy.expose
    @formatresponse
    def getMusicDirectory_view(self, id, **kwargs):
        """
        List an artist dir
        """
        dir_id = int(id)
        dirtype, dirinfo, entity = self.library.db.get_subsonic_musicdir(dirid=dir_id)

        response = ApiResponse()
        response.add_child("directory")
        response.set_attrs(_path="directory", name=entity['name'], id=entity['id'],
                           parent=dirinfo['parent'], playCount=420)

        for childtype, child in entity["children"]:
            # omit not dirs and media in browser
            # if not item["isdir"] and item["type"] not in MUSIC_TYPES:
            #     continue
            # item_meta = item['metadata']
            moreargs = {}
            if childtype == "album":
                moreargs.update(name=child["name"],
                                isDir="true",  # TODO song files in artist dir
                                parent=entity["id"],
                                id=child["dir"])
                if child["coverid"]:
                    moreargs.update(coverArt=child["coverid"])
                # album=item["name"],
                #                title=item["name"],  # TODO dupe?
                #                artist=artist["name"],
                #                coverArt=item["coverid"],
            elif childtype == "song":
                moreargs.update(name=child["title"],
                                artist=child["_artist"]["name"],
                                contentType=child["format"],
                                id=child["id"],
                                duration=child["length"],
                                isDir="false",
                                parent=entity["dir"],
                                # title=xxx
                                )
                if entity["coverid"]:
                    moreargs.update(coverArt=entity["coverid"])
                # duration="230" size="8409237" suffix="mp3" track="2"  year="2005"/>
            response.add_child("child", _parent="directory",
                               size="4096",
                               type="music",
                               **moreargs)

        cherrypy.response.headers['Content-Type'] = 'text/xml; charset=utf-8'
        return response

    @cherrypy.expose
    def stream_view(self, id, maxBitRate="256", **kwargs):
        maxBitRate = int(maxBitRate)
        assert maxBitRate >= 32 and maxBitRate <= 320
        song = self.library.get_song(int(id))
        fpath = song["_fullpath"]
        media_bitrate = song.get("bitrate") / 1024 if song.get("bitrate") else 320
        to_bitrate = min(maxBitRate,
                         self.options.max_bitrate,
                         media_bitrate)
        cherrypy.response.headers['Content-Type'] = 'audio/mpeg'
        #if "media_length" in meta:
        #    cherrypy.response.headers['X-Content-Duration'] = str(int(meta['media_length']))
        cherrypy.response.headers['X-Content-Kbitrate'] = str(to_bitrate)
        if (self.options.skip_transcode or (song.get("bitrate") and media_bitrate == to_bitrate)) \
           and song["format"] == "audio/mpeg":
            def content():
                with open(fpath, "rb") as f:
                    while True:
                        data = f.read(16 * 1024)
                        if not data:
                            break
                        yield data
            return content()
        else:
            # transcode_meta = "transcoded_{}_size".format(to_bitrate)
            # if transcode_meta in meta:
            #     cherrypy.response.headers['Content-Length'] = str(int(meta[transcode_meta]))
            transcode_args = ["ffmpeg", "-i", fpath, "-map", "0:0", "-b:a",
                              "{}k".format(to_bitrate),
                              "-v", "0", "-f", "mp3", "-"]
            logging.info(' '.join(transcode_args))
            proc = subprocess.Popen(transcode_args, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            def content(proc):
                length = 0
                # completed = False
                start = time()
                try:
                    while True:
                        data = proc.stdout.read(16 * 1024)
                        if not data:
                            # completed = True
                            break
                        yield data
                        length += len(data)
                finally:
                    proc.poll()
                    if proc.returncode is None or proc.returncode == 0:
                        logging.warning("transcoded {} in {}s".format(id, int(time() - start)))
                        # if completed:
                        #     self.library.report_transcode(id, to_bitrate, length)
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
        cover = self.library.get_cover(id)
        fpath = cover["_fullpath"]
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

    @cherrypy.expose
    @formatresponse
    def getArtistInfo_view(self, id, includeNotPresent="true", **kwargs):
        info = self.library.get_artist_info(id)
        response = ApiResponse()
        response.add_child("artistInfo")
        response.set_attrs("artistInfo", **info)
        return response

    @cherrypy.expose
    @formatresponse
    def getUser_view(self, username, **kwargs):
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
            response.add_child(itemtype, _parent="starred", **self.render_node(item, item_meta, {}, {}))
        return response

    @cherrypy.expose
    @formatresponse
    def getRandomSongs_view(self, size=50, genre=None, fromYear=0, toYear=0, **kwargs):
        """
        Get a playlist of random songs
        :param genre: genre name to find songs under
        :type genre: str
        """
        response = ApiResponse()
        response.add_child("randomSongs")
        children = self.library.db.get_songs(limit=size, sortby="random")
        for song in children:
            moreargs = {}
            if song["format"]:
                moreargs.update(contentType=song["format"])
            if song["albumcoverid"]:
                moreargs.update(coverArt=song["albumcoverid"])
            if song["length"]:
                moreargs.update(duration=song["length"])
            if song["track"]:
                moreargs.update(track=song["track"])
            if song["year"]:
                moreargs.update(year=song["year"])

            file_extension = song["file"].split(".")[-1]

            response.add_child("song",
                               _parent="randomSongs",
                               title=song["title"],
                               album=song["albumname"],
                               artist=song["artistname"],
                               id=song["id"],
                               isDir="false",
                               parent=song["albumid"],
                               size=song["size"],
                               suffix=file_extension,
                               type="music",
                               **moreargs)
        return response

    @cherrypy.expose
    @formatresponse
    def getGenres_view(self, **kwargs):
        response = ApiResponse()
        response.add_child("genres")
        for row in self.library.db.get_genres():
            response.add_child("genre", _parent="genres", value=row["name"], songCount=420, albumCount=69)
        return response

    @cherrypy.expose
    @formatresponse
    def scrobble_view(self, id, submission, **kwargs):
        """
        :param id: song id being played
        :param submission: True if end of song reached. False on start of track.
        """
        submission = True if submission == "true" else False
        # TODO save played track stats and/or do last.fm bullshit
        return ApiResponse()

    @cherrypy.expose
    @formatresponse
    def search2_view(self, query, artistCount, albumCount, songCount, **kwargs):
        response = ApiResponse()
        response.add_child("searchResult2")

        artistCount = int(artistCount)
        albumCount = int(albumCount)
        songCount = int(songCount)

        query = query.replace("*", "")  # TODO handle this

        artists = 0
        for item in self.library.get_artists():
            if query in item["name"].lower():
                response.add_child("artist", _parent="searchResult2", id=item["id"], name=item["name"])
                artists += 1
                if artists >= artistCount:
                    break

        # TODO make this more efficient
        albums = 0
        for item in self.library.get_artists():
            if query in item["name"].lower():
                response.add_child("album", _parent="searchResult2", **self.render_node(item, item["metadata"], {}, {}))
                albums += 1
                if albums >= albumCount:
                    break

        # TODO make this more efficient
        songs = 0
        for item in self.library.get_songs(limit=9999999, shuffle=False):
            if query in item["name"].lower():
                response.add_child("song", _parent="searchResult2", **self.render_node(item, item["metadata"], {}, {}))
                songs += 1
                if songs > songCount:
                    break

        return response

    @cherrypy.expose
    @formatresponse
    def setRating_view(self, id, rating):
        # rating is 1-5
        pass

    @cherrypy.expose
    def savePlayQueue_view(self, id, current, position, **kwargs):
        print("TODO save playqueue with items {} current {} position {}".format(id, current, position))

        song = self.library.get_song(int(current))
        self.library.db.update_album_played(song['albumid'], time())
        self.library.db.increment_album_plays(song['albumid'])
        # TODO save playlist with items ['378', '386', '384', '380', '383'] current 383 position 4471
        # id entries are strings!

    @cherrypy.expose
    @formatresponse
    def createPlaylist_view(self, name, songId, **kwargs):
        if type(songId) != list:
            songId = [songId]
        user = self.library.db.get_user(cherrypy.request.login)
        self.library.db.add_playlist(user["id"], name, songId)
        return ApiResponse()
        #TODO the response should be the new playlist, check the cap

    @cherrypy.expose
    @formatresponse
    def getPlaylists_view(self, **kwargs):
        user = self.library.db.get_user(cherrypy.request.login)

        response = ApiResponse()
        response.add_child("playlists")
        for playlist in self.library.db.get_playlists(user["id"]):
            response.add_child("playlist",
                               _parent="playlists",
                               id=playlist["id"],
                               name=playlist["name"],
                               owner=user["username"],
                               public=playlist["public"],
                               songCount=69,
                               duration=420,
                               # changed="2018-04-05T23:23:38.263Z"
                               # created="2018-04-05T23:23:38.252Z"
                               # coverArt="pl-1"
                               )

        return response

    @cherrypy.expose
    @formatresponse
    def getPlaylist_view(self, id, **kwargs):
        user = self.library.db.get_user(cherrypy.request.login)
        plinfo, songs = self.library.get_playlist(int(id))

        response = ApiResponse()
        response.add_child("playlist",
                           id=plinfo["id"],
                           name=plinfo["name"],  # TODO this element should match getPlaylists_view
                           owner=user["username"],  # TODO translate id to name
                           public=plinfo["public"],
                           songCount=69,
                           duration=420)
        for song in songs:
            response.add_child("entry",
                               _parent="playlist",
                               id=song["id"],
                               parent=song["albumid"],  # albumid seems wrong? should be dir parent?
                               isDir="false",
                               title=song["title"],
                               album=song["albumname"],
                               artist=song["artistname"],
                               track=song["track"],
                               year=song["year"],
                               genre=song["genrename"],
                               coverArt=song["albumcoverid"],
                               size=song["size"],
                               contentType=song["format"],
                               # suffix="mp3"
                               duration=song["length"],
                               bitRate=song["bitrate"] / 1024,
                               path=song["file"],
                               playCount="1",
                               # created="2015-06-09T15:26:01.000Z"
                               albumId=song["albumid"],
                               artistId=song["artistid"],
                               type="music")
        return response

    @cherrypy.expose
    @formatresponse
    def updatePlaylist_view(self, playlistId, songIndexToRemove=None, songIdToAdd=None, **kwargs):
        user = self.library.db.get_user(cherrypy.request.login)
        plinfo, songs = self.library.get_playlist(int(playlistId))

        assert plinfo["ownerid"] == user["id"]

        if songIndexToRemove:
            self.library.db.remove_index_from_playlist(playlistId, songIndexToRemove)
        elif songIdToAdd:
            self.library.db.add_to_playlist(playlistId, songIdToAdd)
        #TODO there are more modification methods

        return ApiResponse()

    @cherrypy.expose
    @formatresponse
    def deletePlaylist_view(self, id, **kwargs):
        user = self.library.db.get_user(cherrypy.request.login)
        plinfo, _ = self.library.get_playlist(int(id))
        assert plinfo["ownerid"] == user["id"]

        self.library.delete_playlist(plinfo["id"])
        return ApiResponse()
