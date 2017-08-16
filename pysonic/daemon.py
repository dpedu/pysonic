import os
import logging
import cherrypy
from pysonic.api import PysonicApi
from pysonic.library import PysonicLibrary, DuplicateRootException
from pysonic.database import PysonicDatabase


def main():
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Pysonic music streaming server")

    parser.add_argument('-p', '--port', default=8080, type=int, help="tcp port to listen on")
    parser.add_argument('-d', '--dirs', required=True, nargs='+', help="new music dirs to share")
    parser.add_argument('-u', '--user', nargs='+', type=lambda x: x.split(":"), default=[],
                        help="user:password pairs for auth")
    parser.add_argument('--disable-auth', action="store_true", help="disable authentication")
    parser.add_argument('-s', '--database-path', default="./db.sqlite", help="path to persistent sqlite database")
    parser.add_argument('--debug', action="store_true", help="enable development options")

    group = parser.add_argument_group("app options")
    group.add_argument("--skip-transcode", action="store_true", help="instead of trancoding mp3s, send as-is")
    group.add_argument("--max-bitrate", type=int, default=320, help="maximum send bitrate")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.debug else logging.WARNING)

    db = PysonicDatabase(path=args.database_path)
    library = PysonicLibrary(db)
    for dirname in args.dirs:
        assert os.path.exists(dirname) and dirname.startswith("/"), "--dirs must be absolute paths and exist!"
        try:
            library.add_dir(dirname)
        except DuplicateRootException:
            pass
    library.update()

    for username, password in args.user:
        db.add_user(username, password)

    logging.warning("Libraries: {}".format([i["name"] for i in library.get_libraries()]))
    logging.warning("Artists: {}".format([i["name"] for i in library.get_artists()]))
    logging.warning("Albums: {}".format(len(library.get_albums())))

    api_config = {}
    if args.disable_auth:
        logging.warning("starting up with auth disabled")
    else:
        api_config.update({'tools.auth_basic.on': True,
                           'tools.auth_basic.realm': 'pysonic',
                           'tools.auth_basic.checkpassword': db.validate_password})
    cherrypy.tree.mount(PysonicApi(db, library, args), '/rest/', {'/': api_config})

    cherrypy.config.update({
        'sessionFilter.on': True,
        'tools.sessions.on': True,
        'tools.sessions.locking': 'explicit',
        'tools.sessions.timeout': 525600,
        'tools.gzip.on': True,
        'request.show_tracebacks': True,
        'server.socket_port': args.port,
        'server.thread_pool': 25,
        'server.socket_host': '0.0.0.0',
        'server.show_tracebacks': True,
        'server.socket_timeout': 5,
        'log.screen': False,
        'engine.autoreload.on': args.debug
    })

    def signal_handler(signum, stack):
        logging.critical('Got sig {}, exiting...'.format(signum))
        cherrypy.engine.exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        cherrypy.engine.start()
        cherrypy.engine.block()
    finally:
        logging.info("API has shut down")
        cherrypy.engine.exit()

if __name__ == '__main__':
    main()
