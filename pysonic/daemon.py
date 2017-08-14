import logging
import cherrypy
from pysonic.api import PysonicApi
from pysonic.library import PysonicLibrary
from pysonic.database import PysonicDatabase


def main():
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Pysonic music streaming server")

    parser.add_argument('-p', '--port', default=8080, type=int, help="tcp port to listen on")
    parser.add_argument('-d', '--dirs', required=True, nargs='+', help="new music dirs to share")
    parser.add_argument('-s', '--database-path', default="./db.sqlite", help="path to persistent sqlite database")
    parser.add_argument('--debug', action="store_true", help="enable development options")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.debug else logging.WARNING)

    db = PysonicDatabase(path=args.database_path)
    library = PysonicLibrary(db)
    library.update()

    cherrypy.tree.mount(PysonicApi(db, library), '/rest/', {'/': {}})
    cherrypy.config.update({
        'sessionFilter.on': True,
        'tools.sessions.on': True,
        'tools.sessions.locking': 'explicit',
        'tools.sessions.timeout': 525600,
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
        print('Got sig {}, exiting...'.format(signum))
        cherrypy.engine.exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        cherrypy.engine.start()
        cherrypy.engine.block()
    finally:
        print("API has shut down")
        cherrypy.engine.exit()

if __name__ == '__main__':
    main()
