from elasticsearch import Elasticsearch
import sqlite3
import ConfigParser


def es_conn(configfile, session='SRC_ES'):
    config = ConfigParser.RawConfigParser()
    config.read(configfile)

    try:
        es = Elasticsearch(
            [
                "http://%s:%s@%s:%d" % (
                    config.get(session, 'username'),
                    config.get(session, 'password'),
                    config.get(session, 'host'),
                    config.getint(session, 'port')
                )
            ]
        )
    except ConfigParser.NoOptionError:
        es = Elasticsearch(
            [
                "http://%s:%d" % (
                    config.get(session, 'host'),
                    config.getint(session, 'port')
                )
            ]
        )

    return config.get(session, 'index'), es


def sqlite_conn(configfile):
    config = ConfigParser.RawConfigParser()
    config.read(configfile)

    return sqlite3.connect(config.get('SQLite', 'filename'))
