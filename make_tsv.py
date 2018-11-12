#!/usr/bin/env python

import logging
import os
import re
import sqlite3
import time

class FileSet(object):
    _SINGLETON = None

    @classmethod
    def Factory(cls, *args, **kwargs):
        if cls._SINGLETON is None:
            cls._SINGLETON = cls(*args, **kwargs)
        return cls._SINGLETON

    def __init__(self, limit=1000, trim_size=100,  mode="w", reopen_mode="a"):
        self._limit = limit
        self._trim_size = trim_size
        self._mode = mode
        self._reopen_mode = reopen_mode
        self._cache = {}
        self._seen = set()
        self._logger = logging.getLogger(self.__class__.__name__)

    def get(self, path):
        if path not in self._cache:
            return self.open(path)
        else:
            pair = self._cache[path]
            pair[0] = time.time()
            return pair[1]

    def open(self, path):
        # make room
        self.trim()

        if path in self._seen:
            self._logger.info('reopening {0}'.format(path))
            fh = open(path, self._reopen_mode)
        else:
            self._seen.add(path)
            self.mkpath(os.path.dirname(path))
            self._logger.debug('opening {0}'.format(path))
            fh = open(path, self._mode)
        self._cache[path] = [time.time(), fh]
        return fh

    def mkpath(self, path):
        if not path or os.path.exists(path):
            return
        self.mkpath(os.path.dirname(path))
        self._logger.info('making directory {0}'.format(path))
        os.mkdir(path)

    def trim(self):
        if len(self._cache) < self._limit:
            return

        trim_count = len(self._cache) - self._limit + self._trim_size
        full_list = self._cache.items()
        full_list.sort(cmp=lambda x, y: cmp(x[1][0], y[1][0]))
        trim_set = full_list[:trim_count]
        self._logger.info('Trimming {0} of {1} open files'.format(len(trim_set), len(full_list)))
        for path, pair in trim_set:
            # pair[1].close()
            del self._cache[path]

class FlightProcessor(object):
    FLIGHTS_COLUMNS = [ 'callsign', 'lon', 'lat', 'altitude', 'parsed_time', 'date(parsed_time) as date_parsed' ]
    FLIGHTS_RESULTS = [ 'callsign', 'lon', 'lat', 'altitude', 'parsed_time', 'date_parsed']
    FLIGHTS_TABLE = 'flights'

    def __init__(self, conn, dates=None, tsv_path='tsv'):
        self._conn = conn
        self._dates = dates
        self._files = FileSet.Factory()
        self._allpoints_pattern = tsv_path + os.path.sep + '{0}' + os.path.sep + 'allpoints_{0}.tsv'
        self._path_pattern = tsv_path + os.path.sep + '{0}' + os.path.sep + '{1}_{0}.tsv'
        self._logger = logging.getLogger(self.__class__.__name__)
        self.process()

    def process(self):
        fetch_sql = 'SELECT {} FROM {}'.format(', '.join(self.FLIGHTS_COLUMNS), self.FLIGHTS_TABLE)
        if self._dates is not None:
            fetch_sql += ' WHERE date_parsed IN ("{}")'.format('", "'.join(self._dates))
        self._logger.info('Executing SQL "{0}"'.format(fetch_sql))
        results = self._conn.execute(fetch_sql)
        for row in results:
            self.record(row)

    def record(self, row):
        val = {self.FLIGHTS_RESULTS[i]: row[i] for i in range(len(row))}
        allpoints_path = self._allpoints_pattern.format(val['date_parsed'])
        self.write(allpoints_path, val)
        if re.match(r'[A-Z][A-Z][A-Z][0-9]', val['callsign']):
            airline_ident = val['callsign'][:3].lower()
            airline_path = self._path_pattern.format(val['date_parsed'], airline_ident)
            self.write(airline_path, val)
        callsign_path = self._path_pattern.format(val['date_parsed'],
                                                  val['callsign'].rstrip().lower())
        self.write(callsign_path, val)

    def write(self, path, val):
        f = self._files.get(path)
        f.write('{callsign}\t{parsed_time}\t{lon}\t{lat}\t{altitude}\n'.format(**val))


def main():
    import argparse

    parser = argparse.ArgumentParser('make_tsv.py')
    parser.add_argument('databases', metavar='database', nargs='*', help='dump1090-stream-parser.py database file')
    parser.add_argument('--dates', default=None, help='comma seperated list of dates to process')
    parser.add_argument('--debug', default=False, action='store_true', help='Verbose output')
    parser.add_argument('--path', default='tsv', help='where the tsv hierarchy should be written')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.databases:
        databases = args.databases
    else:
        databases = ['adsb_messages.db']

    if args.dates:
        dates = args.dates.split(',')
    else:
        dates = None

    for database in databases:
        if os.path.exists(database):
            logging.info('Opening {0}'.format(database))
            connection = sqlite3.connect(database)
            FlightProcessor(connection, dates, args.path)
        else:
            logging.error('Unable to open {0}'.format(database))
    logging.info('Total of {} files written'.format(len(FileSet.Factory()._seen)))

if __name__ == '__main__':
    main()
