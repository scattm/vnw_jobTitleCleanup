from Connector import *
from Timer import *
from Helper import *
import sys


# -*- coding: utf8 -*-


class ES2SQLite:
    def __init__(self, config_file, config_section):
        self.config_file = config_file
        self.es_index, self.es_conn = es_conn(config_file, config_section)
        self.es_query = {
            "fields": ["_id"],
            "sort": [{"_id": {"order": "asc"}}]
        }
        self.hard_lim_export = 0

    def set_hard_lim_export(self, limit):
        self.hard_lim_export = limit

    def create_sqlite_table(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()

        query = \
            "DROP TABLE IF EXISTS JobSuggester"
        cursor.execute(query)

        query = "CREATE TABLE JobSuggester " \
                "( Id TEXT NOT NULL, " \
                "ContentNonUnicode TEXT NOT NULL);"
        cursor.execute(query)

        conn.commit()
        conn.close()

    def es_export_rows(self, start, limit):
        ret = []
        hits = self.es_conn.search(index=self.es_index,
                                   doc_type='jobTitle',
                                   body=self.es_query,
                                   from_=start, size=limit
                                   )['hits']['hits']
        for hit in hits:
            ret.append({
                "content": hit[u'_id']
            })
        return ret

    def insert_sqlite(self, exported_data, limit):
        v2a = Vietnamese2Ascii()
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        total = len(exported_data)
        start = 0

        while start < total:
            query = "INSERT INTO JobSuggester (Id, ContentNonUnicode) VALUES "
            next_start = start + limit
            if next_start > total:
                next_start = total
            for i in range(start, next_start):
                content = escape_double_quote(exported_data[i]["content"])
                query += "\n(\"%s\", \"%s\")," % (content,
                                                  v2a.convert(content.strip().replace('  ', ' ').lower()))
            query = query[:-1]

            try:
                cursor.execute(query)
                conn.commit()
            except conn.OperationalError:
                print query
                raise conn.OperationalError

            start = next_start

        cursor.close()

    def insert_from_file(self, filename):
        f = open(filename, 'r')
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        v2a = Vietnamese2Ascii()
        error_count = 0
        line_count = 0

        print 'Import from %s' % filename

        for line in f:
            line_count += 1
            line = escape_double_quote(line.decode('utf8')).strip('\n')
            query = "INSERT INTO JobSuggester (Id, ContentNonUnicode) " \
                    "VALUES (\"%s\", \"%s\")" % (line,
                                                 v2a.convert(line.strip().replace('  ', ' ').lower()))

            try:
                cursor.execute(query)
                conn.commit()
            except conn.OperationalError:
                print query
                error_count += 1

        print "Total lines %d" % line_count
        print "Error lines %d" % error_count

    def run(self, export_lim, import_lim, total_delay_time=750):
        print "Estimating total entries to import...",
        no_of_entries = self.es_conn.search(index=self.es_index,
                                            doc_type='jobTitle',
                                            body=self.es_query,
                                            size=1)['hits']['total']
        print "%d entries" % no_of_entries

        print "Begin to import..."
        exported = 0
        start = 0

        while start <= no_of_entries and (self.hard_lim_export == 0 or start <= self.hard_lim_export):
            exported_entries = self.es_export_rows(start, export_lim)
            exported += len(exported_entries)
            start += export_lim
            sys.stdout.write("\r Exported %d entries of total %d entries..." % (exported, no_of_entries))
            sys.stdout.flush()

            delay_timer = Timer()
            self.insert_sqlite(exported_entries, import_lim)
            delay_timer.sleep_if_not_enough(total_delay_time)
