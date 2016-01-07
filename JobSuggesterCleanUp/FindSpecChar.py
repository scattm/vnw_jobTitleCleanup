from Connector import *
import re
import ConfigParser
import sys


class FindSpecChar:
    def __init__(self, config_file):
        self.config_file = config_file
        self.query_prefix = "SELECT ContentNonUnicode FROM JobSuggester WHERE ContentNonUnicode GLOB "
        self.char_list = []
        self.unicode_char_list = []

    def get_ascii_char_list_len(self):
        return len(self.char_list)

    def get_unicode_char_list_len(self):
        return len(self.unicode_char_list)

    def get_ascii_char_list(self):
        return self.char_list

    def get_unicode_char_list(self):
        return self.unicode_char_list

    def run(self, limit=100):
        if self.get_ascii_char_list_len() == 0:
            glob_condition = "*[^a-zA-Z0-9 ]*"
            re_condition = "[^a-zA-Z0-9 ]"
            not_like_condition = ""

        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        count = 0
        while count < limit:
            try:
                query = self.query_prefix + "\"%s\" " % glob_condition + \
                                        not_like_condition + \
                                        "LIMIT 1 "
                result = cursor.execute(query).fetchone()
            except conn.OperationalError:
                print "Error executing the query:"
                print " %s" % query

            try:
                re_patt = re.compile(r'%s' % re_condition)
            except re.error:
                print "Error pattern: %s" % re_condition
                break

            try:
                new_spec_chars = re.findall(re_patt.pattern, result[0])
            except TypeError:
                if result is None:
                    print "Finish searching all suggester!"
                    break

            for char in new_spec_chars:
                try:
                    a_char = char.decode('utf8')
                    if a_char not in self.char_list:
                        self.char_list.append(a_char)
                except UnicodeEncodeError:
                    if char not in self.unicode_char_list:
                        self.unicode_char_list.append(char)

            glob_condition = "*[^a-zA-Z0-9 "
            re_condition = "[^a-zA-Z0-9 "
            not_like_condition = ""
            for char in self.char_list:
                if char not in ("[", "]", "(", ")", "-", "{", "}"):
                    if char == "\\":
                        re_condition += "\\"
                    if char == "\"":
                        glob_condition += "\""
                    glob_condition += char
                    re_condition += char
                else:
                    not_like_condition += "AND ContentNonUnicode NOT LIKE \"%" + char + "%\" "
            for uchar in self.unicode_char_list:
                glob_condition += uchar
            glob_condition += "]*"
            re_condition += "]"

            count += 1
            sys.stdout.write("\rSpecial char found: %d ASCII + "
                             "%d unicode (in %d loops)" % (self.get_ascii_char_list_len(),
                                                           self.get_unicode_char_list_len(),
                                                           count))
            sys.stdout.flush()

        cursor.close()
        conn.close()

        print "\nFinish job after %d loops" % count
        print "Final glob condition: %s" % glob_condition
        print "Final regex condition: %s" % re_condition

        return self.char_list

    def save_result(self, config_file):
        config = ConfigParser.RawConfigParser()
        config.read(config_file)
        try:
            config.set('SPECIAL_CHAR', 'ASCII', self.char_list)
            config.set('SPECIAL_CHAR', 'UNICODE', self.unicode_char_list)
        except ConfigParser.NoSectionError:
            config.add_section('SPECIAL_CHAR')
            config.set('SPECIAL_CHAR', 'ASCII', self.char_list)
            config.set('SPECIAL_CHAR', 'UNICODE', self.unicode_char_list)

        with open(config_file, 'wb') as cf:
            config.write(cf)
