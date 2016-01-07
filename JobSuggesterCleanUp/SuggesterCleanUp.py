from Connector import *
from Helper import *
from elasticsearch import exceptions as eexceptions
import sys
import time


class SuggesterCleanUp:
    def __init__(self, config_file, config_session):
        self.config_file = config_file
        self.es_index, self.es_conn = es_conn(config_file, config_session)

    def create_tables(self):
        conn = sqlite_conn(self.config_file)
        print "Creating tables..."

        # Create remove table
        query = 'DROP TABLE IF EXISTS JobSuggester_Remove'
        conn.execute(query)

        query = 'CREATE TABLE JobSuggester_Remove' \
                '( Id TEXT NOT NULL,' \
                'ContentNonUnicode TEXT NOT NULL, ' \
                'RemoveReason VARCHAR(255) NOT NULL,' \
                'Status TINYINT NOT NULL DEFAULT 0  )'
        conn.execute(query)

        # Create add table
        query = 'DROP TABLE IF EXISTS JobSuggester_Add'
        conn.execute(query)

        query = 'CREATE TABLE JobSuggester_Add' \
                '( Id TEXT NOT NULL,' \
                'ContentNonUnicode TEXT NOT NULL, ' \
                'AddReason VARCHAR(255) NOT NULL,' \
                'Status TINYINT NOT NULL DEFAULT 0 )'
        conn.execute(query)

        # Create edit table
        query = 'DROP TABLE IF EXISTS JobSuggester_Edit'
        conn.execute(query)

        query = 'CREATE TABLE JobSuggester_Edit' \
                '( Id TEXT NOT NULL, ' \
                'ContentNonUnicode TEXT NOT NULL, ' \
                'NewId TEXT NOT NULL, ' \
                'NewContentNonUnicode TEXT NOT NULL, ' \
                'EditReason VARCHAR(255) NOT NULL,' \
                'Status TINYINT NOT NULL DEFAULT 0  )'
        conn.execute(query)

        # Create unique table
        query = 'DROP TABLE IF EXISTS JobSuggester_Unique'
        conn.execute(query)

        query = 'CREATE TABLE JobSuggester_Unique ' \
                '( Id TEXT PRIMARY KEY ASC )'
        conn.execute(query)

        # Drop duplicate table
        query = 'DROP TABLE IF EXISTS JobSuggester_Duplicate'
        conn.execute(query)

        query = 'CREATE TABLE JobSuggester_Duplicate ' \
                '( Id TEXT PRIMARY KEY ASC, ' \
                ' c INT NOT NULL )'
        conn.execute(query)

        conn.commit()
        conn.close()

    def import_to_unique_table(self):
        conn = sqlite_conn(self.config_file)
        print 'Import unique contents to JobSuggester_Unique'

        query = 'INSERT INTO JobSuggester_Unique ' \
                'SELECT DISTINCT ContentNonUnicode ' \
                'FROM JobSuggester;'
        conn.execute(query)
        conn.commit()
        conn.close()

    def import_to_duplicate_count_table(self):
        conn = sqlite_conn(self.config_file)
        print 'Import duplicated count to JobSuggester_Duplicate'

        insert_temp_query = 'INSERT INTO JobSuggester_Duplicate ' \
                            'SELECT ContentNonUnicode, COUNT(Id) c ' \
                            'FROM JobSuggester ' \
                            'GROUP BY ContentNonUnicode '
        conn.execute(insert_temp_query)
        conn.commit()
        conn.close()

    def finding_duplicated(self):
        self.import_to_duplicate_count_table()
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        count = 0

        print "Finding all current duplicated suggester..."

        main_query = 'SELECT Id ' \
                     'FROM JobSuggester_Duplicate ' \
                     'WHERE c > 1 ' \
                     'LIMIT 50'
        all_duplicated = cursor.execute(main_query).fetchall()
        while len(all_duplicated) > 0:
            for duplicated_suggester in all_duplicated:
                content = escape_double_quote(duplicated_suggester[0])

                query = 'SELECT Id ' \
                        'FROM JobSuggester ' \
                        'WHERE ContentNonUnicode = "%s"' % content

                suggesters = cursor.execute(query).fetchall()
                if len(suggesters) > 1:

                    max_score, suggesters = weighting_suggester(suggesters)
                    remove_query = 'INSERT OR IGNORE INTO JobSuggester_Remove ' \
                                   '( Id, ContentNonUnicode, RemoveReason ) ' \
                                   'VALUES '

                    for suggester in suggesters:
                        if suggester['Score'] < max_score:
                            remove_query += '( "%s", "%s", "Duplicated" ),' % (escape_double_quote(suggester['Id']),
                                                                               content)
                            query = 'DELETE FROM JobSuggester_Duplicate ' \
                                    'WHERE Id = "%s"' % content
                            conn.execute(query)
                            query = 'DELETE FROM JobSuggester ' \
                                    'WHERE ContentNonUnicode = "%s"' % content
                            conn.execute(query)
                            count += 1
                            sys.stdout.write("\r %d found" % count)
                            sys.stdout.flush()

                    remove_query = remove_query[:-1]
                    conn.execute(remove_query)
                    conn.commit()
                else:
                    print query
            all_duplicated = cursor.execute(main_query).fetchall()

        print ""
        cursor.close()
        conn.close()

    def find_len_of_one(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find 1 char suggesters"
        count = 0

        main_query = 'SELECT Id, ContentNonUnicode FROM JobSuggester WHERE length(ContentNonUnicode) = 1'
        suggesters = cursor.execute(main_query).fetchall()
        for suggester in suggesters:
            if suggester[1] == '"':
                s_id = '""'
                content = '""'
            else:
                s_id = suggester[0]
                content = suggester[1]
            query = 'INSERT INTO JobSuggester_Remove ' \
                    '( Id, ContentNonUnicode, RemoveReason ) ' \
                    'VALUES ' \
                    '( "%s", "%s", "Single Character" )' % (s_id, content)
            conn.execute(query)
            query = 'DELETE FROM JobSuggester ' \
                    'WHERE Id = "%s"' % s_id
            conn.execute(query)
            count += 1
            sys.stdout.write("\r %d found" % count)
            sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_len_of_too_short(self, min_len=4):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find shorter than %d characters suggesters:" % min_len
        count = 0

        main_query = 'SELECT Id, ContentNonUnicode FROM JobSuggester WHERE length(ContentNonUnicode) > %d' % min_len
        suggesters = cursor.execute(main_query).fetchall()
        for suggester in suggesters:
            s_id = escape_double_quote(suggester[0])
            content = escape_double_quote(suggester[1])
            query = 'INSERT INTO JobSuggester_Remove ' \
                    '( Id, ContentNonUnicode, RemoveReason ) ' \
                    'VALUES ' \
                    '( "%s", "%s", "Too short" )' % (s_id, content)
            conn.execute(query)
            query = 'DELETE FROM JobSuggester ' \
                    'WHERE Id = "%s"' % s_id
            conn.execute(query)
            count += 1
            sys.stdout.write("\r %d found" % count)
            sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_len_of_many(self, max_len=128):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find longer than %d characters suggesters:" % max_len
        count = 0

        main_query = 'SELECT Id, ContentNonUnicode FROM JobSuggester WHERE length(ContentNonUnicode) > %d' % max_len
        suggesters = cursor.execute(main_query).fetchall()
        for suggester in suggesters:
            s_id = escape_double_quote(suggester[0])
            content = escape_double_quote(suggester[1])
            query = 'INSERT INTO JobSuggester_Remove ' \
                    '( Id, ContentNonUnicode, RemoveReason ) ' \
                    'VALUES ' \
                    '( "%s", "%s", "Too long" )' % (s_id, content)
            conn.execute(query)
            query = 'DELETE FROM JobSuggester ' \
                    'WHERE Id = "%s"' % s_id
            conn.execute(query)
            count += 1
            sys.stdout.write("\r %d found" % count)
            sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_delete_begin_with_number(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find suggesters begin with numbers:"
        count = 0

        main_query = 'SELECT Id, ContentNonUnicode FROM JobSuggester WHERE ContentNonUnicode GLOB "[0-9]*" ' \
                     'AND ContentNonUnicode NOT LIKE "2D%" AND ContentNonUniCode NOT LIKE "3D%"'
        suggesters = cursor.execute(main_query).fetchall()
        for suggester in suggesters:
            s_id = suggester[0]
            content = suggester[1]
            query = 'INSERT INTO JobSuggester_Remove ' \
                    '( Id, ContentNonUnicode, RemoveReason ) ' \
                    'VALUES ' \
                    '( "%s", "%s", "Begin with number" )' % (s_id, content)
            conn.execute(query)
            query = 'DELETE FROM JobSuggester ' \
                    'WHERE Id = "%s"' % s_id
            conn.execute(query)
            count += 1
            sys.stdout.write("\r %d found" % count)
            sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_delete_contain(self, words_list):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()

        for word in words_list:
            print "Find suggesters contain: %s" % word
            count = 0

            main_query = 'SELECT Id, ContentNonUnicode FROM JobSuggester ' \
                         'WHERE ContentNonUnicode LIKE \'%\' || \'' + word + '\' || \'%\' ' \
                         'OR ContentNonUnicode LIKE \'' + word.lstrip() + '\' || \'%\' ' \
                         'OR Id LIKE \'%\' || \'' + word + '\' || \'%\' ' \
                         'OR Id LIKE \'' + word.lstrip() + '\' || \'%\' '
            suggesters = cursor.execute(main_query).fetchall()
            for suggester in suggesters:
                s_id = escape_double_quote(suggester[0])
                content = escape_double_quote(suggester[1])
                query = 'INSERT INTO JobSuggester_Remove ' \
                        '( Id, ContentNonUnicode, RemoveReason ) ' \
                        'VALUES ' \
                        '( "%s", "%s", "Begin with number" )' % (s_id, content)
                conn.execute(query)
                query = 'DELETE FROM JobSuggester ' \
                        'WHERE Id = "%s"' % s_id
                conn.execute(query)
                count += 1
                sys.stdout.write("\r %d found" % count)
                sys.stdout.flush()

            print ""
            conn.commit()
        cursor.close()
        conn.close()

    def find_hidden_char(self, char_list):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find suggester having hidden char"
        existed_count = 0
        edit_count = 0

        for char in char_list:
            char_num = ord(char)
            main_query = 'SELECT Id, ContentNonUnicode ' \
                         'FROM JobSuggester ' \
                         'WHERE ContentNonUnicode LIKE \'%\' || X\'' + "%02d" % char_num + '\' || \'%\''
            results = cursor.execute(main_query).fetchall()
            for suggester in results:
                s_id, s_content = suggester
                new_s_id = s_id.replace(char, ' ').replace('  ', ' ')
                new_s_content = s_content.replace(char, ' ').replace('  ', ' ')

                query = 'SELECT Id ' \
                        'FROM JobSuggester_Unique ' \
                        'WHERE Id = "%s"' % escape_double_quote(new_s_content)
                if cursor.execute(query).fetchone():
                    query = 'INSERT INTO JobSuggester_Remove ' \
                            '( Id, ContentNonUnicode, RemoveReason ) ' \
                            'VALUES ' \
                            '( "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                     escape_double_quote(s_content))
                    conn.execute(query)
                    existed_count += 1
                else:
                    query = 'INSERT INTO JobSuggester_Edit ' \
                            '( Id, ContentNonUnicode, NewId, NewContentNonUnicode, EditReason ) ' \
                            'VALUES ' \
                            '( "%s", "%s", "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                                 escape_double_quote(s_content),
                                                                                 escape_double_quote(new_s_id),
                                                                                 escape_double_quote(new_s_content))
                    conn.execute(query)
                    edit_count += 1

                query = 'DELETE FROM JobSuggester ' \
                        'WHERE Id = "%s"' % escape_double_quote(s_id)
                conn.execute(query)
                conn.commit()
                sys.stdout.write("\r Duplicated: %d - Strip to new suggester: %d" % (existed_count, edit_count))
                sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_strip_middle(self, char_list):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find suggester having special unicode char: %s" % " ".join(char_list)
        existed_count = 0
        edit_count = 0

        for char in char_list:
            main_query = 'SELECT Id, ContentNonUnicode ' \
                         'FROM JobSuggester ' \
                         'WHERE ContentNonUnicode LIKE \'%\' || \'' + char + '\' || \'%\''
            results = cursor.execute(main_query).fetchall()
            for suggester in results:
                s_id, s_content = suggester
                new_s_id = s_id.replace(char.decode('utf-8'), ' ').replace('  ', ' ')
                new_s_content = s_content.replace(char.decode('utf-8'), ' ').replace('  ', ' ')

                query = 'SELECT Id ' \
                        'FROM JobSuggester_Unique ' \
                        'WHERE Id = "%s"' % escape_double_quote(new_s_content)
                if cursor.execute(query).fetchone():
                    query = 'INSERT INTO JobSuggester_Remove ' \
                            '( Id, ContentNonUnicode, RemoveReason ) ' \
                            'VALUES ' \
                            '( "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                     escape_double_quote(s_content))
                    conn.execute(query)
                    existed_count += 1
                else:
                    query = 'INSERT INTO JobSuggester_Edit ' \
                            '( Id, ContentNonUnicode, NewId, NewContentNonUnicode, EditReason ) ' \
                            'VALUES ' \
                            '( "%s", "%s", "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                                 escape_double_quote(s_content),
                                                                                 escape_double_quote(new_s_id),
                                                                                 escape_double_quote(new_s_content))
                    conn.execute(query)
                    edit_count += 1

                query = 'DELETE FROM JobSuggester ' \
                        'WHERE Id = "%s"' % escape_double_quote(s_id)
                conn.execute(query)
                conn.commit()
                sys.stdout.write("\r Duplicated: %d - Strip to new suggester: %d" % (existed_count, edit_count))
                sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_strip_bracket(self, strip_from_open=False):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find and strip brackets:"
        existed_count = 0
        edit_count = 0

        main_query = 'SELECT Id, ContentNonUnicode ' \
                     'FROM JobSuggester ' \
                     'WHERE ContentNonUnicode LIKE \'%\' || \'' + "(" + '\' || \'%\''
        results = cursor.execute(main_query).fetchall()
        for suggester in results:
            s_id, s_content = suggester
            new_s_id = re.sub(r'\([^)]*\)', '', s_id).replace('  ', ' ')
            new_s_content = re.sub(r'\([^)]*\)', '', s_id).replace('  ', ' ')

            if strip_from_open:
                new_s_id = re.sub(r'\([^)]*', '', new_s_id).replace('  ', ' ')
                new_s_content = re.sub(r'\([^)]*', '', new_s_content).replace('  ', ' ')

            query = 'SELECT Id ' \
                    'FROM JobSuggester_Unique ' \
                    'WHERE Id = "%s"' % escape_double_quote(new_s_content)
            if cursor.execute(query).fetchone():
                query = 'INSERT INTO JobSuggester_Remove ' \
                        '( Id, ContentNonUnicode, RemoveReason ) ' \
                        'VALUES ' \
                        '( "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                 escape_double_quote(s_content))
                conn.execute(query)
                existed_count += 1
            else:
                query = 'INSERT INTO JobSuggester_Edit ' \
                        '( Id, ContentNonUnicode, NewId, NewContentNonUnicode, EditReason ) ' \
                        'VALUES ' \
                        '( "%s", "%s", "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                             escape_double_quote(s_content),
                                                                             escape_double_quote(new_s_id),
                                                                             escape_double_quote(new_s_content))
                conn.execute(query)
                edit_count += 1

            query = 'DELETE FROM JobSuggester ' \
                    'WHERE Id = "%s"' % escape_double_quote(s_id)
            conn.execute(query)
            conn.commit()
            sys.stdout.write("\r Duplicated: %d - Strip to new suggester: %d" % (existed_count, edit_count))
            sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_replace_middle(self, ori, repl):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find and replace %s with %s" % (ori, repl)
        existed_count = 0
        edit_count = 0

        main_query = 'SELECT Id, ContentNonUnicode ' \
                     'FROM JobSuggester ' \
                     'WHERE ContentNonUnicode LIKE \'%\' || \'' + ori + '\' || \'%\' ' \
                     'OR Id LIKE \'%\' || \'' + ori + '\' || \'%\''
        results = cursor.execute(main_query).fetchall()
        for suggester in results:
            s_id, s_content = suggester
            new_s_id = s_id.replace(ori.decode('utf-8'), repl.decode('utf-8')).replace('  ', ' ')
            new_s_content = s_content.replace(ori.decode('utf-8'), repl.decode('utf-8')).replace('  ', ' ')

            query = 'SELECT Id ' \
                    'FROM JobSuggester_Unique ' \
                    'WHERE Id = "%s"' % escape_double_quote(new_s_content)
            if cursor.execute(query).fetchone():
                query = 'INSERT INTO JobSuggester_Remove ' \
                        '( Id, ContentNonUnicode, RemoveReason ) ' \
                        'VALUES ' \
                        '( "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                 escape_double_quote(s_content))
                conn.execute(query)
                existed_count += 1
            else:
                query = 'INSERT INTO JobSuggester_Edit ' \
                        '( Id, ContentNonUnicode, NewId, NewContentNonUnicode, EditReason ) ' \
                        'VALUES ' \
                        '( "%s", "%s", "%s", "%s", "Strip hidden char" )' % (escape_double_quote(s_id),
                                                                             escape_double_quote(s_content),
                                                                             escape_double_quote(new_s_id),
                                                                             escape_double_quote(new_s_content))
                conn.execute(query)
                edit_count += 1

            query = 'DELETE FROM JobSuggester ' \
                    'WHERE Id = "%s"' % escape_double_quote(s_id)
            conn.execute(query)
            conn.commit()
            sys.stdout.write("\r Duplicated: %d - Strip to new suggester: %d" % (existed_count, edit_count))
            sys.stdout.flush()

        print ""
        conn.commit()
        cursor.close()
        conn.close()

    def find_strip_right(self, char_list):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        strip_string = " ".join(char_list)
        print "Find in right for these characters: %s" % strip_string
        existed_count = 0
        edit_count = 0

        for char in char_list:
            main_query = 'SELECT Id, ContentNonUnicode ' \
                         'FROM JobSuggester ' \
                         'WHERE ContentNonUnicode LIKE "%' + char + '" LIMIT 50'
            results = cursor.execute(main_query).fetchall()
            while len(results) > 0:
                for suggester in results:
                    s_id, s_content = suggester
                    s_content_striped = s_content.rstrip(strip_string).strip()
                    s_id_striped = s_id.rstrip(strip_string).strip()

                    query = 'SELECT Id ' \
                            'FROM JobSuggester_Unique ' \
                            'WHERE Id = "%s"' % escape_double_quote(s_content_striped)
                    if cursor.execute(query).fetchone():
                        query = 'INSERT INTO JobSuggester_Remove ' \
                                '( Id, ContentNonUnicode, RemoveReason ) ' \
                                'VALUES ' \
                                '( "%s", "%s", "Strip %s duplicated" )' % (escape_double_quote(s_id),
                                                                           escape_double_quote(s_content),
                                                                           char)
                        conn.execute(query)
                        existed_count += 1

                    else:
                        query = 'INSERT INTO JobSuggester_Edit ' \
                                '( Id, ContentNonUnicode, NewId, NewContentNonUnicode, EditReason ) ' \
                                'VALUES ' \
                                '( "%s", "%s", "%s", "%s", "Strip %s" )' % (escape_double_quote(s_id),
                                                                            escape_double_quote(s_content),
                                                                            escape_double_quote(s_id_striped),
                                                                            escape_double_quote(s_content_striped),
                                                                            char)
                        cursor.execute(query)
                        query = 'INSERT INTO JobSuggester_Unique ' \
                                '( Id )' \
                                'VALUES ' \
                                '( "%s" )' % escape_double_quote(s_content_striped)
                        conn.execute(query)
                        edit_count += 1

                    query = 'DELETE FROM JobSuggester ' \
                            'WHERE Id = "%s"' % escape_double_quote(s_id)
                    conn.execute(query)
                    conn.commit()

                    sys.stdout.write("\r Duplicated: %d - Strip to new suggester: %d" % (existed_count, edit_count))
                    sys.stdout.flush()
                results = cursor.execute(main_query).fetchall()

        print ""
        cursor.close()
        conn.close()

    def find_strip(self, char_list):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        strip_string = " ".join(char_list)
        print "Find in left and right for these characters: %s" % strip_string
        existed_count = 0
        edit_count = 0

        for char in char_list:
            main_query = 'SELECT Id, ContentNonUnicode ' \
                         'FROM JobSuggester ' \
                         'WHERE ContentNonUnicode LIKE "' + char + '%" ' \
                                                                   'OR ContentNonUnicode LIKE "%' + char + '" ' \
                                                                                                           'LIMIT 50'
            results = cursor.execute(main_query).fetchall()
            while len(results) > 0:
                for suggester in results:
                    s_id, s_content = suggester
                    s_content_striped = s_content.strip(strip_string).strip()
                    s_id_striped = s_id.strip(strip_string).strip()

                    query = 'SELECT Id ' \
                            'FROM JobSuggester_Unique ' \
                            'WHERE Id = "%s"' % escape_double_quote(s_content_striped)
                    if cursor.execute(query).fetchone():
                        query = 'INSERT INTO JobSuggester_Remove ' \
                                '( Id, ContentNonUnicode, RemoveReason ) ' \
                                'VALUES ' \
                                '( "%s", "%s", "Strip %s duplicated" )' % (escape_double_quote(s_id),
                                                                           escape_double_quote(s_content),
                                                                           char)
                        conn.execute(query)
                        existed_count += 1

                    else:
                        query = 'INSERT INTO JobSuggester_Edit ' \
                                '( Id, ContentNonUnicode, NewId, NewContentNonUnicode, EditReason ) ' \
                                'VALUES ' \
                                '( "%s", "%s", "%s", "%s", "Strip %s" )' % (escape_double_quote(s_id),
                                                                            escape_double_quote(s_content),
                                                                            escape_double_quote(s_id_striped),
                                                                            escape_double_quote(s_content_striped),
                                                                            char)
                        cursor.execute(query)
                        query = 'INSERT INTO JobSuggester_Unique ' \
                                '( Id )' \
                                'VALUES ' \
                                '( "%s" )' % escape_double_quote(s_content_striped)
                        conn.execute(query)
                        edit_count += 1

                    query = 'DELETE FROM JobSuggester ' \
                            'WHERE Id = "%s"' % escape_double_quote(s_id)
                    conn.execute(query)
                    conn.commit()

                    sys.stdout.write("\r Duplicated: %d - Strip to new suggester: %d" % (existed_count, edit_count))
                    sys.stdout.flush()
                results = cursor.execute(main_query).fetchall()

        print ""
        cursor.close()
        conn.close()

    def find_split_middle(self, char_list):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Find in middle for these characters:",
        for char in char_list:
            print char,
        print ""
        existed_count = 0
        add_count = 0

        for char in char_list:
            main_query = 'SELECT Id, ContentNonUnicode ' \
                         'FROM JobSuggester ' \
                         'WHERE ContentNonUnicode LIKE "%' + char + '%" ' \
                                                                    'LIMIT 50'
            results = cursor.execute(main_query).fetchall()
            while len(results) > 0:
                for suggester in results:
                    s_id, s_content = suggester

                    query = 'INSERT INTO JobSuggester_Remove ' \
                            '( Id, ContentNonUnicode, RemoveReason ) ' \
                            'VALUES ' \
                            '( "%s", "%s", "Strip %s split" )' % (escape_double_quote(s_id),
                                                                  escape_double_quote(s_content),
                                                                  char)
                    conn.execute(query)

                    sub_suggesters = re.sub(r'\([^)]*\)', '', s_id).split(char)
                    sub_contents = re.sub(r'\([^)]*\)', '', s_content).split(char)
                    for i in range(len(sub_suggesters)):
                        sub_suggester = sub_suggesters[i].strip()
                        sub_content = sub_contents[i].strip()
                        if len(sub_content) > 1:
                            query = 'SELECT Id ' \
                                    'FROM JobSuggester_Unique ' \
                                    'WHERE Id = "%s"' % escape_double_quote(sub_content)
                            if cursor.execute(query).fetchone():
                                existed_count += 1
                            else:
                                query = 'INSERT INTO JobSuggester_Add ' \
                                        '( Id, ContentNonUnicode, AddReason ) ' \
                                        'VALUES ' \
                                        '( "%s", "%s", "Strip %s split" )' % (escape_double_quote(sub_suggester),
                                                                              escape_double_quote(sub_content),
                                                                              char)
                                conn.execute(query)
                                query = 'INSERT INTO JobSuggester_Unique ' \
                                        '( Id ) ' \
                                        'VALUES ' \
                                        '( "%s" )' % escape_double_quote(sub_content)
                                conn.execute(query)

                                add_count += 1

                    query = 'DELETE FROM JobSuggester ' \
                            'WHERE Id = "%s"' % escape_double_quote(s_id)
                    conn.execute(query)
                    conn.commit()

                    sys.stdout.write("\r Duplicated: %d - Add new: %d" % (existed_count, add_count))
                    sys.stdout.flush()
                results = cursor.execute(main_query).fetchall()

        print ""
        cursor.close()
        conn.close()

    def clean_remove(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        print "Removing documents:"

        query = "SELECT Id FROM JobSuggester_Remove WHERE Status = 0"
        remove_suggesters = cursor.execute(query).fetchall()

        es_id_condition = {
            "value": ""
        }

        cleaned_count = 0
        not_found = []

        for suggester in remove_suggesters:
            es_id_condition["value"] = suggester[0]
            try:
                self.es_conn.delete(
                    index=self.es_index,
                    id=suggester[0],
                    doc_type="jobTitle",
                    refresh="true"
                )
                query = "UPDATE JobSuggester_Remove " \
                        "SET Status = 1 " \
                        "WHERE Id = \"%s\"" % escape_double_quote(suggester[0])
                conn.execute(query)
                conn.commit()
                cleaned_count += 1
            except eexceptions.NotFoundError:
                not_found.append(suggester[0])
                print suggester[0]
            sys.stdout.write("\r Removed %d - %d not found" % (cleaned_count,
                                                               len(not_found)))
            sys.stdout.flush()
            if cleaned_count % 10 == 0:
                time.sleep(0.005)

        print ""
        cursor.close()
        conn.close()

    def clean_edit(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        cleaned_count = 0
        not_clean = []
        not_found = []
        print "Editing documents:"

        query = "SELECT Id, ContentNonUnicode, NewId, NewContentNonUnicode FROM JobSuggester_Edit WHERE Status = 0"
        edit_suggesters = cursor.execute(query).fetchall()

        es_id_condition = {
            "value": ""
        }

        for suggester in edit_suggesters:
            es_id_condition["value"] = suggester[0]
            try:
                self.es_conn.delete(
                    index=self.es_index,
                    id=suggester[0],
                    doc_type="jobTitle",
                    refresh="true")
                try:
                    self.es_conn.create(
                        index=self.es_index,
                        doc_type="jobTitle",
                        body={
                            "jobTitleName": suggester[2],
                            "jobTitleNameSuggest": {
                                "input": [
                                    suggester[2]
                                ],
                                "output": suggester[2]
                            }
                        })

                    query = "UPDATE JobSuggester_Edit " \
                            "SET Status = 1 " \
                            "WHERE Id = \"%s\"" % escape_double_quote(suggester[0])
                    conn.execute(query)
                    conn.commit()

                    cleaned_count += 1
                except eexceptions.ConflictError:
                    not_clean.append(suggester[0])
            except eexceptions.NotFoundError:
                not_found.append(suggester[0])
                print suggester[0]
            sys.stdout.write("\r Edited %d - %d not edited - %d not found" % (cleaned_count,
                                                                              len(not_clean),
                                                                              len(not_found)))
            sys.stdout.flush()
            if cleaned_count % 5 == 0:
                time.sleep(0.01)

        print ""
        cursor.close()
        conn.close()

    def clean_add(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()
        cleaned_count = 0
        not_clean = []
        print "Adding documents:"

        query = "SELECT Id, ContentNonUnicode FROM JobSuggester_Add WHERE Status = 0"
        add_suggesters = cursor.execute(query).fetchall()

        for suggester in add_suggesters:
            try:
                self.es_conn.create(
                    index=self.es_index,
                    doc_type="jobTitle",
                    body={
                        "jobTitleName": suggester[0],
                        "jobTitleNameSuggest": {
                            "input": [
                                suggester[0]
                            ],
                            "output": suggester[0]
                        }
                    })
                query = "UPDATE JobSuggester_Add " \
                        "SET Status = 1 " \
                        "WHERE Id = \"%s\"" % escape_double_quote(suggester[0])
                conn.execute(query)
                conn.commit()

                cleaned_count += 1
            except eexceptions.ConflictError:
                not_clean.append(suggester[0])
            sys.stdout.write("\r Add %d documents - # of not added: %d" % (cleaned_count, len(not_clean)))
            sys.stdout.flush()
            if cleaned_count % 5 == 0:
                time.sleep(0.01)

        print ""
        cursor.close()
        conn.close()

    def insert_all_to_add(self):
        conn = sqlite_conn(self.config_file)
        cursor = conn.cursor()

        print "Insert all records from JobSuggester to JobSuggester_Add"
        query = "INSERT INTO JobSuggester_Add " \
                "(Id, ContentNonUnicode, AddReason) " \
                "SELECT Id, ContentNonUnicode, 'Add new' FROM JobSuggester"
        conn.execute(query)
        conn.commit()

        cursor.close()
        conn.close()
