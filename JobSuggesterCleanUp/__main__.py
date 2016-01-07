from Timer import *
from sys import argv
from ES2SQLite import ES2SQLite
from FindSpecChar import FindSpecChar
from SuggesterCleanUp import SuggesterCleanUp

if __name__ == '__main__':
    timer = Timer()
    configfile = 'config.cfg'
    if len(argv) > 1:
        if argv[1] == 'import':
            es2sqlite = ES2SQLite(configfile, 'SRC_ES')
            es2sqlite.create_sqlite_table()
            # es2sqlite.set_hard_lim_export(1)
            es2sqlite.run(export_lim=1000, import_lim=250, total_delay_time=100)

        elif argv[1] == 'importfile':
            if len(argv) < 3:
                print "Please enter file name"
            else:
                filename = argv[2]
                es2sqlite = ES2SQLite(configfile, 'SRC_ES')
                es2sqlite.create_sqlite_table()
                es2sqlite.insert_from_file(filename)

                clean_up = SuggesterCleanUp(configfile, 'DES_ES')

                print "Pre clean up:"
                clean_up.create_tables()
                clean_up.import_to_unique_table()

                clean_up.find_delete_contain([
                    '́', '̣', '̀', '̉', '̃',
                ])
                clean_up.find_hidden_char(['\t', '\n', '\r'])
                clean_up.find_strip([',', '-', ';', ':', '=', '|', '`', '&'])
                clean_up.find_replace_middle('kĩ thuật', 'kỹ thuật')
                clean_up.find_strip_right(['.'])
                clean_up.finding_duplicated()

                clean_up.insert_all_to_add()

                clean_up.clean_add()

        elif argv[1] == 'find':
            print 'Finding titles that contain special characters'
            find_spec_char = FindSpecChar(configfile)
            find_spec_char.run(100)
            print "Special characters list:",
            for char in find_spec_char.get_ascii_char_list():
                print char,
            print ""
            print "Special unicode characters list:",
            for uchar in find_spec_char.get_unicode_char_list():
                print uchar,
            print ""

            print "Writing the results..."
            find_spec_char.save_result(configfile)

        elif argv[1] == 'clean':
            clean_up = SuggesterCleanUp(configfile, 'DES_ES')

            print "Pre clean up:"
            clean_up.create_tables()
            clean_up.import_to_unique_table()

            if argv[2] == '1st':
                clean_up.find_len_of_one()
                clean_up.find_len_of_many()
                # clean_up.find_len_of_too_short()
                clean_up.find_hidden_char(['\t', '\n', '\r'])
                clean_up.find_delete_begin_with_number()
                clean_up.find_delete_contain([
                    'intern ',
                    'gameloft',
                    'zing',
                    'qa&qc',
                    'qa\qc',
                    'www',
                    'ltd',
                    ' Q.',
                    ' Q ',
                    'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q11', 'Q12',
                    ' TP ',
                    ' TP.',
                    'A.Manager',
                    'A Manager',
                    'F Forwarding',
                    'G&G II GARMENTS VIETNAM',
                    'QH Plus Industrial Company',
                    'Eureka Linh Truong Resort',
                    'Eden PLaza',
                    'Eden Resort',
                    '́', '̣', '̀', '̉', '̃',
                    'house" campaign',
                    'tháng',
                    '.com',
                    'năm',
                    'usd',
                    'triệu',
                    '@',
                    'vietnam',
                    'viet nam',
                    'cong ty',
                    'company',
                    'group',
                    'year',
                    'month',
                    ' fpt ',
                    ' sr.', ' sr ',
                    ' jr ', ' jr. ',
                    ' j ',
                    ' j. ',
                    '$', '~', '*'
                    'deverloper',
                    'internship',
                    ' vnd '
                ])
            elif argv[2] == '2nd':
                clean_up.finding_duplicated()
                clean_up.find_strip([',', '-', ';', ':', '=', '|', '`', '&'])
                clean_up.find_replace_middle(' & ', '&')
                clean_up.find_replace_middle('!', '')
                clean_up.find_replace_middle('', '')
                clean_up.find_replace_middle('�', '')
                clean_up.find_replace_middle('', '')
                clean_up.find_replace_middle('?', '')
                clean_up.find_replace_middle('& ', '&')
                clean_up.find_replace_middle(' &', '&')
                clean_up.find_replace_middle('kĩ thuật', 'kỹ thuật')
                clean_up.find_strip_middle(['', '…', ' & ', '•', '，', '・', '`'])
                clean_up.find_strip_right(['.'])
                clean_up.find_split_middle([',', '-', '/', '\\', '|', ';'])
            elif argv[2] == '3rd':
                clean_up.find_strip_bracket()
            elif argv[2] == '4th':
                clean_up.find_strip_bracket(True)

            print "Clean up"
            clean_up.clean_remove()
            clean_up.clean_edit()
            clean_up.clean_add()

        else:
            print 'Please specific a command \n' \
                  ' python JobSuggesterCleanUp import \n' \
                  ' python JobSuggesterCleanUp find \n' \
                  ' python JobSuggesterCleanUp clean'
    else:
        print 'Please specific a command \n' \
              ' python JobSuggesterCleanUp import \n' \
              ' python JobSuggesterCleanUp find \n' \
              ' python JobSuggesterCleanUp clean'
    print 'Total run time %s' % timer.get_total_time()
