# -*- coding: utf8 -*-

import re


class Vietnamese2Ascii:
    def __init__(self):
        self.char_dict = {
            ord(u'á'): u'a',
            ord(u'à'): u'a',
            ord(u'ả'): u'a',
            ord(u'ã'): u'a',
            ord(u'ạ'): u'a',
            ord(u'â'): u'a',
            ord(u'ấ'): u'a',
            ord(u'ầ'): u'a',
            ord(u'ẩ'): u'a',
            ord(u'ẫ'): u'a',
            ord(u'ậ'): u'a',
            ord(u'ă'): u'a',
            ord(u'ắ'): u'a',
            ord(u'ằ'): u'a',
            ord(u'ẳ'): u'a',
            ord(u'ẵ'): u'a',
            ord(u'ặ'): u'a',
            ord(u'đ'): u'd',
            ord(u'é'): u'e',
            ord(u'è'): u'e',
            ord(u'ẻ'): u'e',
            ord(u'ẽ'): u'e',
            ord(u'ẹ'): u'e',
            ord(u'ê'): u'e',
            ord(u'ế'): u'e',
            ord(u'ề'): u'e',
            ord(u'ể'): u'e',
            ord(u'ễ'): u'e',
            ord(u'ệ'): u'e',
            ord(u'í'): u'i',
            ord(u'ì'): u'i',
            ord(u'ỉ'): u'i',
            ord(u'ĩ'): u'i',
            ord(u'ị'): u'i',
            ord(u'ó'): u'o',
            ord(u'ò'): u'o',
            ord(u'ỏ'): u'o',
            ord(u'õ'): u'o',
            ord(u'ọ'): u'o',
            ord(u'ô'): u'o',
            ord(u'ố'): u'o',
            ord(u'ồ'): u'o',
            ord(u'ổ'): u'o',
            ord(u'ỗ'): u'o',
            ord(u'ộ'): u'o',
            ord(u'ơ'): u'o',
            ord(u'ớ'): u'o',
            ord(u'ờ'): u'o',
            ord(u'ở'): u'o',
            ord(u'ỡ'): u'o',
            ord(u'ợ'): u'o',
            ord(u'ú'): u'u',
            ord(u'ù'): u'u',
            ord(u'ủ'): u'u',
            ord(u'ũ'): u'u',
            ord(u'ụ'): u'u',
            ord(u'ư'): u'u',
            ord(u'ứ'): u'u',
            ord(u'ừ'): u'u',
            ord(u'ử'): u'u',
            ord(u'ữ'): u'u',
            ord(u'ự'): u'u',
            ord(u'ý'): u'y',
            ord(u'ỳ'): u'y',
            ord(u'ỷ'): u'y',
            ord(u'ỹ'): u'y',
            ord(u'ỵ'): u'y',
        }

    def convert(self, s):
        try:
            return s.decode('utf8').translate(self.char_dict)
        except UnicodeEncodeError:
            return s.translate(self.char_dict)


def weighting_suggester(suggester_list):
    ret_list = []
    min_length = float(min([len(x[0]) for x in suggester_list]))
    max_score = 0.0
    max_score_count = 1
    suggester_list.sort(reverse=True)

    for suggester in suggester_list:
        score = 0.0
        score += 1 / (len(suggester[0]) / min_length)

        try:
            suggester[0].decode('utf-8')
            score += 0
        except UnicodeEncodeError:
            score += 1

        ret_list.append({
            'Id': suggester[0],
            'Score': score
        })

        if score == max_score:
            max_score_count += 1

        if score > max_score:
            max_score = score
            max_score_count = 1
            std_length = float(len(suggester[0]))

    if max_score_count > 1:
        return weighting_suggester_more(ret_list, max_score, std_length)
    return max_score, ret_list


def weighting_suggester_more(weighted_suggester, max_score, length):
    new_max_score = max_score

    for suggester in weighted_suggester:
        if suggester['Score'] == max_score:
            suggester['Score'] = new_max_score + 1/(length + 1)

            if suggester['Id'].isupper() and ' ' in suggester['Id']:
                suggester['Score'] -= 1 / length

            if re.search(r'[\t\n]', suggester['Id']):
                suggester['Score'] -= 1 / length

            if suggester['Score'] > new_max_score:
                new_max_score = suggester['Score']

    return new_max_score, weighted_suggester


def escape_double_quote(s):
    s_split = s.split('"')
    s_split_len = len(s_split)
    if s_split_len > 1:
        s = ''
        for i in range(s_split_len):
            s += s_split[i]
            if i < s_split_len - 1:
                s += '""'
    return s
