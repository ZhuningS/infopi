# coding=utf-8

import json
import html
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

from red import *
from worker_manage import worker, dataparser

from fetcher import *
from datadefine import *

__all__ = ()


def item_process(text):
    text = str(text)

    # 去转义
    text = html.unescape(text)

    # U+3000、U+200B转空格
    text = text.replace('\u3000', ' ')
    text = text.replace('\u200b', ' ')

    # 空白
    text = text.replace('\n', ' ')
    text = red.sub(r'\s{3,}', r'  ', text)
    text = text.strip()

    return text


def parse_html(data_dict, base_url, html):
    if not html:
        raise c_worker_exception('html为空字符串', data_dict['url'], '')

    r = red.d(r'^\s*$')
    if r.match(html) is not None:
        raise c_worker_exception('html只有空白', data_dict['url'], '')

    # extract json string
    re = red.d(data_dict['re_pattern'], data_dict['re_flags'])
    if re is None:
        raise c_worker_exception('正则表达式编译失败',
                                 '',
                                 '用于提取json字符串的正则表达式编译失败')

    m = re.search(html)
    if m is None:
        raise c_worker_exception('无法用re(正则表达式)提取json字符',
                                 data_dict['url'],
                                 '')
    json_str = m.group(1)

    # replace
    if 'repl' in data_dict:
        r = red.d(data_dict['repl_pattern'], data_dict['repl_flags'])
        if r is None:
            raise c_worker_exception('replace正则表达式编译失败')

        json_str = r.sub(data_dict['repl'], json_str)

    # parse json
    try:
        json_obj = json.loads(json_str)
    except Exception as e:
        raise c_worker_exception('解析json时出错',
                                 data_dict['url'],
                                 str(e))

    # blocks
    json_lst = data_dict['blocks_list']
    ret = list()

    for i, block in enumerate(json_lst):

        # travel path
        path = block[0]
        block_j = json_obj

        # path必定为tuple
        for ii, path_item in enumerate(path):
            try:
                block_j = block_j[path_item]
            except:
                s = '第%d个block, block_path的第%d个路径元素%s无效'
                raise c_worker_exception(
                    s % (i + 1, ii + 1, str(path_item)),
                    data_dict['url'],
                    'path:%s 可能是网站改变了json的设计结构' % str(path)
                )

        # extract
        if type(block_j) == list:
            pass
        elif type(block_j) == dict:
            block_j = block_j.values()
        else:
            s = '第%d个block, block_path找到的不是列表或字典'
            raise c_worker_exception(
                s % (i + 1),
                data_dict['url'],
                'path:%s 可能是网站改变了json的设计结构' % str(path)
            )

        for block_item_j in block_j:
            info = c_info()

            for key, sub_path in block[1].items():

                temp_jj = block_item_j
                for sub_path_item in sub_path:
                    try:
                        temp_jj = temp_jj[sub_path_item]
                    except Exception as e:
                        print('异常：', e)
                        s1 = '处理第%d个block的映射时异常' % (i + 1)
                        s2 = 'path:%s,key:%s,map:%s,无法找到指定元素%s.' % \
                             (str(path), key, str(sub_path),
                              str(sub_path_item))
                        raise c_worker_exception(s1, '', s2)
                ss = item_process(temp_jj)

                if key == 'title':
                    info.title = ss
                elif key == 'url':
                    info.url = ss
                elif key == 'summary':
                    info.summary = ss
                elif key == 'author':
                    info.author = ss
                elif key == 'pub_date':
                    info.pub_date = ss
                elif key == 'urljoin':
                    info.url = urljoin(base_url, ss)
                elif key == 'suid':
                    info.suid = ss
                elif key == 'temp':
                    info.temp = ss
                else:
                    print('无法处理map_rule', key, sub_path)

                if not info.suid:
                    info.suid = info.url

            ret.append(info)

    return ret


# download and parse
@worker('html_json')
def download_process(data_dict, worker_dict):
    url = data_dict['url']
    encoding = data_dict.get('encoding', '')
    errors = data_dict.get('errors', '')

    f = Fetcher()
    string = f.fetch_html(url, encoding, errors)

    return parse_html(data_dict, url, string)


def process_multiline(string):
    ret = ''

    lines = string.strip().split('\n')
    for line in lines:
        ret += line.strip()

    return ret


def process_flags(string):
    def is_this(upper_flag, s1, s2):
        if upper_flag == s1 or upper_flag == s2:
            return True
        else:
            return False

    flags = string.strip().split()

    ret = 0
    for flag in flags:
        f = flag.upper()

        if is_this(f, 'ASCII', 'A'):
            ret |= red.ASCII
        elif 'DEBUG' == f:
            ret |= red.DEBUG
        elif is_this(f, 'IGNORECASE', 'I'):
            ret |= red.IGNORECASE
        elif is_this(f, 'LOCALE', 'L'):
            ret |= red.LOCALE
        elif is_this(f, 'MULTILINE', 'M'):
            ret |= red.MULTILINE
        elif is_this(f, 'DOTALL', 'S'):
            ret |= red.DOTALL
        elif is_this(f, 'VERBOSE', 'X'):
            ret |= red.VERBOSE
        else:
            print('错误!未知的正则模式:', flag)

    return ret


@dataparser('html_json')
def html_json_parser(xml_string):
    d = dict()
    data = ET.fromstring(xml_string).find('data')

    url_tag = data.find('url')
    if url_tag is not None:
        d['url'] = url_tag.text.strip()

        str_encoding = url_tag.attrib.get('encoding', '').strip()
        d['encoding'] = Fetcher.lookup_encoding(str_encoding)

        str_errors = url_tag.attrib.get('errors', '').strip()
        d['errors'] = str_errors

    # extract json string
    re_tag = data.find('re')
    if re_tag is not None:
        d['re_pattern'] = process_multiline(re_tag.text)
        d['re_flags'] = process_flags(re_tag.attrib.get('flags', ''))

    # replace
    replace_tag = data.find('replace')
    if replace_tag is not None:
        replace_re_tag = replace_tag.find('re')
        d['repl_pattern'] = process_multiline(replace_re_tag.text)
        d['repl_flags'] = process_flags(replace_re_tag.attrib.get('flags', ''))

        repl_tag = replace_tag.find('repl')
        d['repl'] = process_multiline(repl_tag.text)

    blocks = data.findall('block')
    if blocks:
        block_list = list()

        for block in blocks:
            path = block.find('block_path')
            if not path.text or not path.text.strip():
                s = '()'
            else:
                s = '(' + path.text.strip() + ',)'
            path_value = eval(s)

            map_dict = dict()
            for r in block.iter():
                if r.tag != 'block' and r.tag != 'block_path':
                    value = eval('(' + r.text.strip() + ',)')

                    if r.tag == 'url':
                        str_urljoin = r.attrib.get('urljoin', '')
                        if not str_urljoin:
                            map_dict['url'] = value
                        elif (str_urljoin.isdigit() and int(str_urljoin)) or \
                                str_urljoin.lower() == 'true':
                            map_dict['urljoin'] = value
                        else:
                            map_dict['url'] = value
                    else:
                        map_dict[r.tag] = value

            tu = (path_value, map_dict)
            block_list.append(tu)

        d['blocks_list'] = block_list

    return d
