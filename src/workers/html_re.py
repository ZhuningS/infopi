# coding=utf-8

from urllib.parse import urljoin
import html
import xml.etree.ElementTree as ET

from red import *
from worker_manage import worker, dataparser

from fetcher import *
from datadefine import *

__all__ = ()


def de_html_char(text):
    '''去掉html转义'''

    # 去标签
    text = red.sub(r'''<(?:[^"'>]|"[^"]*"|'[^']*')*>''', r'', text)

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

# 处理映射规则


def map_attrs(m, one):
    if type(one) == int:
        return de_html_char(m.group(one))
    elif type(one) == tuple:
        s = ''
        for i in one:
            if type(i) == int:
                s += de_html_char(m.group(i))
            elif type(i) == str:
                s += i
        return s
    elif type(one) == str:
        return one
    else:
        print('map_rule的定义出现错误')


def pattern_error(blocknum, isblock=True):
    if isblock:
        s = '第%d个block的blockre编译失败' % (blocknum + 1)
    else:
        s = '第%d个block的itemre编译失败' % (blocknum + 1)

    raise c_worker_exception('正则表达式编译失败',
                             '',
                             s)


def parse_html(data_dict, base_url, html):
    if not html:
        raise c_worker_exception('html为空字符串', data_dict['url'], '')

    r = red.d(r'^\s*$')
    if r.match(html) is not None:
        raise c_worker_exception('html只有空白', data_dict['url'], '')

    re_lst = data_dict['blocks_list']
    ret = list()

    for i, block in enumerate(re_lst):

        # block re
        block_prog = red.d(block[0][0], block[0][1])
        if block_prog is None:
            pattern_error(i)

        itr = block_prog.finditer(html)
        matches = list(itr)
        if len(matches) != 1:
            s = '第%d个block的block_re找到的结果为%d，应为1' % \
                (i + 1, len(matches))
            raise c_worker_exception(s, '',
                                     '可能是网页改版、服务器显示错误信息')
        subhtml = matches[0].group(1)

        # item re
        item_prog = red.d(block[1][0], block[1][1])
        if item_prog is None:
            pattern_error(i, False)

        itr = item_prog.finditer(subhtml)
        matches = list(itr)
        if not matches:
            s = '第%d个block的item_re找到的结果为0，应大于0' % (i + 1)
            raise c_worker_exception(s, '', '可能是网页改版')

        for m in matches:
            info = c_info()

            for k, v in block[2].items():
                try:
                    ss = map_attrs(m, v)
                except Exception as e:
                    s1 = '处理第%d个block的map_rule时异常' % (i + 1)
                    s2 = '赋值%s给%s时出错，%s' % (str(v), str(k), str(e))
                    raise c_worker_exception(s1, '', s2)

                if k == 'title':
                    info.title = ss
                elif k == 'url':
                    info.url = ss
                elif k == 'urljoin':
                    info.url = urljoin(base_url, ss)
                elif k == 'summary':
                    info.summary = ss
                elif k == 'author':
                    info.author = ss
                elif k == 'pub_date':
                    info.pub_date = ss
                elif k == 'suid':
                    info.suid = ss
                elif k == 'temp':
                    info.temp = ss
                else:
                    print('无法处理map_rule', k, v)

                if not info.suid:
                    info.suid = info.url

            ret.append(info)

    return ret


# download and parse
@worker('html_re')
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


@dataparser('html_re')
def html_re_parser(xml_string):
    d = dict()
    data = ET.fromstring(xml_string).find('data')

    url_tag = data.find('url')
    if url_tag is not None:
        d['url'] = url_tag.text.strip()

        str_encoding = url_tag.attrib.get('encoding', '').strip()
        d['encoding'] = Fetcher.lookup_encoding(str_encoding)

        str_errors = url_tag.attrib.get('errors', '').strip()
        d['errors'] = str_errors

    blocks = data.findall('block')
    if blocks:
        block_list = list()

        for block in blocks:
            blockre = block.find('blockre')
            blockre_re = process_multiline(blockre.text)
            blockre_flags = process_flags(blockre.attrib.get('flags', ''))

            itemre = block.find('itemre')
            itemre_re = process_multiline(itemre.text)
            itemre_flags = process_flags(itemre.attrib.get('flags', ''))

            map_dict = dict()
            maprules = block.find('maprules')
            for r in maprules.iter():
                if r.tag != 'maprules':
                    value = eval('(' + r.text.strip() + ')')

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

            tu = ((blockre_re, blockre_flags),
                  (itemre_re, itemre_flags),
                  map_dict
                  )
            block_list.append(tu)

        d['blocks_list'] = block_list

    return d


# worker: html_re_rev
# get a reversed list from html_re
# this worker rely on html_re

@worker('html_re_rev')
def rev_worker(data_dict, worker_dict):
    lst = download_process(data_dict, worker_dict)
    return lst[::-1]


@dataparser('html_re_rev')
def rev_parser(xml_string):
    return html_re_parser(xml_string)
