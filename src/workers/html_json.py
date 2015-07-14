# coding=utf-8

import json
import html
import xml.etree.ElementTree as ET

from red import *
from worker_manage import worker, dataparser

from fetcher import *
from datadefine import *

__all__ = ()

def item_process(text):
    text = str(text)

    # 去转义
    text = html.unescape(text)

    # 0xe38080转空格
    text = text.replace('　', ' ')

    # 空白
    text = text.replace('\n', ' ')
    text = red.sub(r'\s{3,}', r'  ', text)
    text = text.strip()

    return text

# 处理映射规则
def map_attrs(obj, one):
    t = type(one)
    if t == str or t == int:
        return item_process(obj[one])
    elif t == tuple:
        s = ''
        for i in one:
            s += item_process(obj[i])
        return s
    else:
        print('html_json的映射定义出现错误')

def parse_html(data_dict, base_url, html):
    if not html:
        raise c_worker_exception('html为空字符串', data_dict['url'], '')
    
    r = red.d(r'^\s*$')
    if r.match(html) != None:
        raise c_worker_exception('html只有空白', data_dict['url'], '')
    
    # extract json string
    re = red.d(data_dict['re_pattern'], data_dict['re_flags'])
    m = re.search(html)
    if m == None:
        raise c_worker_exception('无法用re(正则表达式)提取json字符', 
                                 data_dict['url'], 
                                 '')
    json_str = m.group(1)

    # parse json
    try:
        j = json.loads(json_str)
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
        temp_j = j
        
        # path必定为tuple
        for ii, one in enumerate(path):
            try:
                temp_j = temp_j[one]
            except:
                s = '第%d个block, block_path的第%d个路径元素%s无效'
                raise c_worker_exception(
                    s % (i+1, ii+1, str(one)), 
                    data_dict['url'], 
                    'path:%s 可能是网站改变了json的设计结构' % str(path)
                    )

        # extract
        if type(temp_j) == dict:
            temp_j = temp_j.values()
        elif type(temp_j) != list:
            s = '第%d个block, block_path找到的不是列表或字典'
            raise c_worker_exception(
                    s % (i+1), 
                    data_dict['url'], 
                    'path:%s 可能是网站改变了json的设计结构' % str(path)
                    )    

        for o in temp_j:
            info = c_info()

            for k, v in block[1].items():
                try:
                    ss = map_attrs(o, v)
                except Exception as e:
                    s1 = '处理第%d个block的映射时异常' % (i+1)
                    s2 = 'path:%s, 无法找到指定元素%s。' % (str(path), str(v))
                    raise c_worker_exception(s1, '', s2)

                if k == 'title':
                    info.title = ss
                elif k == 'url':
                    info.url = ss
                elif k == 'summary':
                    info.summary = ss
                elif k == 'author':
                    info.author = ss
                elif k == 'pub_date':
                    info.pub_date = ss
                elif k == 'suid':
                    info.suid = ss
                else:
                    print('无法处理map_rule', k, v)

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
    if url_tag != None:
        d['url'] = url_tag.text.strip()

        str_encoding = url_tag.attrib.get('encoding', '').strip()
        d['encoding'] = Fetcher.lookup_encoding(str_encoding)
        
        str_errors = url_tag.attrib.get('errors', '').strip()
        d['errors'] = str_errors
        
    re_tag = data.find('re')
    if re_tag != None:
        d['re_pattern'] = process_multiline(re_tag.text)
        d['re_flags'] = process_flags(re_tag.attrib.get('flags', ''))

    blocks = data.findall('block')
    if blocks:
        block_list = list()

        for block in blocks:
            path = block.find('block_path')
            path_value = eval('(' + path.text.strip() + ',)')
            
            map_dict = dict()
            for r in block.iter():
                if r.tag != 'block' and r.tag != 'block_path':
                    value = eval('(' + r.text.strip() + ')')
                    map_dict[r.tag] = value

            tu = (path_value, map_dict)
            block_list.append(tu)

        d['blocks_list'] = block_list

    return d
