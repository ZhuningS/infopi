# coding=utf-8

import html
import xml.etree.ElementTree as ET


from red import *

from worker_manage import worker, dataparser, c_worker_exception

from fetcher import *
from datadefine import *


@worker('pypi')
def do_process(data_dict, worker_dict):
    package = data_dict['package']

    url = 'https://pypi.python.org/pypi/%s/' % package

    f = Fetcher()
    string = f.fetch_html(url)

    if not string:
        raise c_worker_exception('无法下载url', url)

    # single page
    single_re = (r'<span class="breadcrumb-separator">.*?'
                 r'<span class="breadcrumb-separator">.*?'
                 r'<a href="(/pypi/([^/]+)/([^"]+))">.*?'
                 r'class="odd".*?'
                 r'<td>(\d{4}-\d{1,2}-\d{1,2})</td>'
                 )

    prog = red.d(single_re, red.DOTALL)
    m = prog.search(string)
    if m:
        info = c_info()
        info.title = m.group(2) + ' ' + m.group(3)
        info.url = url
        info.pub_date = m.group(4)
        info.suid = info.title

        return [info]

    # table page
    table_re = (r'<tr class="(?:odd|even)">.*?'
                r'<a href="(/pypi/([^/]+)/([^"]+))">'
                r'')
    prog = red.d(table_re, red.DOTALL)
    miter = prog.finditer(string)

    lst = list()
    for m in miter:
        info = c_info()
        info.title = m.group(2) + ' ' + m.group(3)
        info.url = 'https://pypi.python.org' + m.group(1)
        info.suid = info.title

        lst.append(info)
    return lst


@dataparser('pypi')
def html_re_parser(xml_string):
    d = dict()
    data = ET.fromstring(xml_string).find('data')

    url_tag = data.find('package')
    d['package'] = url_tag.text.strip()

    return d
