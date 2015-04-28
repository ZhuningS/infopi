# coding=utf-8

import xml.etree.ElementTree as ET
import html

try:
    from lxml import etree
except:
    etree = None

from red import *
from worker_manage import worker, dataparser
from fetcher import *
from datadefine import *

def de_html_char(text):
    '''去掉html转义'''

    if text == None:
        return ''

    # 去标签
    text = red.sub(r'''<(?:[^"'>]|"[^"]*"|'[^']*')*>''', r'', text)

    # 去转义
    text = html.unescape(text)

    # 0xe38080转空格
    text = text.replace('　', ' ')

    # 空白
    text = text.replace('\n', ' ')
    text = red.sub(r'\s{3,}', r'  ', text)
    text = text.strip()
    
    return text


# rss1.0, rss2.0, atom
tagnames = {
        'f_author': ('channel/title', 'channel/title', 'title'),
        'f_items' : ('channel/item', 'channel/item', 'entry'),
        
        'title'   : ('title', 'title', 'title'),
        'url'     : ('link', 'link', 'link'),
        
        'author'  : ('author', 'author', 'author'),
        'summary' : ('description', 'description', 'summary'),
        'pub_date': ('dc:date', 'pubDate', 'updated'),
        
        'guid'    : ('guid', 'guid', 'id')
        }

# parse xml
def parse_xml(data_dict, xml):
    if not xml:
        raise c_worker_exception('xml为空字符串', data_dict['url'], '')
    
    r = red.d(r'^\s*$')
    if r.match(xml) != None:
        raise c_worker_exception('xml只有空白', data_dict['url'], '')

    # remove namespace of atom
    xml = red.sub(r'''<feed\s+(?:"[^"]*"|'[^']*'|[^"'>])*>''',
                  r'<feed>',
                  xml, 
                  count=1,
                  flags=red.IGNORECASE)

    # ElementTree
    try:
        doc = ET.fromstring(xml)
    except Exception as e:
        doc = None
        if not etree:
            raise c_worker_exception('解析XML失败，可以尝试安装lxml',
                                     data_dict['url'],
                                     str(e)
                                     )

    # lxml
    if doc == None:
        try:
            parser = etree.XMLParser(recover=True, encoding='utf-8')
            doc = etree.fromstring(xml.encode('utf-8'), parser=parser)
            print('使用lxml解析%s' % data_dict['url'])
        except Exception as e:
            raise c_worker_exception('lxml解析XML失败',
                                     data_dict['url'],
                                     str(e)
                                     )

    # get type of the feed
    if doc.tag == 'rss' and doc.get('version', '') == '1.0':
        feedtype = 0
    elif doc.tag == 'rss' and doc.get('version', '') == '2.0':
        feedtype = 1
    elif doc.tag == 'feed':
        feedtype = 2
    else:
        raise c_worker_exception('无法识别XML的feed类型', data_dict['url'], '')

    # get feed author
    if 'use_feed_author' in data_dict:
        f_author = de_html_char(doc.findtext(tagnames['f_author'][feedtype]))

    item_iter = doc.findall(tagnames['f_items'][feedtype])
    if item_iter == None:
        return []

    ret = []
    for item in item_iter:
        # ------- info -------
        one = c_info()
        
        # title
        one.title = de_html_char(item.findtext(tagnames['title'][feedtype]))

        # url
        if feedtype < 2:
            url = de_html_char(item.findtext(tagnames['url'][feedtype]))
        else:
            url = ''
            link_iter = item.findall('link')
            if link_iter != None:
                for tag_link in link_iter:
                    if tag_link.get('rel') == 'alternate':
                        if tag_link.get('type') == 'text/html':
                            url = tag_link.get('href')
                            break
                        url = url or tag_link.get('href')
        one.url = url
        
        # author, summary, pub_date
        if 'use_feed_author' in data_dict:
            one.author = de_html_char(
                            item.findtext(tagnames['author'][feedtype],
                            f_author)
                            )
        one.summary = de_html_char(item.findtext(tagnames['summary'][feedtype]))
        one.pub_date = de_html_char(item.findtext(tagnames['pub_date'][feedtype]))
        
        # suid
        guid = item.findtext(tagnames['guid'][feedtype])
        one.suid = guid or one.url or one.title

        ret.append(one)

    return ret

# download and parse
@worker('rss_atom')
def download_process(data_dict, worker_dict):
    url = data_dict['url']
    encoding = data_dict.get('encoding', '')

    f = Fetcher()
    xml = f.fetch_html(url, encoding)

    return parse_xml(data_dict, xml)


@dataparser('rss_atom')
def rss_atom_parser(xml_string):
    d = dict()
    data = ET.fromstring(xml_string).find('data')

    url_tag = data.find('url')
    d['url'] = url_tag.text.strip()

    str_encoding = url_tag.attrib.get('encoding', '').strip()
    d['encoding'] = Fetcher.lookup_encoding(str_encoding)

    use_feed_author = data.find('use_feed_author')
    if use_feed_author != None:
        d['use_feed_author'] = True

    return d
