# coding=utf-8

import os
import copy
import codecs
import xml.etree.ElementTree as ET

import bvars

from worker_manage import parse_data

__all__ = ('load_sources')

sources = bvars.sources

class c_source:
    __slots__ = ('source_id',
                 'name', 'comment', 'link',
                 'worker_id', 'data',
                 'callback')
    def __init__(self):
        self.source_id = ''

        self.name = ''
        self.comment = ''
        self.link = ''

        self.worker_id = ''
        self.data = None

        self.callback = None


temp_dict = None

def load_xml(sources_path, path, filename):
    def get_text_from_tag(tag):
        if tag != None:
            return tag.text.strip()
        else:
            return ''

    global temp_dict

    full_path = os.path.join(sources_path, path, filename)

    # load file
    try:
        f = open(full_path, 'rb')
        byte_data = f.read()
        f.close()
    except Exception as e:
        print('读取文件%s:%s时出错' % (path, filename), str(e))
        return

    # decode
    try:
        if len(byte_data) >= 3 and byte_data[:3] == codecs.BOM_UTF8:
            byte_data = byte_data[3:]

        string = byte_data.decode('utf-8')
    except Exception as e:
        print('文件%s:%s解码失败，请确保是utf-8编码。' % (path, filename),
              '(有没有BOM无所谓)\n', str(e), '\n')
        return

    # lower and remove '.xml'
    lpath = path.lower()
    lfilename = filename.lower()
    
    if len(lfilename) < 5 or lfilename[-4:] != '.xml':
        return

    short_fn = lfilename[:-4]

    # windows不区分大小写，不必提示
    if short_fn in temp_dict:
        #print('提示:\n%s:%s已存在，本程序不区分大小写' % (lpath, short_fn))
        return

    # get xml object
    try:
        xml = ET.fromstring(string)
    except Exception as e:
        s = ('解析信息源XML文件失败, %s:%s\n'
             '异常:%s\n'
             '如果XML中出现如下字符，请转义替换:\n'
             '  &替换成&amp;\n'
             '  <替换成&lt;\n'
             '  >替换成&gt;\n'
             '请搜索以下关键字了解详情：xml 实体 转义\n'
             )
        print(s % (lpath, lfilename, str(e)))
        return

    # load father first
    father = xml.attrib.get('father', '')
    if father and father not in temp_dict:
        load_xml(sources_path, path, father+'.xml')

    # make source object
    s = c_source()
    s.source_id = lpath + ':' + short_fn

    s.name = get_text_from_tag(xml.find('name'))
    s.comment = get_text_from_tag(xml.find('comment'))
    s.link = get_text_from_tag(xml.find('link'))

    s.worker_id = get_text_from_tag(xml.find('worker'))
    s.data = parse_data(s.worker_id, string)

    callback = get_text_from_tag(xml.find('callback'))
    if callback != '':
        s.callback = compile(callback, '<string>', 'exec')

    # use father data
    if father:
        # data
        father_data = copy.deepcopy(temp_dict[father].data)
        father_data.update(s.data)
        s.data = father_data
        # callback
        if s.callback == None:
            s.callback = temp_dict[father].callback

    # add to dict
    temp_dict[short_fn] = s
    sources[s.source_id] = s


def load_sources():
    # clear first
    sources.clear()

    # sources_path
    sources_path = os.path.join(bvars.root_path, 'cfg')

    # for load father data
    global temp_dict
    temp_dict = dict()

    # load files
    for l1_item in os.listdir(sources_path):
        l1_path = os.path.join(sources_path, l1_item)
        temp_dict.clear()

        if not os.path.isfile(l1_path):
            for l2_item in os.listdir(l1_path):
                l2_path = os.path.join(l1_path, l2_item)
                if os.path.isfile(l2_path):
                    load_xml(sources_path, l1_item, l2_item)

    print('back-side loaded %d sources' % len(sources))

    del temp_dict

