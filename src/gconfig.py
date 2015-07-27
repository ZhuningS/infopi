# coding=utf-8

from enum import IntEnum
import os
import datetime
import codecs

from red import red

import bvars

class VALUE_TYPE(IntEnum):
    STRING = 0
    INT = 1
    INT_TUPLE_2 = 2

class c_runcfg:
    def __init__(self):
        self.max_entries = 50

        self.title_len = 70
        self.summary_len = 160
        self.author_len = 50
        self.pub_date_len = 50

class c_config:
    s_boot_time = None

    def __init__(self):
        self.version = ''

        if not c_config.s_boot_time:
            c_config.s_boot_time = datetime.datetime.\
                                   now().strftime('%Y-%m-%d %H:%M:%S %w')

        self.boot_time = c_config.s_boot_time

        # default values
        self.default_colperpage = 15
        self.default_pad_colperpage = 12
        self.default_bigmobile_colperpage = 12
        self.mobile_colperpage = 10

        # task controller
        self.task_pipes = 3
        self.task_timeout = 900
        self.default_source_interval = 3600
        self.tasks_suspend = False

        # database auto maintance
        self.db_process_at = (4, 00)
        self.db_process_interval = 0
        self.db_process_del_entries = 300
        self.db_process_del_days = 30
        self.db_process_rm_ghost = 1
        self.db_backup_maxfiles = 20


        # run-time status
        self.web_port = 0
        self.https = False
        self.root_path = ''
        self.tmpfs_path = ''

        self.start_time = 0
        self.web_pid = 0
        self.back_pid = 0

        self.static_folder = 'static'
        self.template_folder = 'templates'

        self.runcfg = c_runcfg()


def load_config(version='test', web_port=0,
                https=False, tmpfs_path=''):
    def get_value(string, t):

        if t == VALUE_TYPE.INT:
            p = red.d(r'^(-?\d+)\s*(?:#.*)?$')
            m = p.search(string)
            if m:
                try:
                    return int(m.group(1))
                except:
                    return None

        elif t == VALUE_TYPE.INT_TUPLE_2:
            p = red.d(r'^(\d+)\D+(\d+)\s*(?:#.*)?$')
            m = p.search(string)
            if m:
                try:
                    return (int(m.group(1)), int(m.group(2)))
                except:
                    return None

        elif t == VALUE_TYPE.STRING:
            return string

        return None


    cfg = c_config()
    cfg.version = version

    # run-time status
    cfg.web_port = web_port
    cfg.https = https
    cfg.root_path = bvars.root_path
    cfg.tmpfs_path = tmpfs_path

    cfg.start_time = datetime.datetime.\
                     now().strftime('%Y-%m-%d %H:%M:%S %w')
    cfg.back_pid = os.getpid()
    #cfg.web_pid is not set, send to web-process

    config_path = os.path.join(cfg.root_path, 'cfg', 'config.ini')

    # load file
    try:
        f = open(config_path, 'rb')
        byte_data = f.read()
        f.close()
    except Exception as e:
        print('读取文件config.ini时出错', str(e))
        return cfg

    # decode
    try:
        if byte_data[:3] == codecs.BOM_UTF8:
            byte_data = byte_data[3:]

        text = byte_data.decode('utf-8')
    except Exception as e:
        print('文件config.ini解码失败，请确保是utf-8编码。',
              '(有没有BOM无所谓)\n', str(e), '\n')
        return cfg
   
    # to \n 
    text = text.replace('\r\n', '\n')
    text = text.replace('\r', '\n')

    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        split_lst = line.split('=', 1)
        k = split_lst[0].strip()
        string = split_lst[1].strip()

        # default_colperpage
        if k == 'default_colperpage':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.default_colperpage = v
            else:
                print('default_colperpage', string)
                
        # default_pad_colperpage
        elif k == 'default_pad_colperpage':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.default_pad_colperpage = v
            else:
                print('default_pad_colperpage', string)
                
        # default_bigmobile_colperpage
        elif k == 'default_bigmobile_colperpage':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.default_bigmobile_colperpage = v
            else:
                print('default_bigmobile_colperpage', string)
                
        # mobile_colperpage
        elif k == 'mobile_colperpage':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.mobile_colperpage = v
            else:
                print('mobile_colperpage', string)

        # task control

        elif k == 'task_pipes':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.task_pipes = v
            else:
                print('task_pipes', string)

        elif k == 'task_timeout':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.task_timeout = v
            else:
                print('task_timeout', string)

        elif k == 'default_source_interval':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.default_source_interval = v
            else:
                print('default_source_interval', string)
                
        elif k == 'tasks_suspend':
            v = get_value(string, VALUE_TYPE.INT)
            if v == 1 or v == 0:
                cfg.tasks_suspend = bool(v)
            else:
                print('tasks_suspend', string)

        # fetch setting
        elif k == 'fetch_max_entries':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.runcfg.max_entries = v
            else:
                print('fetch_max_entries', string)  

        elif k == 'fetch_title_len':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.runcfg.title_len = v
            else:
                print('fetch_title_len', string)  

        elif k == 'fetch_summary_len':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.runcfg.summary_len = v
            else:
                print('fetch_summary_len', string)  

        elif k == 'fetch_author_len':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.runcfg.author_len = v
            else:
                print('fetch_author_len', string)  

        elif k == 'fetch_pub_date_len':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.runcfg.pub_date_len = v
            else:
                print('fetch_pub_date_len', string)  


        # database maintenance
        elif k == 'db_process_at':
            v = get_value(string, VALUE_TYPE.INT_TUPLE_2)
            if v and 0 <= v[0] <= 24 and 0 <= v[1] <= 60:
                cfg.db_process_at = v
            else:
                print('db_process_at', string)
                
        elif k == 'db_process_interval':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.db_process_interval = v
            else:
                print('db_process_interval', string)

        elif k == 'db_process_del_entries':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.db_process_del_entries = v
            else:
                print('db_process_del_entries', string)   

        elif k == 'db_process_del_days':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.db_process_del_days = v
            else:
                print('db_process_del_days', string)    

        elif k == 'db_process_rm_ghost':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.db_process_rm_ghost = v
            else:
                print('db_process_rm_ghost', string) 

        elif k == 'db_backup_maxfiles':
            v = get_value(string, VALUE_TYPE.INT)
            if v != None:
                cfg.db_backup_maxfiles = v
            else:
                print('db_backup_maxfiles', string) 

        else:
            print('无法识别的config.ini设置', k)

    cfg.db_process_del_entries = max(cfg.runcfg.max_entries + 1,
                                    cfg.db_process_del_entries)

    return cfg

