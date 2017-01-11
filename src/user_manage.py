# coding=utf-8
import os
import codecs

from red import *

import bvars


class c_user_cfg:
    __slots__ = ('username', 'password',
                 'col_per_page', 'col_per_page_pad',
                 'col_per_page_bigmobile',
                 'usertype', 'show_exceptions', 'category_list')

    def __init__(self):
        self.username = ''
        self.password = ''
        self.col_per_page = -1
        self.col_per_page_pad = -1
        self.col_per_page_bigmobile = -1

        # 0:public, 1:normal, 2:admin
        self.usertype = 1

        # 是否显示异常信息
        self.show_exceptions = True

        # 组织结构列表
        # 列表的元素为tuple: (category, <list>)
        # <list>的元素为list:
        # [sid, level, interval, name, comment, link, last_fetch_str]
        self.category_list = list()

    @staticmethod
    def load_users(cfg=None):
        users_path = os.path.join(bvars.root_path, 'cfg')

        user_cfg_list = list()

        for item in os.listdir(users_path):
            fpath = os.path.join(users_path, item)
            if os.path.isfile(fpath) and \
               item.lower().endswith('.txt'):
                user_cfg = c_user_cfg.parse_cfg(cfg, fpath, item)
                if user_cfg:
                    user_cfg_list.append(user_cfg)

        print('载入了%d个用户' % len(user_cfg_list))
        return user_cfg_list

    @staticmethod
    def parse_cfg(cfg, f_fullpath, f_filename):

        # load file
        try:
            f = open(f_fullpath, 'rb')
            byte_data = f.read()
            f.close()
        except Exception as e:
            print('读取文件%s时出错' % f_filename, str(e))
            return None

        # decode
        try:
            if byte_data[:3] == codecs.BOM_UTF8:
                byte_data = byte_data[3:]

            text = byte_data.decode('utf-8')
        except Exception as e:
            print('文件%s解码失败，请确保是utf-8编码。' % f_filename,
                  '(有没有BOM无所谓)\n', str(e), '\n')
            return None

        # to \n
        text = text.replace('\r\n', '\n')
        text = text.replace('\r', '\n')

        lines = text.split('\n')

        user = c_user_cfg()
        # username
        user.username = f_filename.lower().rstrip('.txt')

        # compiled re
        re_category = red.d(r"^\s*'(.*?)'\s*(?:#.*)?$")
        pattern = (r"^\s*'(.*?)'\s*,\s*(\d+)\s*,"
                   r"\s*([\d.](?:[\d. */+-]*[\d.])?)\s*(?:#.*)?$")
        re_source = red.d(pattern)

        orgnise_started = False
        current_category = None
        for i, line in enumerate(lines):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if not orgnise_started:
                if line.startswith('[organise]'):
                    orgnise_started = True
                    continue

                split_lst = line.split('=', 1)
                k = split_lst[0].strip()
                v = split_lst[1].strip()

                if k == 'password' and len(v) >= 2 and \
                   v.startswith("'") and v.endswith("'"):
                    user.password = v[1:-1]
                elif k == 'col_per_page':
                    try:
                        user.col_per_page = int(v)
                    except:
                        pass
                elif k == 'col_per_page_pad':
                    try:
                        user.col_per_page_pad = int(v)
                    except:
                        pass
                elif k == 'col_per_page_bigmobile':
                    try:
                        user.col_per_page_bigmobile = int(v)
                    except:
                        pass
                elif k == 'usertype':
                    if v == 'public':
                        user.usertype = 0
                    elif v == 'normal':
                        user.usertype = 1
                    elif v == 'admin':
                        user.usertype = 2
                elif k == 'show_exceptions':
                    try:
                        user.show_exceptions = bool(int(v))
                    except:
                        pass
                else:
                    s = '文件%s出现错误，未知键值:\n%s'
                    print(s % (f_filename, k))

            # orgnise started
            else:
                # category
                m = re_category.search(line)
                if m:
                    category_name = m.group(1).strip().replace('/', '\\')
                    user.category_list.append((category_name, list()))
                    current_category = user.category_list[-1][1]

                else:
                    # source
                    m = re_source.search(line)
                    if m:
                        sid = m.group(1).lower()
                        level = int(m.group(2))
                        if level not in {0, 1, 2}:
                            level = 0

                        try:
                            interval = eval(m.group(3))
                            if interval < 0:
                                raise Exception()
                        except:
                            s = ('计算%s用户的%s信息源的刷新间隔时出错'
                                 '：%s，使用默认值作为刷新间隔。')
                            print(s % (user.username, sid, m.group(3)))
                            interval = 0

                        if current_category is not None:
                            # ! sync comments in
                            # backprocess.py and db_wrapper.py
                            # with this list
                            current_category.append([sid, level, interval,
                                                     'name', 'comment', 'link',
                                                     'last_fetch', 'max_db'])
                        else:
                            s = '文件%s出现错误，缺少分类:\n%s'
                            print(s % (f_filename, line))

                    else:
                        s = '\n文件%s出现错误，格式有误\n%s\n'
                        print(s % (f_filename, line))

        if cfg:
            if user.col_per_page == -1:
                user.col_per_page = cfg.default_colperpage
            if user.col_per_page_pad == -1:
                user.col_per_page_pad = cfg.default_pad_colperpage
            if user.col_per_page_bigmobile == -1:
                user.col_per_page_bigmobile = cfg.default_bigmobile_colperpage

        return user
