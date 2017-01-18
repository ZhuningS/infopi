#! /usr/bin/python3
# coding=utf-8

import sys

import bvars
import workers

import source_manage
from worker_manage import test_source


def find_idle():
    # load sources
    source_manage.load_sources()

    # load users config
    from user_manage import c_user_cfg
    user_list = c_user_cfg.load_users()

    # add user used
    user_used = set()
    for user in user_list:
        for cate, lst in user.category_list:
            for item in lst:
                # add sid
                user_used.add(item[0])

    # not used source
    not_used_lst = list()
    for sid, source in bvars.sources.items():
        if sid not in user_used:
            not_used_lst.append((sid, source.name))
    not_used_lst.sort(key=lambda tup: tup[0])

    # print
    print('以下信息源未被用户直接使用，包括了未被直接使用的父信息源：')
    last = None
    for sid, name in not_used_lst:
        p1, p2 = sid.split(':')
        if last != p1:
            print('\n<%s>' % p1)
        last = p1

        print('{0:<14}{1}'.format(p2, name))


def main():
    show_msg = False

    if len(sys.argv) == 2:
        arg1 = sys.argv[1]

        # test a source
        if ':' in arg1:
            # load sources
            source_manage.load_sources(test_sid=arg1)

            if arg1 in bvars.sources:
                test_source(arg1)
            else:
                print('没有加载信息源%s' % arg1)

        # test entire cfg
        elif arg1.lower() == 'cfg':
            # global config
            from gconfig import load_config
            load_config()

            # load sources
            source_manage.load_sources()

            # users config
            from user_manage import c_user_cfg
            c_user_cfg.load_users()

        # find idle sources
        elif arg1.lower() == 'idle':
            find_idle()

        else:
            show_msg = True

    else:
        show_msg = True

    if show_msg:
        s = '''\
使用方法：

./test_source.py cate:source
测试某个指定的信息源

./test_source.py cfg
尝试加载config.ini、加载所有信息源、加载所有用户配置

./test_source.py idle
罗列未被直接使用的信息源
'''
        print(s)

if __name__ == '__main__':
    main()
