#! /usr/bin/python3
# coding=utf-8

import sys

import bvars
import workers

import source_manage
source_manage.load_sources()

from worker_manage import test_source

def find_idle():
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
    if len(sys.argv) == 2:
        arg = sys.argv[1]
        
        # it's source
        if ':' in arg:
            if arg in bvars.sources:
                test_source(arg)
            else:
                print('没有加载信息源%s' % arg)
                
        # find idle sources
        elif arg.lower() == 'idle':
            find_idle()
    
    # test cfg
    elif len(sys.argv) == 1:
        print('正在尝试加载全局配置文件、用户配置文件，以下是尝试结果：')
        
        # global config
        from gconfig import load_config
        load_config()
        
        print()
        
        # users config
        from user_manage import c_user_cfg
        c_user_cfg.load_users()

if __name__ == '__main__':
    main()