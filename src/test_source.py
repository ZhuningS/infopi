# coding=utf-8

import sys

import bvars
import workers

import source_manage
source_manage.load_sources()

from worker_manage import test_source

def main():
    if len(sys.argv) > 1:
        arg1 = sys.argv[1]
        if arg1 in bvars.sources:
            test_source(arg1)
        else:
            print('没有加载信息源%s' % arg1)
    elif len(sys.argv) == 1:
        print('正在尝试加载全局配置文件、用户配置文件')
        
        # global config
        from gconfig import load_config
        load_config('test_source', 0, '')
        
        print()
        
        # users config
        from user_manage import c_user_cfg
        c_user_cfg.load_users()

if __name__ == '__main__':
    main()