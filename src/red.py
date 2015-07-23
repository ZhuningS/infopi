# coding=utf-8

import threading
import re

#========================================
#       regular expression wrapper
#========================================

class red:
    A = re.A
    ASCII = re.ASCII

    DEBUG = re.DEBUG

    I = re.I
    IGNORECASE = re.IGNORECASE

    L = re.L
    LOCALE = re.LOCALE

    M = re.M
    MULTILINE = re.MULTILINE

    S = re.S
    DOTALL = re.DOTALL

    X = re.X
    VERBOSE = re.VERBOSE
    
    # cache
    regexs = dict()

    # threading lock
    lock = threading.Lock()

    @staticmethod
    def d(re_str, flags=0):
        '''compiled pattern cache'''
        red.lock.acquire()

        compiled = red.regexs.get((re_str, flags), 0)
        if compiled == 0:
            try:
                compiled = re.compile(re_str, flags)
            except Exception as e:
                print('编译正则表达式时出现异常:', e)
                print('正则式:', re_str)
                print('模式:', flags, '\n')
                compiled = None
            red.regexs[(re_str, flags)] = compiled

        red.lock.release()

        return compiled

    @staticmethod
    def sub(pattern, repl, string, count=0, flags=0):
        prog = red.d(pattern, flags)
        if prog != None:
            return prog.sub(repl, string, count=0)
        else:
            return ''

    @staticmethod
    def clear_cache():
        red.lock.acquire()
        red.regexs.clear()
        red.lock.release()

