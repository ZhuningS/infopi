# coding=utf-8
import threading
import time
import datetime
import hashlib

import bvars
from datadefine import *

# translate for wz_tooltip.js (web tooltip)
def for_wz(s):
    # 1st line: for html code
    # 2nd line: no line break
    #           replace("'", '"') this rely on html templates
    
    return s.replace('<', '[').replace('>', ']').replace("'", '"').\
             replace('\n', '').replace('\r', '')

def hasher(string):
    try:
        hashobj = hashlib.md5()
        hashobj.update(string.encode('utf-8'))
        return hashobj.hexdigest()
    except Exception as e:
        print('hasher函数异常', e)
        return ''

def unixtime(string, fmt='%m-%d %H:%M'):
    try:
        return datetime.datetime.\
               fromtimestamp(float(string)).\
               strftime(fmt)
    except Exception as e:
        print('unixtime函数异常', e)
        return ''

class c_worker_exception(Exception):

    def __init__(self, title, url='', summary=''):
        self.title = title
        self.url = url
        self.summary = summary

    def __str__(self):
        s = '异常:' + self.title+'\n'
        if self.url:
            s += self.url+'\n'
        if self.summary:
            s += self.summary+'\n'
        return s


# 启动worker线程
def worker_starter(runcfg, source_id):

    def worker_wrapper(runcfg, 
                       worker, source, worker_dict,  
                       back_web_queue, bb_queue,
                       cfg_token):

        #print('线程开始：%s' % source.source_id)

        int_time = int(time.time())
        is_exception = True

        try:
            if worker == None:
                s = '信息源%s没有找到指定worker: %s' % \
                    (source.source_id, source.worker_id)
                print(s)
                raise c_worker_exception(s)
            
            if source.data == None:
                s = '信息源%s的data未能被解析' % source.source_id
                print(s)
                raise c_worker_exception(s)
                
            lst = worker(source.data, worker_dict)

        except c_worker_exception as e:
            s = '\n源%s出现worker异常:' % source.source_id
            print(s, e)

            i = c_info()
            i.title = '异常:' + e.title
            try:
                i.url = e.url or str(source.data.get('url', ''))
            except:
                pass
            i.summary = e.summary
            i.suid = '<exception>'

            lst = [i]

        except Exception as e:
            print('执行worker时程序异常:', e)

            i = c_info()
            i.title = '程序出现异常'
            i.summary = str(e)
            i.suid = '<exception>'

            lst = [i]

        else:
            # max length of info list
            if len(lst) > runcfg.max_entries:
                lst = lst[:runcfg.max_entries]

            # callback函数
            if source.callback != None:
                newlst = list()
                local_d = dict()
                local_d['hasher'] = hasher
                local_d['unixtime'] = unixtime
                
                for i, info in enumerate(lst):
                    local_d['posi'] = i
                    local_d['info'] = info
                    try:
                        exec(source.callback, None, local_d)
                    except Exception as e:
                        print('callback异常:', e)
                        info.title = 'callback代码异常'
                        info.summary = str(e)
                    
                    if info.temp != 'del':
                        newlst.append(info)

                lst = newlst

            # remove duplicate suid, only keep the first one
            # (escape special suid inside this code)
            suid_set = set()
            newlst = list()
            for one in lst:              
                # escape special suid
                if one.suid == '<exception>':
                    one.suid = '#<exception>#'

                if one.suid not in suid_set:
                    suid_set.add(one.suid)
                    newlst.append(one)

            lst = newlst
            
            is_exception = False

        finally:
            # 通知执行结束
            c_message.make(bb_queue,
                           'bb:source_return',
                           cfg_token,
                           [source.source_id, int_time]
                           )
            
            if not lst:
                print('%s获得的列表为空' % source.source_id)
                return

            # 处理内容
            for i in lst:
                i.source_id = source.source_id

                if not i.title:
                    i.title = '<title>'

                if not i.author:
                    i.author = source.name

                i.fetch_date = int_time

                if not i.suid:
                    print(i.source_id, '出现suid为空')

                # length
                if len(i.title) > runcfg.title_len:
                    i.title = i.title[:runcfg.title_len-3] + '...'
                    
                if len(i.summary) > runcfg.summary_len:
                    i.summary = i.summary[:runcfg.summary_len-3] + '...'

                if len(i.author) > runcfg.author_len:
                    i.author = i.author[:runcfg.author_len-3] + '...' 

                if len(i.pub_date) > runcfg.pub_date_len:
                    i.pub_date = i.pub_date[:runcfg.pub_date_len-3] + '...'
                    
                # for html show
                i.summary = for_wz(i.summary)
                i.pub_date = for_wz(i.pub_date)

            data = [source.source_id, int_time, lst]
            if is_exception:
                c_message.make(back_web_queue,
                               'bw:exception_info',
                               cfg_token,
                               data)
            else:
                c_message.make(back_web_queue,
                               'bw:success_infos',
                               cfg_token,
                               data)

        #print('线程结束：%s' % source.source_id)        

    source = bvars.sources[source_id]
    
    try:
        worker_tuple = bvars.workers[source.worker_id]
    except:
        worker = None
        worker_dict = None
    else:
        worker = worker_tuple[0]
        worker_dict = worker_tuple[1]

    t = threading.Thread(target=worker_wrapper, 
                         args=(runcfg,
                               worker, 
                               source, worker_dict,
                               bvars.back_web_queue, bvars.bb_queue,
                               bvars.cfg_token), 
                         daemon=True
                         )
    t.start()


# for test source
def test_source(source_id):
    source = bvars.sources[source_id]

    try:
        worker_tuple = bvars.workers[source.worker_id]
    except:
        print('信息源%s没有找到指定worker: %s' % 
              (source.source_id, source.worker_id)
              )
        return

    worker = worker_tuple[0]
    worker_dict = worker_tuple[1]

    int_time = int(time.time())

    # run
    try:
        if source.data == None:
            raise Exception('信息源%s的data未能被解析' % source.source_id)
        
        lst = worker(source.data, worker_dict)

    except Exception as e:
        s = '\n源%s出现异常:\n' % source.source_id
        print(s + str(e))

    else:
        # callback函数
        if source.callback != None:
            newlst = list()
            local_d = dict()
            local_d['hasher'] = hasher
            local_d['unixtime'] = unixtime

            for i, info in enumerate(lst):
                local_d['posi'] = i
                local_d['info'] = info
                try:
                    exec(source.callback, None, local_d)
                except Exception as e:
                    print('callback异常:', e)
                    info.title = 'callback代码异常'
                    info.summary = str(e)
                
                if info.temp != 'del':
                    newlst.append(info)

            lst = newlst

        for i in lst:
            i.source_id = source.source_id
            if not i.author:
                i.author = source.name
            i.fetch_date = int_time

            if not i.suid:
                print(i.source_id, '出现suid为空')

            if len(i.title) > 70:
                i.title = i.title[:67] + '...'
                
            if len(i.summary) > 160:
                i.summary = i.summary[:157] + '...'

            if len(i.author) > 50:
                i.author = i.author[:47] + '...' 

            if len(i.pub_date) > 50:
                i.pub_date = i.pub_date[:47] + '...'    

        print('\n---------- 以下为测试结果 ----------')
        print(' 信息源id(source_id)为 %s' % source.source_id)
        print(' 获取了%d条信息\n' % len(lst))

        if len(lst) > 16:
            print_str = ''.join(str(i) for i in lst[:8]) + \
                        '...中间省略%d条...\n\n' % (len(lst)-16) + \
                        ''.join(str(i) for i in lst[-8:])
        else:
            print_str = ''.join(str(i) for i in lst)

     
        try:
            print(print_str)
        except UnicodeEncodeError:
            temp = str(print_str)
            for t in temp:
                try:
                    print(t, end='')
                except:
                    print('?', end='')
            print()


# parse source data, return a dict
def parse_data(worker_id, xml_string):
    try:
        parser = bvars.dataparsers[worker_id]
    except:
        # can't find the data-parser, maybe the worker doesn't need data.
        # if worker_id doesn't exist, worker_starter will catch the issue.
        return dict()
    
    try:
        return parser(xml_string)
    except:
        # can't parse the data, return None
        return None

# worker function:
# params: (data_dict, worker_dict) 
# return: list(info) or c_worker_exception

# worker decorator
def worker(worker_id):
    def worker_decorator(func):
        if worker_id not in bvars.workers:
            bvars.workers[worker_id] = (func, dict())
        else:
            print('worker_id: %s already exist in workers' % worker_id)
        return func
    
    return worker_decorator

# dataparser function:
# params: (xml_string)
# return: data_dict

# data-parser decorator
def dataparser(worker_id):
    def dataparser_decorator(func):
        if worker_id not in bvars.dataparsers:
            bvars.dataparsers[worker_id] = func
        else:
            print('worker_id: %s already exist in dataparsers' % worker_id)
        return func
    
    return dataparser_decorator
