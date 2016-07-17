# coding=utf-8

import time
import threading
import queue  
import heapq
import os

import bvars

__all__ = ['main_process']

gcfg = None

# for import
m_datadefine = None
m_task_ctrl = None
m_gconfig = None
m_source_manage = None
m_user_manage = None
c_message = None
c_red = None
c_fetcher = None

# 1, append (name, comment, link) to source_info of user_table
# 2, make timer_heap
# 3, print unable_source and unused_source
def pre_process(users, all_source_dict, 
                remember_time_dict):
    run_source_dict = dict()
    unable_source_list = list()

    sid_sinfolist_dict = dict()
    now_time = int(time.time())
    
    for user in users:
        for category, sinfo_list in user.category_list:
            for sinfo in sinfo_list:
                sid = sinfo[0]

                if sid in all_source_dict:
                    tname = all_source_dict[sid].name
                    tcomment = all_source_dict[sid].comment
                    tlink = all_source_dict[sid].link
                    xml = all_source_dict[sid].xml

                    # sinfo的内容为
                    # [sid, level, interval, name, comment, link]
                    sinfo[3] = tname
                    sinfo[4] = tcomment
                    sinfo[5] = tlink

                    # for timer_heap
                    interval = gcfg.default_source_interval \
                               if sinfo[2] == 0 \
                               else 3600*sinfo[2]
                    interval = max(60, int(round(interval)))

                    if sid not in run_source_dict:
                        # souce_id, interval, next_time
                        unit = m_task_ctrl.c_run_heap_unit(sid, 
                                                           interval, 
                                                           now_time,
                                                           xml)
                        run_source_dict[sid] = unit
                    else:
                        unit = run_source_dict[sid]
                        unit.interval = min(interval, unit.interval)

                    # for show interval
                    if sid not in sid_sinfolist_dict:
                        sid_sinfolist_dict[sid] = list()
                    sid_sinfolist_dict[sid].append(sinfo)
                else:
                    # (name, comment, link) of source info
                    sinfo[3] = '<未加载>'
                    sinfo[4] = '无法找到或无法加载%s的xml文件' % sid
                    sinfo[5] = ''

                    s = ('用户:%s 版块:%s\n'
                         'source_id为%s的信息源定义不存在\n'
                         )
                    print(s % (user.username, category, sid))

                    unable_source_list.append( (user, category, sid) )
                    
    # for next_time
    boot_time = bvars.boot_time

    # make running heap
    timer_heap = list()
    
    for sid, unit in run_source_dict.items():
        # next time
        if sid in remember_time_dict and \
           remember_time_dict[sid].xml == unit.xml:
            if remember_time_dict[sid].temp_next_time != 0:
                next_time = remember_time_dict[sid].temp_next_time
            else:
                next_time = remember_time_dict[sid].next_time
            last_fetch_time = remember_time_dict[sid].last_fetch
        else:
            next_time = boot_time + \
              ((now_time-boot_time) // interval) * interval
            last_fetch_time = 0
        
        # update unit
        unit.next_time = next_time
        unit.last_fetch = last_fetch_time
        
        # push heap
        heapq.heappush(timer_heap, unit)

        # for show
        for sinfo in sid_sinfolist_dict[sid]:
            sinfo[2] = unit.interval
            sinfo[6] = unit.last_fetch

#     # print unused sources
#     t_all_source = set(all_source_dict.keys())
#     t_run_source = set(run_source_dict.keys())
#     t_unuse_source = t_all_source.difference(t_run_source)
#     for sid in t_unuse_source:
#         tname = all_source_dict[sid].name
#         tcomment = all_source_dict[sid].comment
#         tlink = all_source_dict[sid].link
#         s = ('未使用的源%s\nname:%s\ncomment:%s\nlink:%s\n')
#         print(s % (sid, tname, tcomment, tlink))

    return timer_heap, users

def import_files():
    import workers

    import datadefine

    global m_datadefine
    m_datadefine = datadefine
    global c_message
    c_message = datadefine.c_message

    import task_ctrl
    global m_task_ctrl
    m_task_ctrl = task_ctrl

    import gconfig
    global m_gconfig
    m_gconfig = gconfig

    import source_manage
    global m_source_manage
    m_source_manage = source_manage

    import user_manage
    global m_user_manage
    m_user_manage = user_manage

    import red
    global c_red
    c_red = red.red

    import fetcher
    global c_fetcher
    c_fetcher = fetcher.Fetcher

def main_process(version, web_port, https, tmpfs_path,
                 web_back_queue, back_web_queue):

    def load_config_sources_users(web_port, https, tmpfs_path, 
                                  remember_time_dict):
        # check cfg directory exist?
        config_path = os.path.join(bvars.root_path, 'cfg')
        if not os.path.isdir(config_path):
            print('不存在cfg文件夹，无法加载配置。')
            print('请在准备好cfg配置文件夹后重新启动程序。')
            return None, None, None
        
        # clrear red & fetcher cache
        c_red.clear_cache()
        c_fetcher.clear_cache()

        # load config
        cfg = m_gconfig.load_config(version, web_port, https, tmpfs_path)
        global gcfg
        gcfg = cfg

        # load sources
        m_source_manage.load_sources()

        # load users
        user_list = m_user_manage.c_user_cfg.load_users(cfg)
        print('back-side loaded %d users' % len(user_list))

        # pre process
        timer_heap, user_list = pre_process(user_list, bvars.sources, 
                                            remember_time_dict)
        
        # config token
        cfg_token = int(time.time())
        bvars.cfg_token = cfg_token
        
        # tasks_suspend， 挂起时timer_heap返回None
        if gcfg.tasks_suspend:
            return cfg_token, None, user_list

        return cfg_token, timer_heap, user_list

    # -----------------------
    #         start 
    # -----------------------

    # back-process global queues
    bb_queue = queue.Queue()

    bvars.bb_queue = bb_queue
    bvars.back_web_queue = back_web_queue

    # import
    import_files()

    # task controller
    ctrl = m_task_ctrl.c_task_controller(back_web_queue)

    # http-request for notifying web-process
    request_web_check = fun_request_web_check(web_port, https)

    # -----------------------
    # threads
    # -----------------------
    
    def web_back_queue_monitor(web_back_queue, bb_queue):
        while True:
            msg = web_back_queue.get()
            bb_queue.put(msg)

    def timer_thread(bb_queue):
        timer_msg = c_message('bb:timer')
        while True:
            time.sleep(3)
            bb_queue.put(timer_msg)

    # web_back_queue 监视线程
    threading.Thread(target=web_back_queue_monitor,
                     args=(web_back_queue, bb_queue),
                     daemon=True
                     ).start()

    # timer 线程
    threading.Thread(target=timer_thread,
                     args=(bb_queue,),
                     daemon=True
                     ).start()

    # -----------------------
    # main loop
    # -----------------------
    print('back-side process loop starts')

    # used for fetch all sources
    fetch_all = list()

    while True:
        msg = bb_queue.get()

        # timer
        if msg.command == 'bb:timer':
            ctrl.timer()
            #status_str = ctrl.get_status_str()
            #print(status_str)

            # 检查发送队列
            if not back_web_queue.empty():
                try:
                    request_web_check()
                except:
                    pass

        # source执行完毕
        elif msg.command == 'bb:source_return' and \
             msg.token == bvars.cfg_token:
            # msg.data is [source_id, fetch_time]
            ctrl.task_finished(msg.data[0], msg.data[1])
            
        # web端成功添加
        elif msg.command == 'wb:source_updated' and \
             msg.token == bvars.cfg_token:
            ctrl.web_updated(msg.data[0], msg.data[1])

        # 运行sources
        elif msg.command == 'wb:request_fetch':
            #print('web side request fetch')
            
            # 挂起 或 无信息源
            if not fetch_all:
                continue

            # 运行source
            ctrl.fetch(fetch_all if msg.data == None else msg.data)

        # load config, users
        elif msg.command == 'wb:request_load':
            # remember next_time
            remember_time_dict = ctrl.remember_nexttime_dict()
            
            cfg_token, timer_heap, user_list = \
                load_config_sources_users(web_port, https, tmpfs_path,
                                          remember_time_dict)
            
            # clear remember
            remember_time_dict.clear()

            # 加载cfg文件夹失败
            if cfg_token == None:
                continue
            
            # 挂起?
            if timer_heap == None:
                fetch_all = list()
            else:
                fetch_all = [i.source_id for i in timer_heap]

            ctrl.set_data(gcfg, timer_heap)

            # send [cfg_token, config, users] to web
            c_message.make(back_web_queue, 
                           'bw:send_config_users',
                           cfg_token,
                           [cfg_token, gcfg, user_list])
            
        else:
            print('back can not handle:', msg.command, msg.token)


def fun_request_web_check(port, https):
    import urllib.request
    proxy = urllib.request.ProxyHandler({})

    if not https:
        opener = urllib.request.build_opener(proxy)
        req = urllib.request.Request('http://127.0.0.1:%d/check' % port)
    else:
        import ssl
        https_handler = urllib.request.HTTPSHandler(
                context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
                )
        opener = urllib.request.build_opener(proxy, https_handler)
        req = urllib.request.Request('https://127.0.0.1:%d/check' % port)

    def openit():
        opener.open(req)

    return openit