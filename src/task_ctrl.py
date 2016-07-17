# coding=utf-8

import heapq
import collections
import time
import datetime

from datadefine import c_message
import bvars
import worker_manage

class c_run_heap_unit:
    __slots__ = ('source_id', 'interval', 'next_time', 
                 'xml', 'last_fetch', 'temp_next_time')

    def __init__(self, source_id, interval, next_time, xml):
        self.source_id = source_id
        self.interval = interval
        self.next_time = next_time
        
        self.xml = xml
        self.last_fetch = 0
        self.temp_next_time = 0

    def __lt__(self, other):
        if self.next_time < other.next_time:
            return True
        return False

class c_running_unit:
    __slots__ = ('source_id', 'timeout_time')

    def __init__(self, source_id, timeout_time):
        self.source_id = source_id
        self.timeout_time = timeout_time
        
# database process timer
# return (next_time, interval)
def get_db_process_time(gcfg):
    now_int = int(time.time())
    
    # db_process_interval enabled
    if gcfg.db_process_interval > 0:
        interval = gcfg.db_process_interval*3600
        return now_int + 900 + interval, interval
    
    nowdt = datetime.datetime.now()

    one_day = datetime.timedelta(days=1)
    nextdt = datetime.datetime(nowdt.year, nowdt.month, nowdt.day,
                               gcfg.db_process_at[0], 
                               gcfg.db_process_at[1]) + one_day

    dtime = int((nextdt - nowdt).total_seconds())

    dvalue = dtime if dtime < 24*3600 else dtime - 24*3600
    print('database process after %d seconds' % dvalue)

    return now_int + dvalue, 24*3600

class c_task_controller:
    def __init__(self, back_web_queue):
        self.back_web_queue = back_web_queue
        
        self.gcfg = None
        self.timer_heap = None

        self.temp_fetch_list = list()

        # 运行中--------------------
        # key:source_id, value:start time
        self.running_map = dict()
        # sorted list: (source_id, next_time)
        self.running_sorted_list = list()

        # 排队----------------------
        # set: source_id
        self.queue_set = set()
        # deque: source_id
        self.queue_deque = collections.deque()

    def set_data(self, gcfg, timer_heap):
        self.gcfg = gcfg
        self.timer_heap = timer_heap
        
        self.sid_unit_dic = {u.source_id:u for u in timer_heap}

        # database process timer
        if self.timer_heap != None:
            dbnext, dbinterval = get_db_process_time(gcfg)
            db_unit = c_run_heap_unit('db_process',
                                      dbinterval,
                                      dbnext,
                                      '')
            heapq.heappush(self.timer_heap, db_unit)

        # clear
        self.temp_fetch_list.clear()

        self.running_map.clear()
        self.running_sorted_list.clear()

        self.queue_set.clear()
        self.queue_deque.clear()

    def task_finished(self, source_id, fetch_time):
        # remove from running
        if source_id in self.running_map:
            del self.running_map[source_id]

        for i, unit in enumerate(self.running_sorted_list):
            if unit.source_id == source_id:                
                # remove from running list
                del self.running_sorted_list[i]
                break

        self.fresh_job()

    def fresh_job(self):
        now_time = int(time.time())

        while self.queue_set and \
              len(self.running_map) < self.gcfg.task_pipes:

            # remove from queue
            source_id = self.queue_deque.popleft()
            self.queue_set.remove(source_id)

            # add to running
            self.running_map[source_id] = now_time

            item = c_running_unit(source_id, 
                                  now_time+self.gcfg.task_timeout
                                  )
            self.running_sorted_list.append(item)

            # start thread
            worker_manage.worker_starter(self.gcfg.runcfg, source_id)

    def fetch(self, lst):
        now_time = int(time.time())
        
        for source_id in lst:
            if source_id in self.running_map:
                sec_ago = now_time - self.running_map[source_id]
                print('%s已经于%d秒前运行，尚未结束' % \
                      (source_id, sec_ago)
                      )
                continue
            elif source_id in self.queue_set:
                print('%s已经在队列中，请等待' % source_id)
                continue

            # run if has slots
            if len(self.running_map) < self.gcfg.task_pipes:
                self.running_map[source_id] = now_time

                item = c_running_unit(source_id, 
                                      now_time+self.gcfg.task_timeout
                                      )
                self.running_sorted_list.append(item)

                worker_manage.worker_starter(self.gcfg.runcfg, source_id)
            # no slots, add to deque
            else:
                self.queue_set.add(source_id)
                self.queue_deque.append(source_id)          

    def timer(self):
        if not self.timer_heap:
            return
        
        now_time = int(time.time())

        # timer of source
        while now_time >= self.timer_heap[0].next_time:
            # timer heap
            temp = heapq.heappop(self.timer_heap)
            temp.temp_next_time = temp.next_time
            temp.next_time += temp.interval

            # fix wrong system start-up time
            if temp.next_time <= now_time:
                if temp.source_id == 'db_process':
                    dbnext, dbinterval = get_db_process_time(self.gcfg)
                    temp.next_time = dbnext
                else:
                    temp.next_time = bvars.boot_time + \
                      ((now_time-bvars.boot_time)//interval+1) * interval

            heapq.heappush(self.timer_heap, temp)

            if temp.source_id == 'db_process':
                # time to maintenance database
                c_message.make(self.back_web_queue, 
                               'bw:db_process_time',
                               bvars.cfg_token)
            else:
                # temp list
                self.temp_fetch_list.append(temp.source_id)

        # 运行source
        if self.temp_fetch_list:
            self.fetch(self.temp_fetch_list)
            self.temp_fetch_list.clear()

        # timer of running timeout
        mark = False
        while self.running_sorted_list and \
              now_time > self.running_sorted_list[0].timeout_time:
                temp_source_id = self.running_sorted_list[0].source_id
                temp_start_time = self.running_map[temp_source_id]
                del self.running_sorted_list[0]
                del self.running_map[temp_source_id]

                # (source_id, start_time, time_out)
                temp_tuple = (temp_source_id, 
                              temp_start_time,
                              self.gcfg.task_timeout
                              )
                c_message.make(self.back_web_queue, 
                               'bw:source_timeout',
                               bvars.cfg_token,
                               [temp_tuple]
                               )

                mark = True
                print('任务%s超时' % temp_source_id)
        if mark:
            self.fresh_job()
            
    def web_updated(self, sid, fetch_time):
        self.sid_unit_dic[sid].temp_next_time = 0
        self.sid_unit_dic[sid].last_fetch = fetch_time
    
    # remember nexttime of running source
    def remember_nexttime_dict(self):
        d = dict()
        
        # in heap
        if self.timer_heap != None:
            for unit in self.timer_heap:
                d[unit.source_id] = unit
        
        return d

    def get_status_str(self):
        s = ('timer heap length: %d<br>'
             'running source number: %d<br>'
             'queue length: %d<br>')
        s = s % (len(self.timer_heap) if self.timer_heap != None 
                                      else -1, 
                 len(self.running_map), 
                 len(self.queue_deque)
                 )

        return s
