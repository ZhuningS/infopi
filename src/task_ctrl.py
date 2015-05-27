# coding=utf-8

import heapq
import collections
import time

import worker_manage

class c_run_heap_unit:
    __slots__ = ('source_id', 'interval', 'next_time')

    def __init__(self, source_id, interval, next_time):
        self.source_id = source_id
        self.interval = interval
        self.next_time = next_time

    def __lt__(self, other):
        if self.next_time < other.next_time:
            return True
        return False

class c_running_unit:
    __slots__ = ('source_id', 'timeout_time')

    def __init__(self, source_id, timeout_time):
        self.source_id = source_id
        self.timeout_time = timeout_time

    def __lt__(self, other):
        if self.timeout_time < other.timeout_time:
            return True
        return False

class c_task_controller:
    def __init__(self):
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

        # clear
        self.temp_fetch_list.clear()

        self.running_map.clear()
        self.running_sorted_list.clear()

        self.queue_set.clear()
        self.queue_deque.clear()

    def task_finished(self, source_id):
        # remove from running
        if source_id in self.running_map:
            del self.running_map[source_id]

        for i, unit in enumerate(self.running_sorted_list):
            if unit.source_id == source_id:
                del self.running_sorted_list[i]
                break

        self.fresh_job()   

    def fresh_job(self):
        now_time = int(time.time())

        while True:
            if self.queue_set and \
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
                self.running_sorted_list.sort()

                # start thread
                worker_manage.worker_starter(self.gcfg.runcfg, source_id)
            else:
                break

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
                self.running_sorted_list.sort()

                worker_manage.worker_starter(self.gcfg.runcfg, source_id)
            # no slots, add to deque
            else:
                self.queue_set.add(source_id)
                self.queue_deque.append(source_id)          

    def timer(self, now_time):
        if not self.timer_heap:
            return

        # 检查到时的source
        self.temp_fetch_list.clear()

        # timer of source
        while self.timer_heap and now_time >= self.timer_heap[0].next_time:
            # timer heap
            temp = heapq.heappop(self.timer_heap)
            temp.next_time += temp.interval

            # for wrong start-up time
            if temp.next_time <= now_time:
                temp.next_time = now_time + temp.interval

            heapq.heappush(self.timer_heap, temp)

            # temp list
            self.temp_fetch_list.append(temp.source_id)

        # 运行source
        if self.temp_fetch_list:
            self.fetch(self.temp_fetch_list)

        # timer of running timeout
        mark = False
        while self.running_sorted_list and \
              now_time > self.running_sorted_list[0].timeout_time:
                temp_source_id = self.running_sorted_list[0].source_id
                del self.running_map[temp_source_id]
                del self.running_sorted_list[0]

                mark = True
                print('任务%s超时' % temp_source_id)
        if mark:
            self.fresh_job()

        # print('running: %d, queue: %d' % \
        #       (len(running_map), len(queue_set))
        #       )

    def get_status_str(self):
        s = ('timer heap length: %d<br>'
             'running source number: %d<br>'
             'queue length: %d<br>')
        s = s % (len(self.timer_heap), 
                 len(self.running_map), 
                 len(self.queue_deque)
                 )

        return s
