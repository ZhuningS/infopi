# coding=utf-8

import os
import subprocess

import wvars

__all__ = ('get_info_list', 'get_python_process')

def get_info_list(cfg, usertype, db_file='', db_size=''):
    lst = list()

    # cpu temperature
    temp_float = get_cpu_temperature()
    if temp_float != None:
        one = ('系统-CPU温度', str(temp_float))
        lst.append(one)

    # memory
    try:
        lst.extend(get_meminfo())
    except:
        pass

    # version
    one = ('程序版本', cfg.version)
    lst.append(one)

    if usertype > 0:
        # programe start time
        one = ('程序启动时间', cfg.boot_time)
        lst.append(one)

        # config start time
        one = ('配置加载时间', cfg.start_time)
        lst.append(one)

    # web port
    one = ('web端口', cfg.web_port)
    lst.append(one)

    # tmpfs
    one = ('tmpfs目录', cfg.tmpfs_path or '无')
    lst.append(one)

    # database
    one = ('数据库文件', db_file)
    lst.append(one)

    one = ('数据库文件大小', db_size)
    lst.append(one)

    # weberr.txt
    fpath = os.path.join(wvars.upload_forlder, 'weberr.txt')
    try:
        size = os.path.getsize(fpath)
    except:
        size = '目前不存在此文件'

    one = ('web进程异常记录文件大小', size)
    lst.append(one)

    return lst

def get_meminfo():
    with open('/proc/meminfo') as f:
        lines = f.readlines()

    lst = list()

    name, value, other = lines[0].split()
    one = ('系统-总内存(MemTotal)', '%.1f MB' % (int(value)/1024))
    lst.append(one)

    name, value, other = lines[1].split()
    one = ('系统-剩余内存(MemFree)', '%.1f MB' % (int(value)/1024))
    lst.append(one)

    name, value, other = lines[2].split()
    one = ('系统-缓冲区内存(Buffers)', '%.1f MB' % (int(value)/1024))
    lst.append(one)

    name, value, other = lines[3].split()
    one = ('系统-缓存内存(Cached)', '%.1f MB' % (int(value)/1024))
    lst.append(one)

    return lst

def get_cpu_temperature():
    rpi = '/sys/class/thermal/thermal_zone0/temp'
    cb = '/sys/devices/platform/sunxi-i2c.0/i2c-0/0-0034/temp1_input'

    filelist = (rpi, cb)

    for filename in filelist:
        try:
            f = open(filename)
            data = f.readline().strip()
            temp = float(data)/1000
        except:
            pass
        else:
            return temp

    return None

def get_python_process(cfg):
    lst = list()

    try:
        ps = subprocess.Popen(('ps', 'auxw'), stdout=subprocess.PIPE)
        output = ps.communicate()[0].decode('utf-8')
    except:
        return []
    else:
        wpid_str = str(cfg.web_pid)
        bpid_str = str(cfg.back_pid)

        for line in output.split('\n'):
            try:
                items = line.split()

                if items[1] in (wpid_str, bpid_str):
                    lst.append(line)
            except:
                pass

    ret = list()

    for line in lst:
        items = line.split()
        one = list()
        
        # process
        if int(items[1]) == cfg.web_pid:
            one.append('web进程')
        else:
            one.append('后端进程')
            
        # pid
        one.append(items[1])
        # user id
        one.append(items[0])
        # status
        one.append(items[7])
        # cpu usage
        one.append(items[2]+'%')
        # cpu time
        one.append(items[9])
        # mem usage
        one.append(items[3]+'%')
        # phy mem
        one.append(items[5]+' KB')
        # virtual mem
        one.append(items[4]+' KB')

        ret.append(one)

    return ret

