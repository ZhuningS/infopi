# coding=utf-8

import datetime
import time
import os
import queue
import html
from zipfile import ZipFile, is_zipfile
import shutil
import base64
from enum import IntEnum

try:
    import winsound
except:
    has_winsound = False
else:
    has_winsound = True

# ---------------------

from flask import (Flask, render_template, request,
                  make_response, redirect, 
                  send_from_directory)

from werkzeug import secure_filename

# ---------------------

import wvars
from db_wrapper import *
from datadefine import *
from rpi_stat import *

web = Flask(__name__, 
            static_folder=wvars.static_folder, 
            template_folder=wvars.template_folder)

web_back_queue = None
back_web_queue = None

gcfg = None
db = None

template_cache = dict()
login_manager = c_login_manager()

class PG_TYPE(IntEnum):
    GATHER = 0
    CATEGORY = 1
    SOURCE = 2
    M_GATHER = 3
    M_CATEGORY = 4

wrong_key_html = ('在当前的用户配置中，没有找到相应版块。<br>'
                  '请刷新整个页面，以更新左侧的版块目录。')

zero_user_loaded = ('尚未载入任何用户，请在3秒后刷新此页面。<br>'
                   '如问题依旧，请检查用户配置、后端进程的状态。'
                   )

#-------------------------------
#         page part
#-------------------------------

# page nag part
def generate_page(all_count, now_pg, 
                  col_per_page, 
                  p_type, category):

    def make_pattern(p_type, category):
        if p_type == PG_TYPE.GATHER:
            template_tuple = ('<a href="/list', str(category), 
                              '/%d" target="_self">%s</a>')  
        elif p_type == PG_TYPE.CATEGORY:
            template_tuple = ('<a href="/list/', category,
                              '/%d" target="_self">%s</a>')
        elif p_type == PG_TYPE.SOURCE:
            template_tuple = ('<a href="/slist/', category,
                              '/%d" target="_self">%s</a>') 
        elif p_type == PG_TYPE.M_GATHER:
            template_tuple = ('<a href="/ml', str(category), 
                              '/%d" target="_self">%s</a>')   
        elif p_type == PG_TYPE.M_CATEGORY:
            template_tuple = ('<a href="/ml/', category,
                              '/%d" target="_self">%s</a>')
        return ''.join(template_tuple)   


    last_pg = (all_count // col_per_page) + \
              (1 if (all_count % col_per_page) else 0)

    if now_pg < 1:
        now_pg = 1
    elif now_pg > last_pg:
        now_pg = last_pg

    # numbers width
    if p_type in (PG_TYPE.GATHER, PG_TYPE.CATEGORY, PG_TYPE.SOURCE):
        sides = 5
    else:
        sides = 3
    begin_pg = now_pg - sides
    end_pg = now_pg + sides

    if begin_pg < 1:
        end_pg += 1 - begin_pg

    if end_pg > last_pg:
        begin_pg -= end_pg - last_pg
        end_pg = last_pg

    if begin_pg < 1:
        begin_pg = 1

    # format template
    template = template_cache.get((p_type, category))
    if template == None:
        template = make_pattern(p_type, category)
        template_cache[(p_type, category)] = template

    # pc
    if p_type in (PG_TYPE.GATHER, PG_TYPE.CATEGORY, PG_TYPE.SOURCE):
        lst = list()

        lst.append('共%d页' % last_pg)

        # 首页
        if now_pg > 1:
            s = template % (1, '首页')
            lst.append(s)
        else:
            lst.append('已到')

        # 末页
        if now_pg < last_pg:
            s = template % (last_pg, '末页')
            lst.append(s)
        else:
            lst.append('已到')

        # numbers
        for i in range(begin_pg, end_pg+1):
            if i == now_pg:
                ts = '<strong>%d</strong>' % i
            else:
                ts = template % (i, str(i))
            lst.append(ts)

        # 上页
        if now_pg > 1:
            s = template % (now_pg-1, '上页')
            lst.append(s)
        else:
            lst.append('已到')

        # 下页
        if now_pg < last_pg:
            s = template % (now_pg+1, '下页')
            lst.append(s)  
        else:
            lst.append('已到')

        return '&nbsp;'.join(lst)

    # mobile
    else:
        # nag
        lst1 = list()
        # 首页
        if now_pg > 1:
            s = template % (1, '首页')
            lst1.append(s)
        else:
            lst1.append('首页')

        # 末页
        if now_pg < last_pg:
            s = template % (last_pg, '末页&nbsp;&nbsp;&nbsp;')
            lst1.append(s)
        else:
            lst1.append('末页&nbsp;&nbsp;&nbsp;')

        # 上页
        if now_pg > 1:
            s = template % (now_pg-1, '上页')
            lst1.append(s)
        else:
            lst1.append('上页')

        # 下页
        if now_pg < last_pg:
            s = template % (now_pg+1, '下页')
            lst1.append(s)  
        else:
            lst1.append('下页')

        # numbers
        lst2 = list()
        lst2.append('共%d页' % last_pg)
        for i in range(begin_pg, end_pg+1):
            if i == now_pg:
                ts = '<strong>%d</strong>' % i
            else:
                ts = template % (i, str(i))
            lst2.append(ts)

        return '&nbsp;&nbsp;'.join(lst2) + \
               '<br>' + \
               '&nbsp;&nbsp;'.join(lst1)


#-------------------------------
#           generate_list
#-------------------------------
# generate list
def generate_list(username, category, pagenum, p_type, sid=''):
    if pagenum < 1:
        pagenum = 1

    # limit and offset
    if p_type in (PG_TYPE.M_GATHER, PG_TYPE.M_CATEGORY):
        limit = 10
    else:
        limit = db.get_colperpage_by_user(username)
    offset = limit * (pagenum-1)

    # content list
    if p_type == PG_TYPE.SOURCE:
        all_count = db.get_count_by_sid(sid)
        if all_count == -1:
            return None, None, None, None, None
        
        lst = db.get_infos_by_sid(username, sid, offset, limit)
        if lst == None:
            return None, None, None, None, None
    else:
        all_count = db.get_count_by_user_cate(username, category)
        if all_count == -1:
            return None, None, None, None, None
        
        lst = db.get_infos_by_user_category(username, category, 
                                            offset, limit)

    # nag part
    page_html = generate_page(all_count, pagenum,
                              limit, 
                              p_type, category)

    # current time
    int_now_time = int(time.time())

    # 时:分:秒
    now_time = datetime.datetime.\
               fromtimestamp(int_now_time).\
               strftime('%H:%M:%S')

    recent_8h = int_now_time - 3600*8
    recent_24h = int_now_time - 3600*24

    for i in lst:
        if i.fetch_date > recent_8h:
            i.temp = 1
        elif i.fetch_date > recent_24h:
            i.temp = 2
        
        # 月-日 时:分
        i.fetch_date = datetime.datetime.\
                       fromtimestamp(i.fetch_date).\
                       strftime('%m-%d %H:%M')

    if p_type in (PG_TYPE.GATHER, PG_TYPE.M_GATHER):
        if category == 0:
            category = '普通、关注、重要'
        elif category == 1:
            category = '关注、重要'
        elif category == 2:
            category = '重要'
    elif p_type == PG_TYPE.SOURCE:
        category = db.get_name_by_sid(sid)
        
    return lst, all_count, page_html, now_time, category

# return username or None
def check_cookie():
    ha = request.cookies.get('user')
    if ha:
        # return username or None
        return db.get_user_from_hash(ha)
    else:
        return None

@web.route('/')
def index():
    if check_cookie():
        return render_template('main.html')
    else:
        print('to login')
        return redirect('/login')

@web.route('/login', methods=['GET', 'POST'])
def login():
    # check hacker
    ip = request.remote_addr
    allow, message = login_manager.login_check(ip)
    if not allow:
        return message

    # load 0 user
    if db.get_user_number() == 0:
        return render_template('login.html', msg=zero_user_loaded)

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        ha = db.login(username, password)
        if ha:
            response = make_response(redirect('/'))
            # 失效期2038年
            response.set_cookie('user', 
                                value=ha, 
                                expires=2147483640)
            return response
        else:
            login_manager.login_fall(ip)
            return render_template('login.html',
                                    msg='无此用户或密码错误')

    return render_template('login.html')

@web.route('/mlogin', methods=['GET', 'POST'])
def mlogin():
    # check hacker
    ip = request.remote_addr
    allow, message = login_manager.login_check(ip)
    if not allow:
        return message

    # load 0 user
    if db.get_user_number() == 0:
        return render_template('login.html', msg=zero_user_loaded)

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        ha = db.login(username, password)
        if ha:
            response = make_response(redirect('/m'))
            # 失效期2038年
            response.set_cookie('user', 
                                value=ha, 
                                expires=2147483640)
            return response
        else:
            login_manager.login_fall(ip)
            return render_template('login.html',
                                    m='m',
                                    msg='无此用户或密码错误')

    return render_template('login.html', m='m')

@web.route('/left', methods=['GET', 'POST'])
def left():
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'
    
    # user type
    usertype = db.get_usertype(username)

    allow = True if usertype > 0 else False
    if usertype == 0:
        type_str = '公共帐号'
    elif usertype == 1:
        type_str = '普通帐号'
    elif usertype == 2:
        type_str = '管理员'

    if request.method == 'POST':
        name = request.form['name']

        # logout
        if name == 'logout':
            html = r'<script>top.location.href="/";</script>'
            response = make_response(html)
            response.set_cookie('user', expires=0)
            return response

        # fetch my sources
        elif usertype > 0 and name == 'fetch_mine':
            lst = db.get_fetch_list_by_user(username)
            c_message.make(web_back_queue, 'wb:request_fetch', lst)


    category_list = db.get_category_list_by_username(username)
    return render_template('left.html', 
                           usertype=type_str,
                           username=username,
                           allowfetch=allow,
                           categories=category_list)

@web.route('/m', methods=['GET', 'POST'])
def mobile():
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/mlogin";</script>'
    
    # user type
    usertype = db.get_usertype(username)
    allow = True if usertype > 0 else False

    if request.method == 'POST':
        name = request.form['name']

        # logout
        if name == 'logout':
            html = r'<script>top.location.href="/m";</script>'
            response = make_response(html)
            response.set_cookie('user', expires=0)
            return response

        # fetch my sources
        elif usertype > 0 and name == 'fetch_mine':
            lst = db.get_fetch_list_by_user(username)
            c_message.make(web_back_queue, 'wb:request_fetch', lst)


    category_list = db.get_category_list_by_username(username)
    return render_template('m.html', 
                           username=username,
                           allowfetch=allow,
                           categories=category_list)

@web.route('/ml/<category>')
@web.route('/ml/<category>/<int:pagenum>')
def mobile_list(category, pagenum=1):
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/m";</script>'

    t1 = time.perf_counter()

    lst, all_count, page_html, now_time, category = \
            generate_list(username, category, 
                          pagenum, PG_TYPE.M_CATEGORY)
    
    if lst == None:
        return wrong_key_html

    t2 = time.perf_counter()
    during = '%.5f' % (t2-t1)

    return render_template('mlist.html', 
                           entries=lst, 
                           listname=category, 
                           htmlpage=page_html,
                           nowtime=now_time)

@web.route('/ml<int:level>')
@web.route('/ml<int:level>/<int:pagenum>')
def default_mobile(level, pagenum=1):
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/m";</script>'

    t1 = time.perf_counter()

    lst, all_count, page_html, now_time, category = \
            generate_list(username, level, 
                          pagenum, PG_TYPE.M_GATHER)
            
    if lst == None:
        return wrong_key_html

    t2 = time.perf_counter()
    during = '%.5f' % (t2-t1)

    return render_template('mlist.html', 
                           entries=lst, 
                           listname=category,
                           htmlpage=page_html,
                           nowtime=now_time)


@web.route('/list/<category>')
@web.route('/list/<category>/<int:pagenum>')
def right_list(category, pagenum=1):
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'

    t1 = time.perf_counter()

    lst, all_count, page_html, now_time, category = \
            generate_list(username, category, 
                          pagenum, PG_TYPE.CATEGORY)
            
    if lst == None:
        return wrong_key_html

    t2 = time.perf_counter()
    during = '%.5f' % (t2-t1)

    return render_template('list.html', 
                           entries=lst, 
                           listname=category,
                           count=all_count, htmlpage=page_html,
                           time=during, nowtime=now_time)

@web.route('/list<int:level>')
@web.route('/list<int:level>/<int:pagenum>')
def default_page(level, pagenum=1):
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'

    t1 = time.perf_counter()

    lst, all_count, page_html, now_time, category = \
            generate_list(username, level, 
                          pagenum, PG_TYPE.GATHER)
            
    if lst == None:
        return wrong_key_html

    t2 = time.perf_counter()
    during = '%.5f' % (t2-t1)

    return render_template('list.html', 
                           entries=lst, 
                           listname=category,
                           count=all_count, htmlpage=page_html,
                           time=during, nowtime=now_time)

@web.route('/slist/<encoded_url>')
@web.route('/slist/<encoded_url>/<int:pagenum>')
def slist(encoded_url='', pagenum = 1):
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'

    t1 = time.perf_counter()

    try:
        sid = base64.urlsafe_b64decode(encoded_url).decode('utf-8')
    except:
        return '请求的信息源列表url有误:<br>' + encoded_url

    lst, all_count, page_html, now_time, category = \
            generate_list(username, 
                          encoded_url, pagenum, 
                          PG_TYPE.SOURCE, sid
                          )
            
    if lst == None:
        return wrong_key_html
 
    t2 = time.perf_counter()
    during = '%.5f' % (t2-t1)

    return render_template('slist.html', 
                           listname=category,
                           entries=lst, 
                           count=all_count, htmlpage=page_html,
                           time=during, nowtime=now_time)

@web.route('/cateinfo')
def cate_info():
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'

    show_list = db.get_forshow_by_user(username)
    all_s_num, set_s_num = db.get_sourcenum_by_user(username)

    return render_template('cateinfo.html', show_list=show_list,
                            cate_num=len(show_list),
                            allnum=all_s_num, setnum=set_s_num)

def zip_cfg():
    # del .zip files in temp directory first
    files = os.listdir(wvars.upload_forlder)
    for f in files:
        fpath = os.path.join(wvars.upload_forlder, f)
        if not os.path.isdir(fpath) and f.endswith('.zip'):
            try:
                os.remove(fpath)
            except:
                pass

    # target file-name
    int_now_time = int(time.time())
    date_str = datetime.datetime.\
               fromtimestamp(int_now_time).\
               strftime('%y%m%d_%H%M')
    dst = 'cfg' + date_str
    dst = os.path.join(wvars.upload_forlder, dst)


    root_path = gcfg.root_path
    newfile = shutil.make_archive(dst, 'zip', root_path, 'cfg')

    return wvars.upload_forlder, os.path.split(newfile)[1]

def prepare_db_for_download():
    # del .db files in temp directory first
    files = os.listdir(wvars.upload_forlder)
    for f in files:
        fpath = os.path.join(wvars.upload_forlder, f)
        if not os.path.isdir(fpath) and f.endswith('.db'):
            try:
                os.remove(fpath)
            except:
                pass

    # current db
    db.compact_db()
    db_file, db_size = db.get_current_file()

    # target file-name
    int_now_time = int(time.time())
    date_str = datetime.datetime.\
               fromtimestamp(int_now_time).\
               strftime('%y%m%d_%H%M%S')
    dst = 'sql' + date_str + '.db'
    dst = os.path.join(wvars.upload_forlder, dst)

    # copy from database directory
    newfile = shutil.copy2(db_file, dst)

    return wvars.upload_forlder, os.path.split(newfile)[1]

@web.route('/panel', methods=['GET', 'POST'])
def panel():
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'

    usertype = db.get_usertype(username)
    
    if usertype == 2 and request.method == 'POST':
        if 'name' in request.form:
            name = request.form['name']

            # download cfg.zip
            if name == 'download_cfg':
                fpath, fname = zip_cfg()
                return send_from_directory(directory=fpath, 
                                           filename=fname,
                                           as_attachment=True)
            # 压缩数据库
            elif name == 'compact_db':
                print('try to compact database file')
                db.compact_db()

            # 下载数据库
            elif name == 'download_db':
                fpath, fname = prepare_db_for_download()
                return send_from_directory(directory=fpath, 
                                           filename=fname,
                                           as_attachment=True)

            # download weberr.txt
            elif name == 'download_err':
                fpath = os.path.join(wvars.upload_forlder, 'weberr.txt')
                if os.path.isfile(fpath):
                    return send_from_directory(
                           directory=wvars.upload_forlder, 
                           filename='weberr.txt',
                           as_attachment=True)

            # 更新所有
            elif name == 'fetch_all':
                c_message.make(web_back_queue, 'wb:request_fetch')

            # 删除所有异常
            elif name == 'del_except':
                print('try to delete all exceptions')
                db.del_all_exceptions()

            elif name == 'backup_db':
                db.compact_db()
                db.backup_db()

            elif name == 'reload_data':
                c_message.make(web_back_queue, 'wb:request_load')
                
            elif name == 'maintain_db':
                db.db_process()

        elif 'file' in request.files:
            f = request.files['file']
            if f and f.filename and f.filename.lower().endswith('.zip'):
                # save to file
                fpath = os.path.join(wvars.upload_forlder, 'uploaded.zip')
                f.save(fpath)

                if not is_zipfile(fpath):
                    return '无效zip文件'

                cfg_path = os.path.join(gcfg.root_path, 'cfg')
                zftmp = os.path.join(wvars.upload_forlder,'tmp')

                # remove & make tmp dir
                try:
                    shutil.rmtree(zftmp)
                except Exception as e:
                    print('删除/temp/tmp时出现异常，这可能是正常现象。')

                try:
                    os.mkdir(zftmp)
                except Exception as e:
                    print('创建/temp/tmp时出现异常。', e)

                # extract to tmp dir
                try:
                    zf = ZipFile(fpath)
                    namelist = zf.namelist()
                    zf.extractall(zftmp)
                    zf.close()
                except Exception as e:
                    return '解压错误' + str(e)

                # copy to cfg dir
                if 'config.ini' in namelist:
                    cp_src_path = zftmp
                elif 'cfg/config.ini' in namelist:
                    cp_src_path = os.path.join(zftmp, 'cfg')
                else:
                    return 'zip文件里没有找到config.ini文件'

                try:
                    shutil.rmtree(cfg_path)
                except Exception as e:
                    return '无法删除cfg目录' + str(e)

                try:
                    shutil.copytree(cp_src_path, cfg_path)
                except Exception as e:
                    return '无法复制cfg目录' + str(e)

                print('.zip has been extracted')
                c_message.make(web_back_queue, 'wb:request_load')

    db_file, db_size = db.get_current_file()
    info_lst = get_info_list(gcfg, usertype, db_file, db_size)
    proc_lst = get_python_process(gcfg)

    # exception infos
    if usertype == 2:
        exceptions = db.get_all_exceptions()
    else:
        exceptions = db.get_exceptions_by_username(username)
    for i in exceptions:
        i.fetch_date = datetime.datetime.\
                       fromtimestamp(i.fetch_date).\
                       strftime('%m-%d %H:%M')
    
    return render_template('panel.html', type = usertype,
                           info_list=info_lst, proc_list=proc_lst,
                           entries = exceptions)


@web.route('/listall')
def listall():
    username = check_cookie()
    if not username:
        return r'<script>top.location.href="/";</script>'

    usertype = db.get_usertype(username)
    if usertype != 2:
        return '请使用管理员帐号查看此页面'
    
    listall = db.get_listall()
    return render_template('listall.html',
                           items=listall, source_num=len(listall))

@web.errorhandler(404)
def page_not_found(e):
    s = ('无效网址<br>'
         '<a href="/">点击此处返回首页</a>'
         )
    return s

@web.errorhandler(500)
def internal_error(exception):
    # beep
    if has_winsound:
        winsound.Beep(600, 1000)

    # del weberr.txt if size > 1M
    fpath = os.path.join(wvars.upload_forlder, 'weberr.txt')
    try:
        size = os.path.getsize(fpath)
    except:
        size = -1
        
    if size > 1024 * 1024:
        try:
            os.remove(fpath)
        except:
            pass

    # write to weberr.txt
    with open(fpath, 'a') as f:
        print(time.ctime(), file=f)
        print(str(type(exception)), str(exception), '\n', file=f)

    # print to console
    print('web-side exception:', str(exception))
    return str(exception)

@web.route('/check')
def check_bw_queue():
    if request.remote_addr != '127.0.0.1':
        print('%s请求检查web端队列，忽略' % request.remote_addr)
        return ''

    print('/check')

    while True:
        try:
            msg = back_web_queue.get(block=False)
        except queue.Empty:
            break

        if msg.command == 'bw:send_infos':
            db.add_infos(msg.data)

        elif msg.command == 'bw:source_finished':
            db.source_finished(msg.data)

        elif msg.command == 'bw:db_process_time':
            db.db_process()
            login_manager.maintenace()

        elif msg.command == 'bw:send_config_users':
            # config
            cfg = msg.data[0]
            cfg.web_pid = os.getpid()
            print('pid(web, back):', cfg.web_pid, cfg.back_pid)

            global gcfg
            gcfg = cfg

            template_cache.clear()
            login_manager.clear()

            # users
            users = msg.data[1]
            print('web-side got users: %d' % len(users))
            db.add_users(cfg, users)
  
        else:
            print('can not handle back->web message:', msg.command)

    return ''

def run_web(web_port, tmpfs_path,
            wb_queue, bw_queue):

    # queues
    global web_back_queue
    web_back_queue = wb_queue

    global back_web_queue
    back_web_queue = bw_queue

    # database
    global db
    db = c_db_wrapper(tmpfs_path)

    c_message.make(web_back_queue, 'wb:request_load')

    # tornado
    from tornado.wsgi import WSGIContainer
    from tornado.httpserver import HTTPServer
    from tornado.ioloop import IOLoop

    try:
        http_server = HTTPServer(WSGIContainer(web))
        http_server.listen(web_port)
        IOLoop.instance().start()
    except Exception as e:
        print('启动web服务器时出现异常，异常信息:')
        print(e)

    #-----------------
    # web service
    #-----------------
    #web.run(host='0.0.0.0', port=web_port)#, debug=True) 
