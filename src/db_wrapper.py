# coding=utf-8

import bisect
import collections
import time
import hashlib

try:
    import winsound
except:
    winsound = None

from sqldb import *


class c_index_unit:
    __slots__ = ('iid', 'fetch_date')

    def __init__(self, iid, fetch_date):
        self.iid = iid
        self.fetch_date = fetch_date

    def __lt__(self, other):
        if self.fetch_date != other.fetch_date:
            return self.fetch_date > other.fetch_date

        return self.iid > other.iid

    def __eq__(self, other):
        return self.iid == other.iid and \
            self.fetch_date == other.fetch_date

    def __ne__(self, other):
        return self.iid != other.iid or \
            self.fetch_date != other.fetch_date

    def __str__(self):
        return str(self.iid) + ',' + str(self.fetch_date)


class c_user_table:
    __slots__ = ('username', 'password', 'up_hash',
                 'col_per_page', 'col_per_page_pad',
                 'col_per_page_bigmobile', 'usertype', 'show_exceptions',
                 'sid_level_dict', 'sid_list',
                 'cate_list', 'cate_indexlist_dict',
                 'show_list', 'appeared_source_num')

    def __init__(self):
        self.username = ''
        self.password = ''
        self.up_hash = ''
        self.col_per_page = 15
        self.col_per_page_pad = 12
        self.usertype = 1
        self.show_exceptions = True

        # source_id -> level
        self.sid_level_dict = dict()

        # for fetch request
        self.sid_list = list()

        # 元素为tuple: (category, <list>)
        # <list>元素为source_id
        self.cate_list = list()

        # category -> <list>
        # <list>元素为c_index_unit
        # 0,1,2为三种级别信息，-1为异常信息
        self.cate_indexlist_dict = dict()

        # 元素为tuple: (category, <list>)
        # <list>元素为c_for_show
        self.show_list = list()

        # 出现的信息源数目(包括重复的)
        self.appeared_source_num = 0


class c_source_table:
    __slots__ = ('source_id',
                 'name', 'comment', 'link', 'interval', 'max_db',
                 'user_cateset_dict', 'index_list', 'last_fetch_date')

    def __init__(self):
        self.source_id = ''

        self.name = ''
        self.comment = ''
        self.link = ''
        self.interval = 0
        self.max_db = None

        # username -> <set>
        # <set>的元素为 category
        self.user_cateset_dict = dict()

        # 元素为c_index_unit
        self.index_list = list()

        # last fetch_date
        self.last_fetch_date = '尚未刷新'

    def last_date_distance(self):
        if len(self.index_list) > 0:
            t = self.index_list[0].fetch_date
            distance = int(time.time()) - t

            if distance <= 24 * 3600:
                return '1天内'
            elif distance <= 7 * 24 * 3600:
                return '1周内'
            elif distance <= 30 * 24 * 3600:
                return '1月内'
            elif distance <= 3 * 30 * 24 * 3600:
                return '3月内'
            elif distance <= 6 * 30 * 24 * 3600:
                return '6月内'
            else:
                return '6月以上'
        else:
            return '尚无信息'


class c_for_show:
    __slots__ = ('source',
                 'level_str', 'interval_str', 'encoded_url')

    def __init__(self):
        self.source = None

        self.level_str = ''
        self.interval_str = ''
        self.encoded_url = ''


class c_for_listall:
    __slots__ = ('source',
                 'interval_str', 'userlist', 'color')

    def __init__(self):
        self.source = None

        self.interval_str = ''
        self.userlist = list()
        self.color = 0

    def __lt__(self, other):
        if self.source.source_id < other.source.source_id:
            return True
        else:
            return False


def get_interval_str(interval):
    interval_str = ''

    if interval >= 24 * 3600:
        interval_str += '%d天' % (interval // (24 * 3600))
        interval = interval % (24 * 3600)

    if interval >= 3600:
        interval_str += '%d小时' % (interval // 3600)
        interval = interval % 3600

    if interval >= 60:
        interval_str += '%d分钟' % (interval // 60)
        interval = interval % 60

    if interval > 0:
        interval_str += '%d秒' % interval

    return interval_str


def hasher(string):
    hashobj = hashlib.md5()
    hashobj.update(string.encode('utf-8'))
    return hashobj.hexdigest()


class c_db_wrapper:
    __slots__ = ('sqldb',
                 'users', 'sources', 'hash_user', 'encoded_sid',
                 'ghost_sources', 'exceptions_index',
                 'cfg', 'listall')

    def __init__(self, tmpfs_path):
        self.sqldb = c_sqldb_keeper(tmpfs_path)
        self.sqldb.set_callbacks(self.callback_append_one_info,
                                 self.callback_remove_from_indexs,
                                 self.callback_add_to_indexs)

        self.users = dict()
        self.sources = dict()

        self.hash_user = dict()
        self.encoded_sid = dict()

        # sid
        self.ghost_sources = set()

        # 元素为c_index_unit
        self.exceptions_index = list()

        self.cfg = None
        self.listall = None

    def add_infos(self, lst):
        if not lst:
            return

        itr = (i for i in lst[::-1] if i.source_id in self.sources)
        updated = self.sqldb.add_info_list(itr)

        if updated:
            print('database was added or updated')
            # 发出响声
            if winsound is not None:
                try:
                    winsound.Beep(350, 300)
                except:
                    pass

    # 成功获取列表
    def success_infos(self, sid, fetch_time_str, lst):
        # fetch time
        self.sources[sid].last_fetch_date = fetch_time_str

        # remove exception
        self.sqldb.del_exception_by_sid(sid)

        # add infos
        self.add_infos(lst)

    # 出现异常
    def exception_info(self, einfo_lst):
        self.add_infos(einfo_lst)

    def add_one_user(self, cfg, user):
        # create user_table
        ut = c_user_table()
        self.users[user.username] = ut

        ut.username = user.username
        ut.password = user.password
        ut.usertype = user.usertype
        ut.show_exceptions = user.show_exceptions
        ut.col_per_page = user.col_per_page
        ut.col_per_page_pad = user.col_per_page_pad
        ut.col_per_page_bigmobile = user.col_per_page_bigmobile

        # cate_indexlist_dict, for level 0, 1, 2
        ut.cate_indexlist_dict[0] = list()
        ut.cate_indexlist_dict[1] = list()
        ut.cate_indexlist_dict[2] = list()
        # exception infos
        ut.cate_indexlist_dict[-1] = list()

        # temp dict for encoded_sid
        temp_sid_int_dic = dict()

        for cate_tuple in user.category_list:
            now_cate = cate_tuple[0]

            # cate_indexlist_dict
            ut.cate_indexlist_dict[now_cate] = list()

            # cate_list.cate
            ut.cate_list.append((cate_tuple[0], list()))

            for source_tuple in cate_tuple[1]:
                # sinfo was created in user_manage
                # 0~2   [sid, level, interval,
                # 3~5    'name', 'comment', 'link',
                # 6~7    'last_fetch', 'max_db']
                now_sid = source_tuple[0]

                # cate_list.cate.sid
                ut.cate_list[-1][1].append(now_sid)

                # sid_level_dict, level
                if now_sid not in ut.sid_level_dict:
                    ut.sid_level_dict[now_sid] = source_tuple[1]
                else:
                    ut.sid_level_dict[now_sid] = \
                        max(ut.sid_level_dict[now_sid], source_tuple[1])

                # sources table
                st = self.sources.setdefault(now_sid, c_source_table())
                if not st.source_id:
                    st.source_id = now_sid
                    st.interval = source_tuple[2]
                    st.name = source_tuple[3]
                    st.comment = source_tuple[4]
                    st.link = source_tuple[5]
                    if source_tuple[6]:
                        st.last_fetch_date = source_tuple[6]
                    st.max_db = source_tuple[7]
                    #print(st.name, st.comment)

                # source_table.user_cateset_dict
                ucs = st.user_cateset_dict.setdefault(user.username, set())
                ucs.add(now_cate)

        # for category 0, 1, 2
        for category, sid_list in ut.cate_list:
            for sid in sid_list:
                level = ut.sid_level_dict[sid]

                st = self.sources[sid]
                ucs = st.user_cateset_dict[user.username]

                if level == 0:
                    ucs.add(0)
                elif level == 1:
                    ucs.add(0)
                    ucs.add(1)
                elif level == 2:
                    ucs.add(0)
                    ucs.add(1)
                    ucs.add(2)
                else:
                    print('add user: level error')

        # hash->user dict
        s = user.username + ' (^.^) ' + user.password
        up_hash = hasher(s)

        self.hash_user[up_hash] = user.username
        ut.up_hash = up_hash

        # for fetch request
        ut.sid_list = list(ut.sid_level_dict.keys())

        # for show
        for i, (cate, sid_lst) in enumerate(ut.cate_list):
            temp_lst = list()

            for sid in sid_lst:
                one = c_for_show()
                source = self.sources[sid]

                # source ref
                one.source = source

                # encoded url
                if sid not in temp_sid_int_dic:
                    one.encoded_url = str(len(temp_sid_int_dic) + 1)
                    temp_sid_int_dic[sid] = one.encoded_url
                    self.encoded_sid[(user.username, one.encoded_url)] = sid
                else:
                    one.encoded_url = temp_sid_int_dic[sid]

                # level
                temp_level = ut.sid_level_dict[sid]
                if temp_level == 0:
                    one.level_str = '普通'
                elif temp_level == 1:
                    one.level_str = '关注'
                elif temp_level == 2:
                    one.level_str = '重要'

                # interval str
                one.interval_str = get_interval_str(source.interval)

                temp_lst.append(one)

                # count appeared source number
                ut.appeared_source_num += 1

            ut.show_list.append((i, cate, temp_lst))

        #print('显示列表 %d' % len(ut.show_list))

    def add_users(self, cfg, users_lst):
        # clear first
        self.users.clear()
        self.sources.clear()
        self.hash_user.clear()
        self.encoded_sid.clear()
        self.ghost_sources.clear()
        self.exceptions_index.clear()

        self.cfg = cfg

        # creat data-structs
        for user in users_lst:
            self.add_one_user(cfg, user)

        # load data to build indexs
        self.sqldb.get_all_for_make_index()

        # build listall infomation ---------------
        tempd = dict()
        for source in self.sources.values():
            item = c_for_listall()
            item.source = source
            item.interval_str = get_interval_str(source.interval)

            tempd[source.source_id] = item

        for user, ut in self.users.items():
            for sid in ut.sid_list:
                tempd[sid].userlist.append(user)

        # sort by source_id
        self.listall = [item for item in tempd.values()]
        self.listall.sort()

        last_category = ''
        now_color = 2
        for item in self.listall:
            # sort userlist
            item.userlist.sort()
            item.userlist = ' '.join(item.userlist)

            # color
            category, temp = item.source.source_id.split(':')
            if category != last_category:
                now_color = 2 if now_color == 1 else 1
                last_category = category
            item.color = now_color

    # --------------- callbacks -------------------

    # used for creating indexs
    def callback_append_one_info(self, source_id, iid, fetch_date, suid):
        if source_id not in self.sources:
            # print and add to ghost
            if source_id not in self.ghost_sources:
                s = 'datebase wrapper: %s is ghost source'
                print(s % source_id)
                self.ghost_sources.add(source_id)
            return

        unit = c_index_unit(iid, fetch_date)

        # category indexs
        ucd = self.sources[source_id].user_cateset_dict
        for user, cateset in ucd.items():
            if suid != '<exception>' or self.users[user].show_exceptions:
                for cate in cateset:
                    self.users[user].cate_indexlist_dict[cate].append(unit)

        # source index
        sindex = self.sources[source_id].index_list
        sindex.append(unit)

        # exception index
        if suid == '<exception>':
            self.exceptions_index.append(unit)
            for user in ucd.keys():
                self.users[user].cate_indexlist_dict[-1].append(unit)

    # remove from indexs
    def callback_remove_from_indexs(self, source_id, iid, fetch_date, suid):
        # it's ghost source
        if source_id not in self.sources:
            return

        unit = c_index_unit(iid, fetch_date)

        # category indexs
        ucd = self.sources[source_id].user_cateset_dict
        for user, cate_set in ucd.items():
            if suid != '<exception>' or self.users[user].show_exceptions:
                for cate in cate_set:
                    index = self.users[user].cate_indexlist_dict[cate]
                    p = bisect.bisect_left(index, unit)

                    assert unit == index[p], '删除时，c_db_wrapper的索引出错'
                    del index[p]

        # source index
        sindex = self.sources[source_id].index_list
        p = bisect.bisect_left(sindex, unit)

        assert unit == sindex[p], '删除时，c_db_wrapper的索引出错'
        del sindex[p]

        # exception index
        if suid == '<exception>':
            sindex = self.exceptions_index
            p = bisect.bisect_left(sindex, unit)

            assert unit == sindex[p], '删除时，c_db_wrapper的索引出错'
            del sindex[p]

            for user in ucd.keys():
                sindex = self.users[user].cate_indexlist_dict[-1]
                p = bisect.bisect_left(sindex, unit)

                assert unit == sindex[p], '删除时，c_db_wrapper的索引出错'
                del sindex[p]

    # add to indexs
    def callback_add_to_indexs(self, source_id, iid, fetch_date, suid):
        unit = c_index_unit(iid, fetch_date)

        # category indexs
        ucd = self.sources[source_id].user_cateset_dict
        for user, cate_set in ucd.items():
            if suid != '<exception>' or self.users[user].show_exceptions:
                for cate in cate_set:
                    index = self.users[user].cate_indexlist_dict[cate]
                    bisect.insort_left(index, unit)

        # source index
        sindex = self.sources[source_id].index_list
        bisect.insort_left(sindex, unit)

        # exception index
        if suid == '<exception>':
            bisect.insort_left(self.exceptions_index, unit)

            for user in ucd.keys():
                index = self.users[user].cate_indexlist_dict[-1]
                bisect.insort_left(index, unit)

    # ----------- utility --------------
    def compact_db(self):
        self.sqldb.compact_db()

    def backup_db(self):
        self.sqldb.backup_db(self.cfg.db_backup_maxfiles)

    def db_process(self):
        print('database maintenance')

        # del too-many data
        del_lst = list()
        if self.cfg.db_process_del_days == -1:
            for s in self.sources.values():
                sid = s.source_id
                index = s.index_list

                max_entires = s.max_db if s.max_db is not None else self.cfg.db_process_del_entries
                if len(index) > max_entires:
                    p = max_entires
                    #(source_id, id, fetch_date)
                    tuple_lst = ((sid, i.iid, i.fetch_date) for i in index[p:])
                    del_lst.extend(tuple_lst)
        else:
            before_del = int(time.time()) - \
                self.cfg.db_process_del_days * 24 * 3600
            tmp_unit = c_index_unit(0, before_del)

            for s in self.sources.values():
                sid = s.source_id
                index = s.index_list

                max_entires = s.max_db if s.max_db is not None else self.cfg.db_process_del_entries
                if len(index) > max_entires:
                    p = bisect.bisect_left(index, tmp_unit)
                    #(source_id, id, fetch_date)
                    tuple_lst = ((sid, i.iid, i.fetch_date) for i in index[p:])
                    del_lst.extend(tuple_lst)

        print('%d条数据将被删除' % len(del_lst))
        self.sqldb.del_info_by_tuplelist(del_lst)

        # ghost source
        if self.cfg.db_process_rm_ghost:
            for sid in self.ghost_sources:
                self.sqldb.del_ghost_by_sid(sid)
            self.ghost_sources.clear()

        # backup
        self.sqldb.compact_db()
        self.sqldb.backup_db(self.cfg.db_backup_maxfiles)

    def get_current_file(self):
        return self.sqldb.get_current_file()

    def del_all_exceptions(self):
        self.sqldb.del_all_exceptions()

    # for left category
    def get_category_list_by_username(self, username):
        if username not in self.users:
            return None

        ut = self.users[username]
        return (cate for cate, lst in ut.cate_list)

    # return col_per_page
    def get_colperpage_by_user(self, username):
        return self.users[username].col_per_page

    def get_colperpagepad_by_user(self, username):
        return self.users[username].col_per_page_pad

    def get_colperpagebm_by_user(self, username):
        return self.users[username].col_per_page_bigmobile

    def get_colperpagemobile(self):
        return self.cfg.mobile_colperpage

    # for show
    def get_name_by_sid(self, sid):
        return self.sources[sid].name

    # for show
    def get_forshow_by_user(self, username):
        return self.users[username].show_list

    # for show
    def get_sid_by_encoded(self, username, encoded):
        try:
            return self.encoded_sid[(username, encoded)]
        except:
            return ''

    def get_cate_list_for_fetch(self, username, cate_idx):
        try:
            _, _, lst = self.users[username].show_list[int(cate_idx)]
        except:
            return None

        return [one.source.source_id for one in lst]

    def is_valid_sid(self, sid):
        return sid in self.sources

    # listall
    def get_listall(self):
        return self.listall

    # for cateinfo. all/unduplicated sources number
    def get_sourcenum_by_user(self, username):
        return self.users[username].appeared_source_num, \
            len(self.users[username].sid_list)

    # get fetch list (sid)
    def get_fetch_list_by_user(self, username):
        return self.users[username].sid_list

    def get_usertype(self, username):
        return self.users[username].usertype

    # 从('iid', 'fetch_date')索引获得数据
    def get_infos(self, index, offset, limit):
        allcount = len(index)
        end = min(offset + limit, len(index))

        ret_list = self.sqldb.get_info_by_iid_list(
            index[i].iid for i in range(offset, end)
        )

        return allcount, ret_list

    # get infos of a page
    def get_infos_by_user_category(self,
                                   username, category,
                                   offset, limit):
        try:
            index = self.users[username].cate_indexlist_dict[category]
        except:
            return None, None

        return self.get_infos(index, offset, limit)

    # get infos of a source
    def get_infos_by_sid(self, username, sid, offset, limit):
        if sid not in self.users[username].sid_level_dict:
            return None, None

        index = self.sources[sid].index_list

        return self.get_infos(index, offset, limit)

    # get all exceptions infos
    def get_infos_all_exceptions(self, offset, limit):
        return self.get_infos(self.exceptions_index, offset, limit)

    # get user exceptions infos
    def get_infos_user_exception(self, username, offset, limit):
        index = self.users[username].cate_indexlist_dict[-1]
        return self.get_infos(index, offset, limit)

    # get all exceptions
    def get_all_exceptions(self):
        return self.sqldb.get_info_by_iid_list(
            i.iid for i in self.exceptions_index
        )

    # 所有异常信息的数目
    def get_all_exception_num(self):
        return len(self.exceptions_index)

    # get exceptions by username
    def get_exceptions_by_username(self, username):
        return self.sqldb.get_info_by_iid_list(
            i.iid for i in self.users[username].cate_indexlist_dict[-1]
        )

    # 用户的异常数
    def get_exceptions_num_by_username(self, username):
        return len(self.users[username].cate_indexlist_dict[-1])

    # 此用户是否显示异常信息
    def should_show_exceptions(self, username):
        return self.users[username].show_exceptions

    # ----------- for login --------------

    # login
    def login(self, username, password):
        if username not in self.users:
            return ''

        ut = self.users[username]
        if password == ut.password:
            return ut.up_hash
        else:
            return ''

    # get user from hash_user dict
    def get_user_from_hash(self, ha):
        return self.hash_user.get(ha)

    # get user number:
    def get_user_number(self):
        return len(self.users)


class c_login_manager:
    # if one ip has tried RECENT_COUNT in the
    # last RECENT_TIME, then forbid login for FORBID_TIME
    # (unit of times are seconds)
    RECENT_TIME = 3 * 60
    RECENT_COUNT = 4
    FORBID_TIME = 10 * 60
    FAILED_LOGIN_ALARM = 50

    def __init__(self, write_weberr):
        # ip -> <list>
        # <list>: [next_time, deque(time)]
        self.ip_dict = dict()

        self.fail_count = 0
        self.write_weberr = write_weberr

    def login_check(self, ip):
        now_time = int(time.time())

        if ip in self.ip_dict and now_time < self.ip_dict[ip][0]:
            delta = self.ip_dict[ip][0] - now_time
            return False, '尝试登录次数太多，请于%d秒后再试' % delta

        return True, ''

    def login_fail(self, ip):
        now_time = int(time.time())

        # del old
        self.maintenace(now_time)

        # append now_time
        if ip not in self.ip_dict:
            self.ip_dict[ip] = [0, collections.deque()]
        self.ip_dict[ip][1].append(now_time)

        # forbid ip?
        if len(self.ip_dict[ip][1]) >= c_login_manager.RECENT_COUNT:
            self.ip_dict[ip][0] = now_time + c_login_manager.FORBID_TIME

            # msg & log
            msg = '您的IP地址因多次登录失败被暂时禁止登录。'
            e = Exception('IP地址%s因多次登录失败被暂时禁止登录。' % ip)
            self.write_weberr(e)
        else:
            msg = '无此用户或密码错误'

        # all fail count
        self.fail_count += 1
        if self.fail_count % c_login_manager.FAILED_LOGIN_ALARM == 0:
            e = Exception('程序启动以来，登录失败总数达到%d次。' %
                          self.fail_count)
            self.write_weberr(e)

        return msg

    def maintenace(self, now_time=None):
        if now_time is None:
            now_time = int(time.time())

        recent = now_time - c_login_manager.RECENT_TIME

        temp_lst = list()

        for ip, (next_time, deck) in self.ip_dict.items():
            # del old record
            while deck and deck[0] < recent:
                deck.popleft()

            # clear record
            if not deck:
                temp_lst.append(ip)

        for ip in temp_lst:
            del self.ip_dict[ip]
