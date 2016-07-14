# coding=utf-8

from enum import IntEnum
import os
import shutil
import fnmatch
import sqlite3
import time
import bisect

from datadefine import *
from db_wrapper import *
import wvars

__all__ = ('DB_RESULT', 'c_sqldb_keeper')

class DB_RESULT(IntEnum):
    NO = 0
    ADDED = 1
    UPDATED = 2

class c_sqldb:
    filename = 'sql.db'

    def __init__(self, tempfs_dir=''):
        # vars
        self.dbfile_dir = self.get_dbfile_dir()
        self.tmpfs_dir = tempfs_dir
        self.current_file = ''
        self.has_changed = False

        # callbacks
        self.cb_append = None
        self.cb_remove = None
        self.cb_add = None

        db_lst = self.get_dbfile_list(self.dbfile_dir)

        # get current file
        if len(db_lst):
            self.current_file = db_lst[-1]
        else:
            filename = 'sql' + self.get_time_str() + '.db'
            filename = os.path.join(self.dbfile_dir, filename)

            # creat db
            self.open(filename)
            self.creat_db()
            self.close()

            self.current_file = filename

        # has tmpfs
        if self.tmpfs_dir:
            # full path in tmpfs
            new_dst_file_name = os.path.join(self.tmpfs_dir, 'sql.db')

            # remove exist sql.db
            try:
                os.remove(new_dst_file_name)
            except:
                pass

            # copy
            shutil.copy2(self.current_file, new_dst_file_name)

            self.current_file = new_dst_file_name


        print('current db file:', self.current_file)
        self.open(self.current_file)

    # now time to str
    def get_time_str(self):
        return time.strftime('%y%m%d_%H%M%S')

    # get sorted db file list
    def get_dbfile_list(self, dbfile_dir):
        lst = list()

        for root, dirs, files in os.walk(dbfile_dir):
            for fn in files:
                if fnmatch.fnmatch(fn, 'sql*.db'):
                    fn = os.path.join(root, fn)
                    lst.append(fn)
        lst.sort()
        return lst

    # get db files dir
    def get_dbfile_dir(self):
        script_dir = os.path.join(wvars.root_path, 'database')
        return script_dir

    # backup db file
    def backup_db(self, max_files=10):
        db_lst = self.get_dbfile_list(self.dbfile_dir)
        
        if not self.has_changed and db_lst:
            return
        
        print('try to backup database file')

        # close db
        self.close()

        # copy db
        dest_fn = 'sql' + self.get_time_str() + '.db'
        dest_fn = os.path.join(self.dbfile_dir, dest_fn)
        shutil.copy2(self.current_file, dest_fn)
        print('copy db file from %s to %s' % \
              (self.current_file, dest_fn)
              )

        # no tmpfs, point current_file to newest db
        if not self.tmpfs_dir:
            self.current_file = dest_fn

        # remove earlier db
        if len(db_lst) > max_files:
            db_lst = db_lst[0:len(db_lst)-max_files]
            for del_fn in db_lst:
                try:
                    os.remove(del_fn)
                except:
                    pass

        # re-open db
        self.has_changed = False
        self.open(self.current_file)
        print('current db file:', self.current_file)

    # 压缩数据库
    def compact_db(self):
        self.cursor.execute('VACUUM')
    
    # 创建数据库
    def creat_db(self):      
        # table 
        sql = ('CREATE TABLE info_tbl ( '
               'id INTEGER PRIMARY KEY AUTOINCREMENT, '
               'source_id TEXT, '              # index
               'suid TEXT, '                   # index
               'fetch_date INTEGER NOT NULL, ' # index
                       
               'title TEXT NOT NULL, '
               'url TEXT, '
                      
               'author TEXT, '
               'summary TEXT, '
               'pub_date TEXT);'
               )
        self.cursor.execute(sql)

        # index
        sql = 'CREATE INDEX source_id_info_idx ON info_tbl(source_id);'
        self.cursor.execute(sql)

        sql = 'CREATE INDEX suid_info_idx ON info_tbl(suid);'
        self.cursor.execute(sql)

        sql = 'CREATE INDEX fetch_date_info_idx ON info_tbl(fetch_date);'
        self.cursor.execute(sql)

        print('database file created')


    # open db
    def open(self, filename):
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()        

    # close connection
    def close(self):
        self.cursor.close()
        self.conn.close()

    def set_callbacks(self, append, remove, add):
        self.cb_append = append
        self.cb_remove = remove
        self.cb_add = add

    def get_current_file(self):
        size = os.path.getsize(self.current_file)
        size = format(size,',')
        return self.current_file, size
    #--------------------------
    #  添加info
    #--------------------------

    # 添加单个
    def add_info(self, one):
        # 是否已有suid, source_id？
        sql = ('SELECT id, fetch_date '
               'FROM info_tbl '
               'WHERE suid = ? '
               '  AND source_id = ?'
               )
        r = self.cursor.execute(sql, (one.suid, one.source_id))
        fetched = r.fetchone()
        
        # 已有，更新
        if fetched:
            _id = fetched[0]
            _fetch_date = fetched[1]

            sql = ('UPDATE info_tbl '
                   'SET title = ?, url = ?, '
                   '    author = ?, summary = ?, pub_date = ?, '
                   '    fetch_date = ? '
                   'WHERE id = ? AND '
                   '    ( pub_date <> ? '
                   '   OR title <> ? '
                   '   OR summary <> ? '
                   '   OR author <> ? '
                   '   OR url <> ? )'
                   )
            self.cursor.execute(sql, (one.title, one.url,
                                      one.author, one.summary, one.pub_date,
                                      one.fetch_date,
                                      
                                      _id,

                                      one.pub_date,
                                      one.title,
                                      one.summary,
                                      one.author,
                                      one.url
                                      )
                                )
            
            if self.cursor.rowcount > 0:
                one.id = _id

                self.conn.commit()
                self.has_changed = True

                # update index
                self.cb_remove(one.source_id, _id, _fetch_date, one.suid)
                self.cb_add(one.source_id, _id, one.fetch_date, one.suid)

                return DB_RESULT.UPDATED
            else:
                return DB_RESULT.NO
                
        # 没有，添加        
        else:
            sql = ('INSERT INTO info_tbl VALUES '
                      '(NULL, ?, '  # id, source_id
                      ' ?, ?, '     # suid, fetch_date
                      ' ?, ?, '     # title, url
                      ' ?, ?, ?);'  # author, summary, pub_date 
                   )
            self.cursor.execute(sql,
                                (one.source_id,
                                 one.suid, one.fetch_date,
                                 one.title, one.url,
                                 one.author, one.summary, one.pub_date
                                 )
                                )

            # 添加的id
            one.id = self.cursor.lastrowid

            self.conn.commit()
            self.has_changed = True

            self.cb_add(one.source_id, one.id, one.fetch_date, one.suid)

            return DB_RESULT.ADDED             

    #--------------------------
    #  获取info
    #--------------------------

    # get all, for gengerate wrapper's index
    def get_all_for_make_index(self):
        sql = 'SELECT * FROM info_tbl ORDER BY fetch_date DESC, id DESC'
        self.cursor.execute(sql)

        count = 0
        while True:
            row = self.cursor.fetchone()
            if row == None:
                break

            s = c_info()
            
            s.id = row[0]
            s.source_id = row[1]
            s.suid = row[2]
            s.fetch_date = row[3]

            s.title = row[4]
            s.url = row[5]

            s.author = row[6]
            s.summary = row[7]
            s.pub_date = row[8]

            self.cb_append(s.source_id, s.id, s.fetch_date, s.suid)
            count += 1

        print('sqlite: %d rows loaded' % count)    

    # get one info by id
    def get_info_by_iid(self, iid):
        sql = ('SELECT * '
               'FROM info_tbl '
               'WHERE id = ?;'
               )
        self.cursor.execute(sql, (iid,))
        row = self.cursor.fetchone()
        if row == None:
            return None

        s = c_info()
        
        s.id = row[0]
        s.source_id = row[1]
        s.suid = row[2]
        s.fetch_date = row[3]

        s.title = row[4]
        s.url = row[5]

        s.author = row[6]
        s.summary = row[7]
        s.pub_date = row[8]

        return s

    # 转为info
    def get_infos_function(self, sql, paras=()):
        r = self.cursor.execute(sql, paras)

        ret = []
        for row in r.fetchall():
            s = c_info()
            
            s.id = row[0]
            s.source_id = row[1]
            s.suid = row[2]
            s.fetch_date = row[3]

            s.title = row[4]
            s.url = row[5]

            s.author = row[6]
            s.summary = row[7]
            s.pub_date = row[8]

            ret.append(s)

        return ret

    def get_all_exceptions(self):
        sql = ('SELECT * FROM info_tbl '
                  'WHERE suid = "<exception>" '
                  'ORDER BY fetch_date DESC, id DESC')
        ret = self.get_infos_function(sql)
        return ret

    #--------------------------
    #  删除info
    #--------------------------

    # 删除所有异常信息
    def del_all_exceptions(self, source_dict):
        # del exist from indexs
        sql = ('SELECT id, source_id, fetch_date '
               'FROM info_tbl '
               'WHERE suid = "<exception>"'
               )
        r = self.cursor.execute(sql)

        for i in r.fetchall():
            _id = i[0]
            _souce_id = i[1]
            _fetch_date = i[2]

            self.cb_remove(_souce_id, _id, _fetch_date, '<exception>')

        # del from sqlite
        sql = 'DELETE FROM info_tbl WHERE suid = "<exception>"'
        self.cursor.execute(sql)

        if self.cursor.rowcount > 0:
            self.conn.commit()
            self.has_changed = True

    def del_exceptions_by_sid(self, source_id):
        # del exist from indexs
        sql = ('SELECT id, fetch_date '
               'FROM info_tbl '
               'WHERE suid = "<exception>" '
               '  AND source_id = ?'
               )
        r = self.cursor.execute(sql, (source_id,))
        row = r.fetchone()
        
        # no exception, return
        if row == None:
            return
        
        # callback, remove from index
        _id = row[0]
        _fetch_date = row[1]
        self.cb_remove(source_id, _id, _fetch_date, '<exception>')

        # del from sqlite
        sql = 'DELETE FROM info_tbl WHERE id = ?'
        self.cursor.execute(sql, (_id,))

        if self.cursor.rowcount > 0:
            self.conn.commit()
            self.has_changed = True

    # lst: (source_id, id, fetch_date)
    def del_info_by_tuplelist(self, lst):
        if not lst:
            return
        
        sql1 = 'SELECT suid FROM info_tbl WHERE id = ?'
        sql2 = 'DELETE FROM info_tbl WHERE id = ?'

        for _sid, _id, _fetch_date in lst[::-1]:
            # sql1
            r = self.cursor.execute(sql1, (_id,))
            suid = r.fetchone()[0]
            
            # callback
            self.cb_remove(_sid, _id, _fetch_date, suid)
            
            # sql2
            self.cursor.execute(sql2, (_id,))

        if self.cursor.rowcount > 0:
            self.conn.commit()
            self.has_changed = True

    # remove ghost source
    def del_ghost_by_sid(self, sid):
        # del from keeper
        sql = ('SELECT id, suid, fetch_date '
               'FROM info_tbl '
               'WHERE source_id = ?'
               )
        r = self.cursor.execute(sql, (sid,))

        for i in r.fetchall():
            _id = i[0]
            _suid = i[1]
            _fetch_date = i[2]

            self.cb_remove(sid, _id, _fetch_date, _suid)
        
        # del from db
        sql = 'DELETE FROM info_tbl WHERE source_id = ?'
        self.cursor.execute(sql, (sid,))

        if self.cursor.rowcount > 0:
            self.conn.commit()
            self.has_changed = True
            print('%s有%d条幽灵数据被删除' % (sid, self.cursor.rowcount))
            
# =============== keeper ===============

class c_keeper_item:
    __slots__ = ('id', 'source_id', 'suid', 'fetch_date')
    
    def __init__(self, id, source_id, suid, fetch_date):
        self.id = id
        self.source_id = source_id
        self.suid = suid
        self.fetch_date = fetch_date
        
    def __lt__(self, other):
        if self.fetch_date != other.fetch_date:
            return self.fetch_date > other.fetch_date
        
        return self.id > other.id
    
    def __eq__(self, other):
        return self.id == other.id and \
               self.fetch_date == other.fetch_date
               
# no need to reload file from disk when reloading configure
class c_sqldb_keeper(c_sqldb):
    def __init__(self, tempfs_dir=''):
        super().__init__(tempfs_dir)
        
        self.full_list = None
        self.exception_dic = dict()
        
    def set_callbacks(self, append, remove, add):
        # for this layer
        self.cb_append = self.callback_append_one_info
        self.cb_remove = self.callback_remove_from_indexs
        self.cb_add = self.callback_add_to_indexs
        
        # for db_wrapper
        self.cb_append2 = append
        self.cb_remove2 = remove
        self.cb_add2 = add
        
    def callback_append_one_info(self, source_id, iid, fetch_date, suid):
        item = c_keeper_item(iid, source_id, suid, fetch_date)
        self.full_list.append(item)
        
        if suid == '<exception>':
            self.exception_dic[source_id] = item
        
        self.cb_append2(source_id, iid, fetch_date, suid)
    
    def callback_remove_from_indexs(self, source_id, iid, fetch_date, suid):
        item = c_keeper_item(iid, source_id, suid, fetch_date)
        p = bisect.bisect_left(self.full_list, item)
        
        del self.full_list[p]
        
        if suid == '<exception>':
            del self.exception_dic[source_id]
        
        self.cb_remove2(source_id, iid, fetch_date, suid)
    
    def callback_add_to_indexs(self, source_id, iid, fetch_date, suid):
        item = c_keeper_item(iid, source_id, suid, fetch_date)
        bisect.insort_left(self.full_list, item)
        
        if suid == '<exception>':
            self.exception_dic[source_id] = item
        
        self.cb_add2(source_id, iid, fetch_date, suid)
    
    def get_all_for_make_index(self):
        # first load
        if self.full_list == None:
            self.full_list = list()
            
            # load data to build indexs
            super().get_all_for_make_index()
        # buffered load
        else:
            print('sqlite keeper: %d rows buffered' % len(self.full_list))
            
            for item in self.full_list:
                self.cb_append2(item.source_id, item.id, 
                                item.fetch_date, item.suid)

