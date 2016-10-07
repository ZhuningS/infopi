# coding=utf-8
# 本工具用于紧凑数据库id，使之连续。
# 本工具基本没用，只用于防备出现极端情况。
# 需要安装sqlalchemy

import sys
import os

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select


def doit(src, desc):
    # read
    engine = create_engine('sqlite:///' + src)

    metadata = MetaData()
    info_tbl = Table('info_tbl', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('source_id', String, index=True),
                     Column('suid', String, index=True),
                     Column('fetch_date', Integer, index=True, nullable=False),

                     Column('title', String, nullable=False),
                     Column('url', String),

                     Column('author', String),
                     Column('summary', String),
                     Column('pub_date', String)
                     )
    conn = engine.connect()

    s = select([info_tbl]).order_by(info_tbl.c.id.asc())
    result = conn.execute(s)

    # write
    e2 = create_engine('sqlite:///' + desc)
    m2 = MetaData()
    info_tbl = Table('info_tbl', m2,
                     Column('id', Integer, primary_key=True),
                     Column('source_id', String, index=True),
                     Column('suid', String, index=True),
                     Column('fetch_date', Integer, index=True, nullable=False),

                     Column('title', String, nullable=False),
                     Column('url', String),

                     Column('author', String),
                     Column('summary', String),
                     Column('pub_date', String)
                     )
    m2.create_all(e2)
    c2 = e2.connect()

    lst = []
    for i in result.fetchall():
        d = {'source_id': i[1],
             'suid': i[2],
             'fetch_date': i[3],

             'title': i[4],
             'url': i[5],

             'author': i[6],
             'summary': i[7],
             'pub_date': i[8]}
        lst.append(d)

    c2.execute(info_tbl.insert(), lst)

    # VACUUM
    c2.execute('VACUUM')


def print_tip():
    print('本工具用于紧凑数据库主键(info_tbl的id字段)，使之变得连续。用法：')
    print('compact_db_id.py  <已有数据库文件名>  <新数据库文件名>')
    print('本工具必须在数据库文件的目录下执行，<已有数据库>必须存在，<新数据库>必须不存在。')


def main():
    if len(sys.argv) != 3:
        print_tip()
        return

    if not os.path.isfile(sys.argv[1]):
        print('已有数据库必须存在')
        return

    if os.path.isfile(sys.argv[2]):
        print('新数据库必须不存在')
        return

    doit(sys.argv[1], sys.argv[2])

    size1 = format(os.path.getsize(sys.argv[1]), ',')
    size2 = format(os.path.getsize(sys.argv[2]), ',')
    print('完成，旧文件 %s字节，新文件 %s字节。' % (size1, size2))

if __name__ == '__main__':
    main()
