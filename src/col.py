# coding=utf-8
# 用于统计目录里的行数

import os
import sys

exts = ['.py', '.c', '.h']
here = os.path.abspath(os.path.dirname(sys.argv[0]))
idx = sys.argv[0].rfind('\\')
if idx != -1:
    myname = sys.argv[0][idx + 1:]
else:
    myname = ''


def read_line_count(fname):
    count = 0
    for line in open(fname, encoding='utf-8').readlines():
        line.strip()
        if line and not line.startswith('#'):
            count += 1
    return count

if __name__ == '__main__':
    line_count = 0
    file_count = 0
    for base, dirs, files in os.walk(here):
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext in exts \
               and not (base == here and file == myname):
                file_count += 1
                path = os.path.join(base, file)
                c = read_line_count(path)
                print(".%s : %d" % (path[len(here):], c))
                line_count += c

    print('File count : %d' % file_count)
    print('Line count : %d' % line_count)

    if os.name == 'nt':
        print()
        os.system('pause')
