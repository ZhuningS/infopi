# coding=utf-8
import os
import pkgutil

pkgpath = os.path.dirname(__file__)
pkgname = os.path.basename(pkgpath)

for _, file, _ in pkgutil.iter_modules([pkgpath]):
    print(file)
    __import__(pkgname+'.'+file)