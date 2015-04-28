from glob import glob
from keyword import iskeyword
from os.path import dirname, join, split, splitext

basedir = dirname(__file__)

__all__ = []
for name in glob(join(basedir, '*.py')):
    module = splitext(split(name)[-1])[0]
    if not module.startswith('_') and \
       module.isidentifier() and \
       not iskeyword(module):

       __import__(__name__+'.'+module)
       __all__.append(module)

        # try:
        #     __import__(__name__+'.'+module)
        # except Exception as e:
        #     print('无法加载worker: %s' % module)
        #     print('异常信息:', e, '\n')
        # else:
        #     __all__.append(module)

#print('import了%d个worker文件' % len(__all__))
