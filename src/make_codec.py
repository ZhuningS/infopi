# coding=utf-8

# generate encoding names

try:
    # https://pypi.python.org/pypi/webencodings/
    import webencodings
except ImportError:
    webencodings = None

import codecs
import itertools
    
PATCH = {
    'chinese':             'gb18030',
    'csgb2312':            'gb18030',
    'csiso58gb231280':     'gb18030',
    'gb18030':             'gb18030',
    'gb18030-2000':        'gb18030',
    'gb18030-2005':        'gb18030', # tiny difference
    'gb2312':              'gb18030',
    'gb_2312':             'gb18030',
    'gb_2312-80':          'gb18030',
    'gbk':                 'gb18030',
    'iso-ir-58':           'gb18030',
    'x-gbk':               'gb18030',
    'iso-2022-cn':         'gb18030',
    'iso-2022-cn-ext':     'gb18030',
    'hz-gb-2312':          'hz',
    'big5':                'big5',
    'big5-hkscs':          'big5hkscs',
    'hkscs':               'big5hkscs',
    'cn-big5':             'big5',
    'csbig5':              'big5',
    'x-x-big5':            'big5',
    }
    
def main():
    LABELS = webencodings.LABELS
    print('webencodings has %d codec names.' % len(LABELS))
    
    REMAP = webencodings.PYTHON_NAMES
    print('webencodings has %d remap names.' % len(REMAP))
    
    # del 'x-user-defined' : 'x-user-defined'
    try:
        del LABELS['x-user-defined']
    except:
        pass
    
    # remap
    for k, v in LABELS.items():
        if v in REMAP:
            LABELS[k] = REMAP[v]
            
    # add patch
    for k, v in PATCH.items():
        LABELS[k] = v
        
    # verify
    for v in LABELS.values():
        if '2312' in v or 'gbk' in v:
            print('ERROR: %s is not gb18030.' % v)
        
        try:
            codecs.lookup(v)
        except:
            print("ERROR: codecs module doesn't has %s" % v)
        
    print('Generated %d codec names.' % len(LABELS))
    
    # sort
    l = sorted( (v,k) for k,v in LABELS.items() )
    
    # chain
    c = itertools.chain(('    LABELS = {',),
                        ("        {!r:<28}:{!r},".format(k,v) for v,k in l),
                        ('    }',)
                        )
     
 
    # save to file
    with open('codecname.txt', 'w', encoding='utf-8') as f:
        f.writelines('\n'.join(c))

if __name__ == '__main__':
    if webencodings != None:
        main()
