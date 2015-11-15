# coding=utf-8

import codecs
import urllib.request
import time
import gzip
import zlib
import threading
from http.cookiejar import CookieJar

# import cchardet or chardet
try:
    import cchardet as chardet
except:
    try:
        import chardet
    except:
        chardet = None

from red import *
from worker_manage import c_worker_exception

#========================================
#       网络获取
#========================================

class FetcherInfo:
    def __init__(self):
        self.ua = ('Mozilla/5.0 (Windows NT 6.1; rv:38.0)'
                   ' Gecko/20100101 Firefox/38.0')
        self.referer = ''
        self.open_timeout = 120
        self.retry_count = 4
        self.retry_interval = 3


# compiled re
re_contenttype = red.d(r'charset\s*=\s*([^;\s]*)', red.I|red.A)

meta_encoding = (br'''<(?:meta|\?xml)[^>]*?'''
                 br'''(?:charset|encoding)\s*=\s*["']?([^"'>;\s]{1,30})'''
                )
re_meta = red.d(meta_encoding, red.A|red.S|red.I)


class Fetcher:
    '''web获取器'''
    #------------------
    # encoding cache
    #------------------
    encoding_cache = dict()
    lock = threading.Lock()

    @staticmethod
    def d(url, bytes_data):
        if chardet == None:
            return ''

        Fetcher.lock.acquire()

        if url not in Fetcher.encoding_cache:
            cd_r = chardet.detect(bytes_data)
            cd_confidence = cd_r['confidence']
            cd_encoding = cd_r['encoding']

            if cd_confidence > 0.8:
                ret = Fetcher.lookup_encoding(cd_encoding)
                print('\n%s\nchardet[encoding:%s, confidence:%.5f]' % \
                      (url, ret, cd_confidence)
                      )
            else:
                ret = ''

            # add to cache
            Fetcher.encoding_cache[url] = ret

        else:
            ret = Fetcher.encoding_cache[url]

        Fetcher.lock.release()
        return ret

    @staticmethod
    def clear_cache():
        Fetcher.lock.acquire()
        Fetcher.encoding_cache.clear()
        Fetcher.lock.release()
    # -----------------

    def __init__(self, fetcher_info=None):
        if not fetcher_info:
            fetcher_info = FetcherInfo()
            
        self.referer = fetcher_info.referer
        self.info = fetcher_info
        
        # ============
        #    opener
        # ============
        
        # no proxy
        proxy = urllib.request.ProxyHandler({})
        # cookie for redirect
        cj = urllib.request.HTTPCookieProcessor(CookieJar())
        # opener
        self.opener = urllib.request.build_opener(proxy, cj)

    def fetch_html(self, url, encoding='', errors='strict'):
        if not errors:
            errors = 'strict'

        bytes_data, bytes_encoding = self.fetch_bytes_encoding(url)

        # get encoding
        if not encoding:
            # server send
            encoding = bytes_encoding

            if not encoding:
                # chardect
                encoding = Fetcher.d(url, bytes_data)

                if not encoding:
                    # default: utf-8
                    print('fetcher:打开%s时，使用utf-8作默认' % url)
                    encoding = 'utf-8'

        # decode
        try:
            return bytes_data.decode(encoding, errors)
        except:
            print('下载器<解文本编码>失败')

            s = '字节长度:%d.使用编码:%s.网址:%s' % \
                (len(bytes_data), encoding, url)
            raise c_worker_exception('解文本编码(decode)时出现异常', url, s)

    def fetch_bytes_encoding(self, url):
        '''return (byte data, encoding)'''
        def get_encoding(r):
            contenttype = r.getheader('Content-Type', '')
            if not contenttype:
                return ''
            
            matcher = re_contenttype.search(contenttype)
            if matcher:
                return Fetcher.lookup_encoding(matcher.group(1))
            else:
                return ''

        # --------------主体开始-------------

        # request对象
        req = urllib.request.Request(url)
        req.add_header('User-Agent', self.info.ua)

        e = None
        # 重试用的循环
        for i in range(self.info.retry_count):
            try:
                # r是HTTPResponse对象
                r = self.opener.open(req,
                                timeout=self.info.open_timeout
                                )
                ret_data = r.read()
                encoding = get_encoding(r)

                # decompress
                contentenc = r.getheader('Content-Encoding', '')
                if contentenc:
                    contentenc = contentenc.lower()
                    if 'gzip' in contentenc:
                        ret_data = gzip.decompress(ret_data)
                    elif 'deflate' in contentenc:
                        try:
                            # first try: zlib
                            ret_data = zlib.decompress(ret_data, 15)
                        except:
                            # second try: raw deflate
                            ret_data = zlib.decompress(ret_data, -15)
                
                # get encoding from bytes content
                if not encoding:
                    matcher = re_meta.search(ret_data)
                    if matcher:
                        try:
                            extract = matcher.group(1).decode('ascii')
                        except:
                            encoding = ''
                        else:
                            encoding = Fetcher.lookup_encoding(extract)

                return ret_data, encoding

            except Exception as ee:
                e = ee
            
            if i < self.info.retry_count-1:
                time.sleep(self.info.retry_interval)
        else:
            print('fetcher:异常,下载%s失败' % url, '\n异常信息:', e)

            s = '%s (下载%s失败，重试了%d次，连接超时限制%d秒)' % \
                (str(e), url, self.info.retry_count, self.info.open_timeout)
            raise c_worker_exception('下载url失败', url, s)

    @staticmethod
    def lookup_encoding(in_encoding):
        in_encoding = in_encoding.strip().lower()
        if not in_encoding:
            return ''

        encoding = Fetcher.LABELS.get(in_encoding, None)

        if encoding == None:
            try:
                codecs.lookup(in_encoding)
            except:
                print('无此编码', in_encoding)
                encoding = ''
            else:
                encoding = in_encoding
            Fetcher.LABELS[in_encoding] = encoding

        return encoding

    # generated by webencodings
    # https://pypi.python.org/pypi/webencodings/
    LABELS = {
        'unicode-1-1-utf-8':   'utf-8',
        'utf-8':               'utf-8',
        'utf8':                'utf-8',
        '866':                 'ibm866',
        'cp866':               'ibm866',
        'csibm866':            'ibm866',
        'ibm866':              'ibm866',
        'csisolatin2':         'iso-8859-2',
        'iso-8859-2':          'iso-8859-2',
        'iso-ir-101':          'iso-8859-2',
        'iso8859-2':           'iso-8859-2',
        'iso88592':            'iso-8859-2',
        'iso_8859-2':          'iso-8859-2',
        'iso_8859-2:1987':     'iso-8859-2',
        'l2':                  'iso-8859-2',
        'latin2':              'iso-8859-2',
        'csisolatin3':         'iso-8859-3',
        'iso-8859-3':          'iso-8859-3',
        'iso-ir-109':          'iso-8859-3',
        'iso8859-3':           'iso-8859-3',
        'iso88593':            'iso-8859-3',
        'iso_8859-3':          'iso-8859-3',
        'iso_8859-3:1988':     'iso-8859-3',
        'l3':                  'iso-8859-3',
        'latin3':              'iso-8859-3',
        'csisolatin4':         'iso-8859-4',
        'iso-8859-4':          'iso-8859-4',
        'iso-ir-110':          'iso-8859-4',
        'iso8859-4':           'iso-8859-4',
        'iso88594':            'iso-8859-4',
        'iso_8859-4':          'iso-8859-4',
        'iso_8859-4:1988':     'iso-8859-4',
        'l4':                  'iso-8859-4',
        'latin4':              'iso-8859-4',
        'csisolatincyrillic':  'iso-8859-5',
        'cyrillic':            'iso-8859-5',
        'iso-8859-5':          'iso-8859-5',
        'iso-ir-144':          'iso-8859-5',
        'iso8859-5':           'iso-8859-5',
        'iso88595':            'iso-8859-5',
        'iso_8859-5':          'iso-8859-5',
        'iso_8859-5:1988':     'iso-8859-5',
        'arabic':              'iso-8859-6',
        'asmo-708':            'iso-8859-6',
        'csiso88596e':         'iso-8859-6',
        'csiso88596i':         'iso-8859-6',
        'csisolatinarabic':    'iso-8859-6',
        'ecma-114':            'iso-8859-6',
        'iso-8859-6':          'iso-8859-6',
        'iso-8859-6-e':        'iso-8859-6',
        'iso-8859-6-i':        'iso-8859-6',
        'iso-ir-127':          'iso-8859-6',
        'iso8859-6':           'iso-8859-6',
        'iso88596':            'iso-8859-6',
        'iso_8859-6':          'iso-8859-6',
        'iso_8859-6:1987':     'iso-8859-6',
        'csisolatingreek':     'iso-8859-7',
        'ecma-118':            'iso-8859-7',
        'elot_928':            'iso-8859-7',
        'greek':               'iso-8859-7',
        'greek8':              'iso-8859-7',
        'iso-8859-7':          'iso-8859-7',
        'iso-ir-126':          'iso-8859-7',
        'iso8859-7':           'iso-8859-7',
        'iso88597':            'iso-8859-7',
        'iso_8859-7':          'iso-8859-7',
        'iso_8859-7:1987':     'iso-8859-7',
        'sun_eu_greek':        'iso-8859-7',
        'csiso88598e':         'iso-8859-8',
        'csisolatinhebrew':    'iso-8859-8',
        'hebrew':              'iso-8859-8',
        'iso-8859-8':          'iso-8859-8',
        'iso-8859-8-e':        'iso-8859-8',
        'iso-ir-138':          'iso-8859-8',
        'iso8859-8':           'iso-8859-8',
        'iso88598':            'iso-8859-8',
        'iso_8859-8':          'iso-8859-8',
        'iso_8859-8:1988':     'iso-8859-8',
        'visual':              'iso-8859-8',
        'csiso88598i':         'iso-8859-8-i',
        'iso-8859-8-i':        'iso-8859-8-i',
        'logical':             'iso-8859-8-i',
        'csisolatin6':         'iso-8859-10',
        'iso-8859-10':         'iso-8859-10',
        'iso-ir-157':          'iso-8859-10',
        'iso8859-10':          'iso-8859-10',
        'iso885910':           'iso-8859-10',
        'l6':                  'iso-8859-10',
        'latin6':              'iso-8859-10',
        'iso-8859-13':         'iso-8859-13',
        'iso8859-13':          'iso-8859-13',
        'iso885913':           'iso-8859-13',
        'iso-8859-14':         'iso-8859-14',
        'iso8859-14':          'iso-8859-14',
        'iso885914':           'iso-8859-14',
        'csisolatin9':         'iso-8859-15',
        'iso-8859-15':         'iso-8859-15',
        'iso8859-15':          'iso-8859-15',
        'iso885915':           'iso-8859-15',
        'iso_8859-15':         'iso-8859-15',
        'l9':                  'iso-8859-15',
        'iso-8859-16':         'iso-8859-16',
        'cskoi8r':             'koi8-r',
        'koi':                 'koi8-r',
        'koi8':                'koi8-r',
        'koi8-r':              'koi8-r',
        'koi8_r':              'koi8-r',
        'koi8-u':              'koi8-u',
        'csmacintosh':         'macintosh',
        'mac':                 'macintosh',
        'macintosh':           'macintosh',
        'x-mac-roman':         'macintosh',
        'dos-874':             'windows-874',
        'iso-8859-11':         'windows-874',
        'iso8859-11':          'windows-874',
        'iso885911':           'windows-874',
        'tis-620':             'windows-874',
        'windows-874':         'windows-874',
        'cp1250':              'windows-1250',
        'windows-1250':        'windows-1250',
        'x-cp1250':            'windows-1250',
        'cp1251':              'windows-1251',
        'windows-1251':        'windows-1251',
        'x-cp1251':            'windows-1251',
        'ansi_x3.4-1968':      'windows-1252',
        'ascii':               'windows-1252',
        'cp1252':              'windows-1252',
        'cp819':               'windows-1252',
        'csisolatin1':         'windows-1252',
        'ibm819':              'windows-1252',
        'iso-8859-1':          'windows-1252',
        'iso-ir-100':          'windows-1252',
        'iso8859-1':           'windows-1252',
        'iso88591':            'windows-1252',
        'iso_8859-1':          'windows-1252',
        'iso_8859-1:1987':     'windows-1252',
        'l1':                  'windows-1252',
        'latin1':              'windows-1252',
        'us-ascii':            'windows-1252',
        'windows-1252':        'windows-1252',
        'x-cp1252':            'windows-1252',
        'cp1253':              'windows-1253',
        'windows-1253':        'windows-1253',
        'x-cp1253':            'windows-1253',
        'cp1254':              'windows-1254',
        'csisolatin5':         'windows-1254',
        'iso-8859-9':          'windows-1254',
        'iso-ir-148':          'windows-1254',
        'iso8859-9':           'windows-1254',
        'iso88599':            'windows-1254',
        'iso_8859-9':          'windows-1254',
        'iso_8859-9:1989':     'windows-1254',
        'l5':                  'windows-1254',
        'latin5':              'windows-1254',
        'windows-1254':        'windows-1254',
        'x-cp1254':            'windows-1254',
        'cp1255':              'windows-1255',
        'windows-1255':        'windows-1255',
        'x-cp1255':            'windows-1255',
        'cp1256':              'windows-1256',
        'windows-1256':        'windows-1256',
        'x-cp1256':            'windows-1256',
        'cp1257':              'windows-1257',
        'windows-1257':        'windows-1257',
        'x-cp1257':            'windows-1257',
        'cp1258':              'windows-1258',
        'windows-1258':        'windows-1258',
        'x-cp1258':            'windows-1258',
        'x-mac-cyrillic':      'mac-cyrillic',
        'x-mac-ukrainian':     'mac-cyrillic',
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
        'hz-gb-2312':          'hz-gb-2312',
        'big5':                'big5',
        'big5-hkscs':          'big5hkscs',
        'hkscs':               'big5hkscs',
        'cn-big5':             'big5',
        'csbig5':              'big5',
        'x-x-big5':            'big5',
        'cseucpkdfmtjapanese': 'euc-jp',
        'euc-jp':              'euc-jp',
        'x-euc-jp':            'euc-jp',
        'csiso2022jp':         'iso-2022-jp',
        'iso-2022-jp':         'iso-2022-jp',
        'csshiftjis':          'shift_jis',
        'ms_kanji':            'shift_jis',
        'shift-jis':           'shift_jis',
        'shift_jis':           'shift_jis',
        'sjis':                'shift_jis',
        'windows-31j':         'shift_jis',
        'x-sjis':              'shift_jis',
        'cseuckr':             'euc-kr',
        'csksc56011987':       'euc-kr',
        'euc-kr':              'euc-kr',
        'iso-ir-149':          'euc-kr',
        'korean':              'euc-kr',
        'ks_c_5601-1987':      'euc-kr',
        'ks_c_5601-1989':      'euc-kr',
        'ksc5601':             'euc-kr',
        'ksc_5601':            'euc-kr',
        'windows-949':         'euc-kr',
        'csiso2022kr':         'iso2022_kr',
        'iso-2022-cn':         'gb18030',
        'iso-2022-cn-ext':     'gb18030',
        'iso-2022-kr':         'iso2022_kr',
        'utf-16be':            'utf-16be',
        'utf-16':              'utf-16le',
        'utf-16le':            'utf-16le',
    }
