# coding=utf-8

class c_info:
    __slots__ = ('id', 'source_id',
                 'title', 'url',
                 'author', 'summary', 'pub_date',
                 'suid', 'fetch_date', 'temp')
    
    def __init__(self):
        self.id = -1
        self.source_id = ''
        self.suid = ''
        self.fetch_date = 0
        
        self.title = ''
        self.url = ''

        self.author = ''
        self.summary = ''
        self.pub_date = ''

        self.temp = 0

    def __lt__(self, other):
        if self.fetch_date > other.fetch_date:
            return True
        elif self.fetch_date == other.fetch_date and self.id > other.id:
            return True
        else:
            return False

    def __str__(self):
        def make_str(name, attr):
            if attr:
                return name + ': ' + attr + '\n'
            else:
                return ''

        lst = ( ('title', self.title),
                ('url', self.url),
                ('author', self.author),
                ('summary', self.summary),
                ('pub_date', self.pub_date),
                ('suid', self.suid),
               )

        return ''.join(make_str(*i) for i in lst) + '\n'
        

class c_message:
    __slots__ = ('command', 'token', 'data')

    def __init__(self, command, token=0, data=None):
        self.command = command
        self.token = token
        self.data = data

    @staticmethod
    def make(sendto, command, token=0, data=None):
        m = c_message(command, token, data)
        sendto.put(m)
