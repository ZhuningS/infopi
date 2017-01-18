# coding=utf-8

import datetime
import socket
import struct
import time

import xml.etree.ElementTree as ET

from worker_manage import worker, dataparser, c_worker_exception
from datadefine import *

__all__ = ()

# worker：ntp_check
# 通过NTP服务器获取网络时间，当系统误差超过指定秒数时报警

# NTP code from https://pypi.python.org/pypi/ntplib
# The MIT License


class NTPException(Exception):
    pass


class NTP:
    _SYSTEM_EPOCH = datetime.date(*time.gmtime(0)[0:3])
    """system epoch"""
    _NTP_EPOCH = datetime.date(1900, 1, 1)
    """NTP epoch"""
    NTP_DELTA = (_SYSTEM_EPOCH - _NTP_EPOCH).days * 24 * 3600
    """delta between system and NTP time"""


class NTPPacket:
    _PACKET_FORMAT = "!B B B b 11I"
    """packet format to pack/unpack"""

    def __init__(self, version=2, mode=3, tx_timestamp=0):
        """Constructor.

        Parameters:
        version      -- NTP version
        mode         -- packet mode (client, server)
        tx_timestamp -- packet transmit timestamp
        """
        self.leap = 0
        """leap second indicator"""
        self.version = version
        """version"""
        self.mode = mode
        """mode"""
        self.stratum = 0
        """stratum"""
        self.poll = 0
        """poll interval"""
        self.precision = 0
        """precision"""
        self.root_delay = 0
        """root delay"""
        self.root_dispersion = 0
        """root dispersion"""
        self.ref_id = 0
        """reference clock identifier"""
        self.ref_timestamp = 0
        """reference timestamp"""
        self.orig_timestamp = 0
        """originate timestamp"""
        self.recv_timestamp = 0
        """receive timestamp"""
        self.tx_timestamp = tx_timestamp
        """tansmit timestamp"""

    def to_data(self):
        """Convert this NTPPacket to a buffer that can be sent over a socket.

        Returns:
        buffer representing this packet

        Raises:
        NTPException -- in case of invalid field
        """
        _to_frac = lambda timestamp, n=32: int(
            abs(timestamp - int(timestamp)) * 2**n)
        try:
            packed = struct.pack(NTPPacket._PACKET_FORMAT,
                                 (self.leap << 6 |
                                  self.version << 3 | self.mode),
                                 self.stratum,
                                 self.poll,
                                 self.precision,
                                 int(self.root_delay) << 16 | _to_frac(
                                     self.root_delay, 16),
                                 int(self.root_dispersion) << 16 |
                                 _to_frac(self.root_dispersion, 16),
                                 self.ref_id,
                                 int(self.ref_timestamp),
                                 _to_frac(self.ref_timestamp),
                                 int(self.orig_timestamp),
                                 _to_frac(self.orig_timestamp),
                                 int(self.recv_timestamp),
                                 _to_frac(self.recv_timestamp),
                                 int(self.tx_timestamp),
                                 _to_frac(self.tx_timestamp))
        except struct.error:
            raise NTPException("Invalid NTP packet fields.")
        return packed

    def from_data(self, data):
        """Populate this instance from a NTP packet payload received from
        the network.

        Parameters:
        data -- buffer payload

        Raises:
        NTPException -- in case of invalid packet format
        """
        try:
            unpacked = struct.unpack(NTPPacket._PACKET_FORMAT,
                                     data[0:struct.calcsize(NTPPacket._PACKET_FORMAT)])
        except struct.error:
            raise NTPException("Invalid NTP packet.")

        _to_time = lambda integ, frac, n=32: integ + float(frac) / 2**n
        self.tx_timestamp = _to_time(unpacked[13], unpacked[14])

    @property
    def tx_time(self):
        """Transmit timestamp in system time."""
        return self.tx_timestamp - NTP.NTP_DELTA


class NTPClient:
    """NTP client session."""

    def request(self, host, version=2, port='ntp', timeout=5):
        """Query a NTP server.

        Parameters:
        host    -- server name/address
        version -- NTP version to use
        port    -- server port
        timeout -- timeout on socket operations

        Returns:
        NTPStats object
        """
        # lookup server address
        addrinfo = socket.getaddrinfo(host, port)[0]
        family, sockaddr = addrinfo[0], addrinfo[4]

        # create the socket
        s = socket.socket(family, socket.SOCK_DGRAM)

        try:
            s.settimeout(timeout)

            # create the request packet - mode 3 is client
            query_packet = NTPPacket(mode=3, version=version,
                                     tx_timestamp=time.time() + NTP.NTP_DELTA)

            # send the request
            s.sendto(query_packet.to_data(), sockaddr)

            # wait for the response - check the source address
            src_addr = None,
            while src_addr[0] != sockaddr[0]:
                response_packet, src_addr = s.recvfrom(256)

        except socket.timeout:
            raise NTPException("No response received from %s." % host)
        finally:
            s.close()

        # construct corresponding statistics
        stats = NTPPacket()
        stats.from_data(response_packet)

        return stats


@worker('ntp_check')
def ntp_check_worker(data_dict, worker_dict):
    c = NTPClient()

    try:
        info = c.request(data_dict['server'])
    except Exception as e:
        raise c_worker_exception('获取网络时间出错',
                                 summary=str(e) + '请检查NTP服务器是否失效')

    v = info.tx_time - time.time()
    if abs(v) > data_dict['seconds']:
        if v > 0:
            s = '系统时间 比 网络时间 慢了%.2f秒'
        else:
            s = '系统时间 比 网络时间 快了%.2f秒'

        raise c_worker_exception('系统时间的误差超过报警值',
                                 summary=s % abs(v))

    info = c_info()
    info.title = '系统时间在正常范围内'
    info.summary = '当前误差小于%.2f秒' % data_dict['seconds']
    info.suid = 'normal'

    return [info]


@dataparser('ntp_check')
def ntp_check_parser(xml_string):
    d = dict()
    data = ET.fromstring(xml_string).find('data')

    try:
        tag = data.find('server')
        d['server'] = tag.text.strip()
    except:
        print('ntp_check需要在data里指定server（NTP服务器地址）')
        raise

    try:
        tag = data.find('seconds')
        d['seconds'] = eval(tag.text.strip())
    except:
        print('ntp_check需要在data里指定seconds（报警误差秒数）')
        raise

    return d
