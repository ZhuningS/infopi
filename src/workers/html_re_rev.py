# coding=utf-8
from worker_manage import worker, dataparser 

from .html_re import download_process, html_re_parser

# get a reversed list from html_re
# this worker rely on html_re.py

__all__ = ()

@worker('html_re_rev')
def rev_worker(data_dict, worker_dict):
    lst = download_process(data_dict, worker_dict)
    return lst[::-1]

@dataparser('html_re_rev')
def rev_parser(xml_string):
    return html_re_parser(xml_string)
