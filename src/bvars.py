# coding=utf-8
import os

# source_id -> source
sources = dict()

# worker_id -> <tuple>
# <tuple>: (worker_function, worker_dict)
workers = dict()

# worker_id -> parser_function
dataparsers = dict()

cfg_token = 0

back_web_queue = None
bb_queue = None

#gcfg = None

root_path = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.dirname(root_path)