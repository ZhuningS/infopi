# coding=utf-8


# source_id -> source
sources = dict()

# worker_id -> <tuple>
# <tuple>: (worker_function, worker_dict)
workers = dict()

# worker_id -> parser_function
dataparsers = dict()


back_web_queue = None
bb_queue = None

#gcfg = None