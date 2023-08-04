# 通过输出流传给帆软后台
def info(log):
    print("LOG@INFO:{}".format(log))


def error(log):
    print("LOG@ERROR:{}".format(log))


def debug(log):
    print("LOG@DEBUG:{}".format(log))


def param(params):
    print("PARAM@{}".format(params))


def value(query_value):
    print("VALUE@{}".format(query_value))
