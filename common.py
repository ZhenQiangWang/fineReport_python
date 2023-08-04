import json
import log
import cx_Oracle
import MySQLdb

# 传给帆软后台数据列名称
def send_param(params):
    try:
        # 判断数据类型
        judge_type(params)
        # 数组转JSON, 通过输出流传给帆软后台
        param_json = json.dumps(params, ensure_ascii=False)
        log.param(param_json)
    except Exception as ex:
        log.error(ex)


# 传给帆软后台实际查询数据
def send_value(value):
    try:
        judge_type(value)
        param_json = json.dumps(value, ensure_ascii=False)
        log.value(param_json)
    except Exception as ex:
        log.error(ex)


# 判断数据类型,需为list
def judge_type(params):
    try:
        param_type = type(params)
        if param_type is list:
            return 1
        else:
            raise Exception("参数类型需为list,当前数据类型为{},信息无法传至报表".format(param_type))
    except Exception as ex:
        log.error(ex)


'''
连接数据库
type:数据库类型
userId:用户名
password:密码
url:IP
str_port:端口
database_name:数据名称
str_encoding:编码
'''
def connect_database(type, userId, password, url, str_port, database_name, str_encoding):
    if type.strip().upper() == 'ORACLE':
        db = cx_Oracle.connect(userId, password, url+':' + str(str_port) + '/'+database_name, encoding=str_encoding)
        return db
    elif type.strip().upper() == 'MYSQL':
        db = MySQLdb.connect(url, userId, password, database_name, port=str_port, charset=str_encoding)
        return db.cursor()
        # sql = "SELECT * from bibb_file_remark"
        # curs.execute(sql)
        # fetchall = curs.fetchall()
        # print(fetchall)


def disconnect(curs):
    curs.close()