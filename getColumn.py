# @Author   :zhenqiang.wang
# @Time     :2023/7/13 9:41
#
import common as com
import sys
import log

if __name__ == '__main__':
    # argv = sys.argv
    # log.info("获取列名----{}".format(argv[1]))
    # print("获取列名")
    # table_name = argv[1]
    table_name = "htrb_chart1"
    db = com.connect_database('oracle', 'nx_mes', 'nx_mes', '192.168.68.60', '1521', 'orcl', 'UTF-8')
    curs = db.cursor()
    sel_sql = "SELECT COLUMN_NAME FROM finereport where TEMPLATE_NAME = :V1  ORDER BY ID"
    dis = curs.execute(sel_sql, ({"V1": table_name})).fetchall()
    result = [[t[0]] for t in dis]
    com.send_value(result)
    curs.close()