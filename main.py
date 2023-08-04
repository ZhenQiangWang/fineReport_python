import sys

import cx_Oracle
import datetime
import log
import common as com
import htrb_common as htrb

'''
    计算器件小时数
    days:发货日期跟查询日期相差天数
    dif_days:计算减去默认天数,前端参数
    fhrq:发货日期
    fhsl:发货数量
    curr_sxsl:当前查询的失效数量
    total_sxsl:该主键下总失效数量
    sxrq:失效日期
    total_bad:当前主键下历史失效小时数
    计算器件小时数的前提：
        1. 假设产品发货后90天开始进入终端应用
        2. 假设产品每天工作运行12小时
    查询时间 - 出货日期 < 90，不计算
'''


if __name__ == '__main__':
    try:
        db = com.connect_database('oracle', 'nx_mes', 'nx_mes', '192.168.68.60', '1521', 'orcl', 'UTF-8')
        curs = db.cursor()
        argv_ = sys.argv[1:]
        queryTime_str = argv_[0]  # 查询时间
        # queryTime_str = '2022-12-05'  # 查询时间
        queryTime = datetime.datetime.strptime(queryTime_str, "%Y-%m-%d")

        dif_days, yzsh, confidence_level, early_fail_qty = htrb.get_config(curs)
        result_list = []
        '''
            1.查询当前查询时间内的主键
            2.根据主键一一查询计算出数据
              2.1 只有一笔数据，则直接计算
              2.2 存在多笔数据，需循环计算
        '''
        disSqlStr = "select distinct CHDH,PH,XH from  htrbdata where SHRQ <= to_date(:V1,'yyyy-mm-dd')"
        dis = curs.execute(disSqlStr, ({"V1": queryTime_str})).fetchall()
        for tup in dis:
            chdh = tup[0]  # 出货单号
            ph = tup[1]  # 批号
            xh = tup[2]  # 型号
            sqlStr = "select * from htrbdata where CHDH = :V1 AND PH = :V2 AND XH = :V3 ORDER BY ID"
            lists = curs.execute(sqlStr, ({'V1': chdh, 'V2': ph, 'V3': xh})).fetchall()  # 查询当前主键下的值
            total_sxsl = 0
            total_bad = 0
            for val in lists:
                id = val[0]  # ID
                fhsl = val[7]  # 发货数量
                curr_sxsl = val[9]  # 失效数量
                fhrq = val[8]  # 发货日期
                sxrq = val[10]  # 失效日期
                total_sxsl += curr_sxsl
                days = (queryTime - fhrq).days  # 当前日期减去发货日期
                if days - dif_days >= 0:
                    bad, qjxsh = htrb.calculate_qjxsh(days, dif_days, fhrq, fhsl, curr_sxsl, total_sxsl, sxrq,
                                                 total_bad, yzsh)  # 计算器件小时数
                    total_bad += bad
                    result = [id, val[1], val[2], val[3], val[4], val[5], val[6],
                              fhsl, str(fhrq), curr_sxsl, "" if sxrq is None else str(sxrq), qjxsh]
                    result_list.append(result)
        result_list = sorted(result_list, key=lambda x: x[8], reverse=True)
        com.send_value(result_list)
        com.disconnect(curs)
    except Exception as ex:
        log.error(ex)
