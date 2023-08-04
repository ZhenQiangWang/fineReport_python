"""
画图逻辑计算
"""
import datetime
import cx_Oracle
import log
import common as com
from dateutil.relativedelta import *
import htrb_common as htrb
import sys

if __name__ == '__main__':
    # try:
        # argv_ = sys.argv[1:]
        # queryTime_str = argv_[0]  # 查询时间
        curs = com.connect_database('oracle', 'nx_mes', 'nx_mes', '192.168.68.60', '1521', 'orcl', 'UTF-8')
        dif_days, yzsh, confidence_level, early_fail_qty = htrb.get_config(curs)
        result_lists = []
        queryTime_str = '2022-12-05'  # 查询时间
        start_date = htrb.get_mintime_by_queryTime(curs, queryTime_str)  # 最小时间,用于统计每月FIT
        end_data = datetime.datetime.strptime(queryTime_str, '%Y-%m-%d')
        diff_year = end_data.year - start_date.year
        diff_month = diff_year * 12 + end_data.month - start_date.month  # 计算相差月份
        curr_month = str(start_date.year) + "-" + f"{start_date.month:02d}"  # 计算至当前月份

        qjxsh_dist = htrb.calculate_total_qjxsh(curs, dif_days, queryTime_str, yzsh)  # 小于等于查询时间内的每个月器件小时数
        for i in range(1, diff_month):
            # 查询当前月份数据
            if curr_month in qjxsh_dist:
                total_qjxsh = qjxsh_dist[curr_month]  # 当月器件小时总数
                total_sxsl_sql = "select SUM(SXSL) total_sxsl from htrbdata where to_char(SHRQ,'YYYY-MM') = :V0 group by SHRQ"
                total_sxsl_result = curs.execute(total_sxsl_sql, ({"V0": curr_month})).fetchall()
                total_sxsl = 0 if len(total_sxsl_result) == 0 else total_sxsl_result[0][0]  # 失效数量
                # 查询FLT卡方计算结果表
                flt_x2 = htrb.get_kf_val(curs, confidence_level, total_sxsl)
                flt_sxl = flt_x2 / total_qjxsh  # FLT失效率
                flt = flt_sxl * 1000000000
                # 查询ELFR卡方计算结果表
                elfr_x2 = htrb.get_kf_val(curs, confidence_level, early_fail_qty)
                elfr_sxl = early_fail_qty / total_qjxsh  # ELFR失效率
                elfr_flt_time = elfr_sxl * 1000000000
                elfr = elfr_flt_time * 0.001*365*24/2
                result_list = [curr_month, flt, elfr]
                result_lists.append(result_list)
            months_ = (datetime.datetime.strptime(curr_month, '%Y-%m') + relativedelta(months=+1))
            curr_month = str(months_.year) + "-" + f"{months_.month:02d}"
        com.send_value(result_lists)
        curs.close()
        db.close()
    # except Exception as ex:
    #     log.error(ex)
