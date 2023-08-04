"""
画图逻辑计算
"""
import datetime
import common as com
from dateutil.relativedelta import *
import htrb_common as htrb
import calendar
import sys
import log


def assembly_str(params, column, sql):
    if params is not ' ':
        param_list = params.split("@")
        str = " AND "+column+" IN ("
        for param in param_list:
            str += "'"
            str += param
            str += "',"
        str = str.strip(",'")
        str += "') "
        sql += str
    return sql

def get_baseInfo(curs, str_query_time, orders, lot_ids, spec_types, zptdms, mptdms, types):
    # sql = "select * from htrb_basedata where SHRQ <= to_date(:V0,'YYYY-mm-dd') order by SHRQ"
    sql = "select * from htrb_basedata where SHRQ <= to_date(:V0,'YYYY-mm-dd')"
    if orders is not ' ':
        sql = assembly_str(orders, "CHDH", sql)
    if lot_ids is not ' ':
        sql = assembly_str(lot_ids, "PH", sql)
    if spec_types is not ' ':
        sql = assembly_str(spec_types, "XH", sql)
    if zptdms is not ' ':
        sql = assembly_str(zptdms, "ZPTDM", sql)
    if mptdms is not ' ':
        sql = assembly_str(mptdms, "MPTDM", sql)
    if types is not ' ':
        sql = assembly_str(types, "LX", sql)

    sql += "order by SHRQ"
    return curs.execute(sql, ({"V0": str_query_time})).fetchall()


def get_failInfo(curs, base_id, curr_month):
    sql = "select * from htrb_failInfo where BASE_ID = :V0  AND TO_DATE(TO_CHAR(SXRQ,'YYYY-MM-DD'),'YYYY-MM-DD')<= TO_DATE(:V1,'YYYY-MM-DD')"
    return curs.execute(sql, ({"V0": base_id, "V1": curr_month})).fetchall()


# 获取卡方值
def get_kf_val(curs, confidence_level, qty):
    kf_sql = "select VAL from htrb_kf where CONFIDENCE_LEVEL = :V0 and SXSL = :V1"
    dis = curs.execute(kf_sql, ({"V0": confidence_level, "V1": qty})).fetchall()
    if len(dis) == 0:
        raise Exception("卡方查询失败,confidence_level={},qty={}".format(str(confidence_level), str(qty)))
    x2 = dis[0][0]  # 卡方数值
    return x2


# 计算FIT跟ELFR
def calculate(curs, confidence_level, total_time, total_curr_month_fail_qty, total_curr_month_early_fail_qty):
    # 计算每月FIT
    fit_x2 = get_kf_val(curs, confidence_level, total_curr_month_fail_qty)  # 查询的差方值
    fit_per_fail = fit_x2 / total_time  # 失效率
    fit_val = fit_per_fail * 1000000000  # FIT

    # elfr
    # elfr_x2 = get_kf_val(curs, confidence_level, total_curr_month_early_fail_qty)  # 查询的差方值
    elfr_per_fail = total_curr_month_early_fail_qty / total_time  # 失效率
    elfr_flt_val = elfr_per_fail * 1000000000
    elfr_val = elfr_flt_val * 4.38  # 0.001*356*24/2  FIT
    return fit_val, elfr_val


def assembly_by_type(total_qjxsh_id, index):
    fit_list = []
    for month, list in total_qjxsh_id.items():  # 每个月
        print("month:{}".format(month))
        # 产品型号:[器件小时数,失效数量,早期失效数量]
        spec_type_qjxsh = {}
        for list_id in list:
            for id, attr in list_id.items():
                qjxsh = attr[8]
                type = attr[index]
                curr_month_fail_qty = attr[6]
                curr_month_early_fail_qty = attr[7]
                if type in spec_type_qjxsh:
                    spec_type_qjxsh[type][0] = spec_type_qjxsh[type][0] + qjxsh
                    spec_type_qjxsh[type][1] = spec_type_qjxsh[type][1] + curr_month_fail_qty
                    spec_type_qjxsh[type][2] = spec_type_qjxsh[type][2] + curr_month_early_fail_qty
                else:
                    spec_type_qjxsh[type] = [qjxsh, curr_month_fail_qty, curr_month_early_fail_qty]
        for type, info in spec_type_qjxsh.items():
            fit_val, elfr_val = calculate(curs, confidence_level, info[0], info[1], info[2])
            fit = [type, month, fit_val, elfr_val]
            fit_list.append(fit)
    return fit_list


if __name__ == '__main__':
    try:
        # argv = sys.argv
        # queryTime_str = '2022-12-6'  # 查询时间
        # query_type = "0"
        argv = ['D:\\HTRB\\htrb_chart2.py', ' ', ' ', ' ', ' ', 'MC1A1.1_002@RF1A4.1@RF1A4.2@RF1A6.1@RF1A4.3', ' ', ' ', '2020-01-30']
        # log.error("脚本接收参数数量:" + str(len(argv)))
        log.debug("脚本接收参数:" + str(argv))
        query_type = argv[1]
        orders = argv[2]
        lot_ids = argv[3]
        spec_types = argv[4]
        zptdms = argv[5]
        mptdms = argv[6]
        types = argv[7]
        queryTime_str = argv[8]
        db = com.connect_database('oracle', 'nx_mes', 'nx_mes', '192.168.68.60', '1521', 'orcl', 'UTF-8')
        curs = db.cursor()
        '''
            dif_days:最小相差天数
            yzsh：运行时间
            early_fail_qty：失效率
        '''
        start = datetime.datetime.now()
        print("start:{}".format(start))
        dif_days, yzsh, confidence_level, early_fail_diff_days = htrb.get_config(curs)
        result_lists = []
        start_date = htrb.get_mintime_by_queryTime(curs, queryTime_str)  # 最小时间,用于统计每月FIT
        end_data = datetime.datetime.strptime(queryTime_str, '%Y-%m-%d')
        diff_year = end_data.year - start_date.year
        diff_month = diff_year * 12 + end_data.month - start_date.month  # 计算相差月份
        curr_month = start_date  # 计算至当前月份
        str_curr_month = str(start_date.year) + "-" + f"{start_date.month:02d}"  # 计算至当前月份
        '''
            1、查询指定时间前发货数据
            2、计算每个月各个ID的器件小时数[{年月份1:[{ID1:小时数},{ID2:小时数}...]},{年月份2:[{ID1:小时数},{ID2:小时数}...]}...]
        '''
        total_qjxsh_id = {}
        base_infos = get_baseInfo(curs, queryTime_str, orders, lot_ids, spec_types, zptdms, mptdms, types)
        for i in range(0, diff_month + 1):
            # 查询当月最后一天(2021-31)，以此计算出当月的器件小时数
            str_last_day_of_month = str(curr_month.year) + '-' + str(curr_month.month) + '-' + calendar.month(
                curr_month.year, curr_month.month)[-3:].strip()
            if i == diff_month:
                str_last_day_of_month = queryTime_str
            last_day_of_month = datetime.datetime.strptime(str_last_day_of_month, '%Y-%m-%d')
            # dist_curr_qjxsh = {}
            list_curr_qjxsh_id = []  # 该ID当月器件小时数

            curr_month_total_fail_qty = 0  # 当月失效总数量
            curr_month_total_early_fail_qty = 0  # 当月早期失效总数量
            for base_info in base_infos:
                sales_date = base_info[8]  # 发货日期
                if sales_date > last_day_of_month or (end_data - sales_date).days < 90:  # 当月无发货
                    continue

                id = base_info[0]
                order = base_info[1]
                lot_id = base_info[2]
                spec_type = base_info[3]
                zptdm = base_info[4]
                mptdm = base_info[5]
                type = base_info[6]
                sales_qty = base_info[7]  # 发货数量
                # 良品当月器件小时数 = (当月最后一天-发货日期) * (发货数量-失效数量)
                days = (last_day_of_month - sales_date).days - dif_days
                if days <= 0:
                    continue
                # 查找出该ID这个月失效数据
                list_fail_info = get_failInfo(curs, id, str_last_day_of_month)
                total_fail_qty = 0  # 该ID在该月的失效总数量
                total_early_fail_qty = 0  # 该ID在该月的早期失效总数量
                total_fail_time = 0  # 该ID在当月总失效小时数
                for fail_info in list_fail_info:
                    base_id = fail_info[0]
                    fail_id = fail_info[1]
                    fail_qty = fail_info[2]  # 失效数量
                    fail_date = fail_info[3]  # 失效日期
                    early_fail_qty = 0  # 早期失效数量
                    # 当前失效小时数 = 失效日期-发货日期 * 失效数量
                    fail_time = ((fail_date - sales_date).days - dif_days) * fail_qty
                    total_fail_qty += fail_qty
                    total_fail_time += fail_time
                    if (fail_date - sales_date).days-dif_days <= early_fail_diff_days:
                        early_fail_qty = fail_qty
                    total_early_fail_qty += early_fail_qty

                good_qjxsh = days * (sales_qty - total_fail_qty)
                qjxsh = (good_qjxsh + total_fail_time) * yzsh
                # [{年月份1: [{ID1: [属性新增+器件小时数]}, {ID1: [属性新增+器件小时数]}}, {年月份2: [{ID1: 小时数}, {ID2: 小时数}...]}...]
                dist_qjxsh = {id: [order, lot_id, spec_type, zptdm, mptdm, type, total_fail_qty, total_early_fail_qty,
                                   qjxsh]}  # 这个ID 当月的器件小时数
                list_curr_qjxsh_id.append(dist_qjxsh)
                curr_month_total_fail_qty += total_fail_qty
                curr_month_total_early_fail_qty += total_early_fail_qty

            if len(list_curr_qjxsh_id) > 0:
                total_qjxsh_id[str_curr_month] = list_curr_qjxsh_id
            curr_month = (datetime.datetime.strptime(str_curr_month, '%Y-%m') + relativedelta(months=+1))
            str_curr_month = str(curr_month.year) + "-" + f"{curr_month.month:02d}"

        # 返回值类型[系列[用户前端选择],分类(月份),flt值, elfr值]
        # 每个月器件小时总数，[出货/销退单号, 批号, 型号, 子平台代码, 母平台代码, 类型,当月失效总数,当月早期失效总数,器件小时数]
        fit_list = []
        if query_type is '0':
            # by出货/销退单号查询[0]
            fit_list = assembly_by_type(total_qjxsh_id, 0)
        elif query_type is '1':
            # by批号查询[1]
            fit_list = assembly_by_type(total_qjxsh_id, 1)
        elif query_type is '2':
            # by产品型号查询[2]
            fit_list = assembly_by_type(total_qjxsh_id, 2)
        elif query_type is '3':
            # by子平台查询[3]
            fit_list = assembly_by_type(total_qjxsh_id, 3)
        elif query_type is '4':
            # by母平台查询[4]
            fit_list = assembly_by_type(total_qjxsh_id, 4)
        elif query_type is '5':
            # by类型查询[5]
            fit_list = assembly_by_type(total_qjxsh_id, 5)
        else:
            # by总时间查询
            for month, list in total_qjxsh_id.items():  # 每个月
                time = []
                # 当月器件小时数
                total_time = 0
                # 当月失效总数
                total_curr_month_fail_qty = 0
                # 当月早期失效数
                total_curr_month_early_fail_qty = 0

                for list_id in list:
                    for id, attr in list_id.items():
                        qjxsh = attr[8]
                        curr_month_fail_qty = attr[6]
                        curr_month_early_fail_qty = attr[7]
                        total_time += qjxsh
                        total_curr_month_fail_qty += curr_month_fail_qty
                        total_curr_month_early_fail_qty += curr_month_early_fail_qty
                fit_val, elfr_val = calculate(curs, confidence_level, total_time, total_curr_month_fail_qty,
                                              total_curr_month_early_fail_qty)
                fit = ["", month, fit_val, elfr_val]
                fit_list.append(fit)

        com.send_value(fit_list)
        print("end:{},耗时:{}".format(datetime.datetime.now(), datetime.datetime.now() - start))
        curs.close()
    except Exception as ex:
        log.error(str(ex))
        raise Exception(ex)
