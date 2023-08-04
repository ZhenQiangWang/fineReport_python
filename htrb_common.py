import datetime

'''
configs_[0]: 相差最小天数
configs_[1]: 产品每天工作时间
configs_[2]: CONFIDENCE_LEVEL
configs_[3]: 早期失效数
'''


def get_config(curs):
    config_sql = "SELECT * FROM htrb_config"
    configs = curs.execute(config_sql).fetchall()
    configs_ = configs[0]
    return configs_[0], configs_[1], configs_[2], configs_[3]


'''
    计算器件小时数
'''


def calculate_qjxsh(days, dif_days, fhrq, fhsl, curr_sxsl, total_sxsl, sxrq, total_bad, yzsh):
    good = (days - dif_days) * (fhsl - total_sxsl)
    bad = 0
    if sxrq is None:
        pass
    else:
        bad = ((sxrq - fhrq).days - dif_days) * curr_sxsl
    qjxsh = (good + bad + total_bad) * yzsh  # 器件小时数
    return bad, qjxsh


'''
计算每个月器件小时总数
curs：数据库连接
dif_days：相差天数
queryTime：查询时间
yzsh:产品每天工作运行时间
'''


def calculate_total_qjxsh(curs, dif_days, queryTime_str, yzsh):
    queryTime = datetime.datetime.strptime(queryTime_str, "%Y-%m-%d")
    disSqlStr = "select distinct CHDH,PH,XH FROM  htrbdata where SHRQ <= to_date(:V1,'yyyy-mm-dd')"
    dis = curs.execute(disSqlStr, ({"V1": queryTime_str})).fetchall()
    qjxsh_dist = {}
    for tup in dis:
        chdh = tup[0]  # 出货单号
        ph = tup[1]  # 批号
        xh = tup[2]  # 型号
        sqlStr = "select * from htrbdata where CHDH = :V1 AND PH = :V2 AND XH = :V3 ORDER BY ID"
        lists = curs.execute(sqlStr, ({'V1': chdh, 'V2': ph, 'V3': xh})).fetchall()  # 查询当前主键下的值
        total_sxsl = 0
        total_bad = 0
        for val in lists:
            fhsl = val[7]  # 发货数量
            curr_sxsl = val[9]  # 失效数量
            fhrq = val[8]  # 发货日期
            sxrq = val[10]  # 失效日期
            total_sxsl += curr_sxsl
            days = (queryTime - fhrq).days  # 当前日期减去发货日期
            if days - dif_days >= 0:
                bad, qjxsh = calculate_qjxsh(days, dif_days, fhrq, fhsl, curr_sxsl, total_sxsl, sxrq,
                                             total_bad, yzsh)  # 计算器件小时数
                total_bad += bad
                key = str(fhrq.year) + '-' + f"{fhrq.month:02d}"
                if key in qjxsh_dist:
                    qjxsh_dist[key] = qjxsh_dist[key] + qjxsh
                else:
                    qjxsh_dist[key] = qjxsh
            else:
                pass
    return qjxsh_dist


# 获取当前查询时间前的最小时间
def get_mintime_by_queryTime(curs, queryTime_str):
    sel_sql = "SELECT MIN(SHRQ) FROM htrb_basedata where SHRQ <= to_date(:V1,'yyyy-mm-dd')"
    dis = curs.execute(sel_sql, ({"V1": queryTime_str})).fetchall()
    return dis[0][0]



