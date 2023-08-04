import common as com
import pandas as pd

if __name__ == '__main__':
    db = com.connect_database('oracle', 'nx_mes', 'nx_mes', '192.168.68.60', '1521', 'orcl', 'UTF-8')
    curs = db.cursor()
    database_list = []
    insert_sql = '''
    insert into htrb_basedata (CHDH, PH, XH, ZPTDM, MPTDM, LX, FHSL, SHRQ)
    values (:v0, :v1, :v2, :v3, :v4, :v5, :v6, to_date(:v7, 'yyyy-mm-dd'))
    '''

    file_path = 'C:\\Users\\zhenqiang.wang\\Desktop\\sc.xls'
    excel = pd.read_excel(file_path)
    fillna = excel.iloc[:, 0:10].fillna('NA')
    shape = fillna.shape
    line_size = shape[0]  # 行数
    vertical_size = shape[1]  # 列数
    line_index, line_index1 = 0, 1
    while line_index < line_size:
        line_info = fillna[line_index:line_index1]
        CHDH = line_info.iloc[0][0]
        PH = line_info.iloc[0][1]
        XH = line_info.iloc[0][2]
        ZPTDM = line_info.iloc[0][3]
        MPTDM = line_info.iloc[0][4]
        LX = line_info.iloc[0][5]
        FHSL = int(line_info.iloc[0][6])
        SHRQ = str(line_info.iloc[0][7].year) + "-" + str(line_info.iloc[0][7].month) + "-" + str(
            line_info.iloc[0][7].day)
        sxsl = int(line_info.iloc[0][8])
        line_index += 1
        line_index1 += 1
        if sxsl > 0:
            print("失效数量大于0，跳出")
            continue
        database_map = {"v0": CHDH, "v1": PH, "v2": XH,
                        "v3": ZPTDM, "v4": MPTDM, "v5": LX, "v6": FHSL, "v7": SHRQ}
        database_list.append(database_map)
    executemany = curs.executemany(insert_sql, database_list)
    db.commit()
    curs.close()
