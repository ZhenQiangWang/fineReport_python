import pandas as pd
import common as com


if __name__ == '__main__':
    excl = pd.read_csv("C://Users//zhenqiang.wang//Desktop//kf.csv", header=None)
    shape = excl.shape  # 维度
    line_size = shape[0]  # 行数
    vertical_size = shape[1]  # 列数
    line_index, vertical_index = 1, 0
    insert_data_sql = "INSERT INTO htrb_kf values(:V0,:V1,:V2)"
    database_list = []
    db = com.connect_database('oracle', 'nx_mes', 'nx_mes', '192.168.68.60', '1521', 'orcl', 'UTF-8')
    curs = db.cursor()
    while vertical_index < vertical_size-1:
        while line_index < line_size:
            value = excl.iloc[line_index][vertical_index]
            database_map = {"v0": (vertical_index*0.1 + 0.1), "v1": (line_index - 1), "v2": value}
            database_list.append(database_map)
            line_index = line_index + 1
        vertical_index = vertical_index + 1
        line_index = 0
    curs.executemany(insert_data_sql, database_list)
    db.commit()
    db.close()
print("123")
