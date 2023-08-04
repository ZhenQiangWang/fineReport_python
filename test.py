import os.path

import openpyxl


def generate_text_file(file_path):
    # 打开文件并写入内容
    with open(file_path, 'w') as file:
        file.write('This is a text file.')


def generate_excel_file(file_path):
    # 创建工作簿和工作表
    workbook = openpyxl.Workbook()
    worksheet = workbook.active

    # 在工作表中写入数据
    worksheet['A1'] = 'Hello'
    worksheet['B1'] = 'World'

    # 保存工作簿为 Excel 文件
    workbook.save(file_path)


if __name__ == '__main__':
    file_path = 'D:\\新建文件夹\\source1\\新建文件夹'
    count = 0
    while count <= 1:
        # 指定文件路径
        text_file_path = os.path.join(file_path, str(count+1) + 'A.txt')

        # 调用函数生成文本文件
        generate_text_file(text_file_path)

        # 指定文件路径
        excel_file_path = os.path.join(file_path, str(count) + '.xlsx')

        # 调用函数生成 Excel 文件
        generate_excel_file(excel_file_path)
        count += 1
