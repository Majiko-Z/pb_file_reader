import csv, time
import datetime
"""
向一个文件中持续写入数据
"""
file_name = "/Users/yaohui/projects/pb_file_reader/test/data.csv"


if __name__ == "__main__":
    # 首次运行时清空文件并写入表头
    with open(file_name, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["a", "b", "c"])  # 写入表头
        
    for i in range(1, 1145141919):
        with open(file_name, "a", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([i, i * i, i * i * i])
            print("第{}条数据;当前时间:{}".format(i, datetime.datetime.now()))
            time.sleep(10)