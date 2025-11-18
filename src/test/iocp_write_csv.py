import csv, time
import datetime
"""
向一个文件中持续写入数据
"""
file_name = "E:\\projects\\file_reader\\test\\data.csv"

if __name__ == "__main__":
    for i in range(1, 1145141919):
        with open(file_name, "a") as f:
            writer = csv.writer(f)
            writer.writerow([i, i * i, i * i * i])
            print("第{}条数据;当前时间:{}".format(i, datetime.datetime.now()))
            time.sleep(10)