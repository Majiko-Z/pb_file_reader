import csv, time
import datetime
import os

"""
向多个文件中持续写入数据
"""
file_template = "/Users/yaohui/projects/pb_file_reader/test/{}.csv"
sleep_secs = 0.001 # 10ms写入一份数据

def write_to_csv(file_id):
    """向指定ID的CSV文件写入数据"""
    file_name = file_template.format(file_id)
    
    # 首次运行时清空文件并写入表头
    with open(file_name, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["a", "b", "c"])  # 写入表头
    
    # 循环写入数据
    for i in range(1, 1145141919):
        with open(file_name, "a", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([i, i * i, i * i * i])
            print("文件{}: 第{}条数据;当前时间:{}".format(file_id, i, datetime.datetime.now()))
            time.sleep(sleep_secs)

if __name__ == "__main__":
    # 创建多个线程分别写入不同文件
    import threading
    
    # 定义要创建的文件数量
    num_files = 20
    
    threads = []
    for file_id in range(1, num_files + 1):
        thread = threading.Thread(target=write_to_csv, args=(file_id,))
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()