import dbf
import time
import random
import os
from datetime import datetime

# --- 配置参数（保持小数值+纯英文/数字）---
DBF_FILENAME = '/Users/yaohui/projects/pb_file_reader/test/data.dbf'
# c字段：N(11,6)（5位整数+6位小数，小数值适配）
FIELD_SPECS = 'a N(10,0);b C(50);c N(13,3);'  
POLLING_INTERVAL_SECONDS = 10
TOTAL_RECORDS_TO_WRITE = 1000

def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        print(f"Directory '{directory}' does not exist, creating...")
        os.makedirs(directory, exist_ok=True)
        print(f"Directory '{directory}' created successfully")

def create_or_append_dbf(filename, field_specs, new_data):
    ensure_directory_exists(filename)
    existing_records = []

    if os.path.exists(filename):
        print(f"File '{filename}' exists, reading existing records...")
        try:
            old_table = dbf.Table(filename)
            old_table.open(mode=dbf.READ_ONLY)
            existing_records = [tuple(record) for record in old_table]
            old_table.close()
            print(f"Successfully read {len(existing_records)} existing records")
        except Exception as e:
            print(f"Failed to read old records: {str(e)}, creating new file")
            existing_records = []

    # 内存表：仅通过 field_specs 定义字段，库自动识别类型
    table = dbf.Table(
        filename='temp',
        field_specs=field_specs,
        on_disk=False,
    )
    table.open(mode=dbf.READ_WRITE)

    # 写入新旧记录
    for old_record in existing_records:
        table.append(old_record)
    table.append(new_data)
    print(f"Successfully added new record: {new_data} (Total records: {len(table)})")

    try:
        # 核心修复：删除多余的 default_data_types 参数（避免类型名称错误）
        physical_table = table.new(
            filename=filename,
            # 去掉 default_data_types，库自动根据 field_specs 识别字段类型
        )
        with physical_table:
            for record in table:
                physical_table.append(record)
        print(f"File '{filename}' saved to disk")
    except Exception as e:
        print(f"Failed to write to disk: {str(e)}")
        raise
    finally:
        table.close()
        print(f"In-memory table closed")

def generate_test_data(record_number):
    integer_a = min(record_number, 9999999999)  # a字段10位整数
    base_str = f"Record_{record_number}_generated_at_{datetime.now().strftime('%H%M%S')}"
    string_b = base_str[:50]  # b字段纯英文/数字
    float_c = round(random.uniform(0.0, 99999.999999), 6)  # c字段小数值（0-99999.999999）
    return (integer_a, string_b, float_c)

def main():
    print(f"Start writing data to '{DBF_FILENAME}' periodically...")
    print(f"Interval: {POLLING_INTERVAL_SECONDS}s | Total records to write: {TOTAL_RECORDS_TO_WRITE}")
    print(f"Fields: {FIELD_SPECS.strip(';').replace(';', ' | ')} | Data type: Small number + English only")
    print("Press Ctrl+C to exit early")
    print("-" * 70)

    record_counter = 1
    try:
        while record_counter <= TOTAL_RECORDS_TO_WRITE:
            test_data = generate_test_data(record_counter)
            create_or_append_dbf(DBF_FILENAME, FIELD_SPECS, test_data)

            if record_counter < TOTAL_RECORDS_TO_WRITE:
                print(f"\nWaiting {POLLING_INTERVAL_SECONDS}s for next record...")
                time.sleep(POLLING_INTERVAL_SECONDS)

            record_counter += 1
            print("-" * 50)
    except KeyboardInterrupt:
        print("\n" + "-" * 70)
        print(f"Program interrupted by user | Written records: {record_counter - 1}")
    except Exception as e:
        print("\n" + "-" * 70)
        print(f"Task failed: {str(e)} | Written records: {record_counter - 1}")
    finally:
        print(f"\nTask ended | Target file: {DBF_FILENAME}")

if __name__ == '__main__':
    main()