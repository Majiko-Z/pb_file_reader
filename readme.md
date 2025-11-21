
# 金融PB文件单接入, 扫单文件读取底层优化

## 优化动机/背景

1. 现有情况, 每个文件开启一个线程读取(线程总数太多问题)
2. 线程读取时，loop + sleep循环读取, 尝试反序列化(CPU占用过高问题)
3. 同一PB不同账户, 对应同一扫单文件, 也会开启多个线程读取(线程数太多+CPU占用高)

## features

1. 通过事件驱动而非循环等待
2. 同一文件只有一个线程扫单读, 根据反序列化信息发给不同业务线程

## 逻辑层级

1. level 1

使用一个线程L监听所有注册的扫单文件变化(`[windows IOCP]`/`[linux inotif]`/`[common notify]`)

默认使用`common notify`, 跨平台

2. level 2

文件读取线程R,阻塞,直到收取到线程L的通知, 并附带重试机制

3. level 3 (业务实现)

业务循环线程, select等待线程R的数据并处理

## 说明

1. 线程数T与监听文件数F相关 `T = F + 1`
2. 提供给业务mpsc::bounded接口,以便业务曾可以select同时处理多个文件

## 使用方法

使用方法参考下方DEMO, 或参考[](src/test)下的test_*.rs

```rust
// CSV 文件使用 (DBF文件使用get_or_create_dbf_reader, remove_dbf_reader)
use pb_file_reader::reader::manager::{get_or_create_csv_reader, remove_csv_reader};
use pb_file_reader::utils::model::{EncType, CSV};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TestCsvStruct1 {
    trade_acc: String,
    b: i32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TestCsvStruct2 {
    trade_acc: String,
    b: i32,
}
let file_1 = PathBuf::from("file_1.csv");
let file_2 = PathBuf::from("file_2.csv");

let cur_tradeacc = "114514".to_string();

let is_crement = true;
let csv_reader_1 = get_or_create_csv_reader::<TestCsvStruct1>(&file_1, is_crement, EncType::UTF8);
let csv_reader_2 = get_or_create_csv_reader::<TestCsvStruct2>(&file_2, is_crement, EncType::UTF8);
if csv_reader_1.is_err() {
    // xxx
}
let reader1 = csv_reader_1.unwrap(); // [safe] not err
let reader2 = csv_reader_2.unwrap(); // [safe] not err

let (cert_key1, recv_chan1) = reader1.subscribe(, |cur_tradeacc, data| -> bool {
    data.tradeacc == cur_tradeacc // 这里根据结构选择过滤, 直接为true, 代表接收所有数据
});

let (cert_key2, recv_chan2) = reader2.subscribe(, |cur_tradeacc, data| -> bool {
    data.tradeacc == cur_tradeacc
});
let mut selector = crossbeam::channel::Select::new();
let chan1_idx = selector.recv(&recv_chan1);
let chan2_idx = selector.recv(&recv_chan2);

while true { // [退出条件]
    let select_idx = selector.select(); // 阻塞等待数据
    match select_idx.index() {
        i if i == chan1_idx {
            match select_idx.recv(&recv_chan1) {
                Ok(data_list) => {
                    for data in data_list {
                        if data.is_err() {
                            // 处理错误
                        }
                        // 处理数据
                    }
                }
                Err(err) => {/** 错误处理 */}
            }
        }
        i if i == chan2_idx {
            match select_idx.recv(&recv_chan2) {
                Ok(data_list) => {
                    for data in data_list {
                        if data.is_err() {
                            // 处理错误
                        }
                        // 处理数据
                    }
                }
                Err(err) => {/** 错误处理 */}
            }
        }
        _ => unreachable!()
    }
    
}
// 不再使用reader, 需提供key,扫单路径,是否增量读参数
remove_csv_reader(cert_key1, is_crement, &file_1); 
remove_csv_reader(cert_key2, is_crement, &file_2);
```
