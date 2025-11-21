
#[allow(unused_imports)]
mod test {
    use std::path::PathBuf;

    use crate::reader::manager::*;
    use crate::utils::model::EncType;
    use serde::{Deserialize, Serialize};
    use crate::utils::init_logger_for_test;
    #[allow(dead_code)]
    #[derive(Debug, Deserialize, Serialize, Clone)]
    struct TestCsvStruct1 {
        a: i32,
        b: i32,
        c: i64,
    }
    #[allow(dead_code)]
    #[derive(Debug, Deserialize, Serialize, Clone)]
    struct TestCsvStruct2 {
        a: i32,
        b: i32,
        c: i64,
    }

    #[test]
    fn test_single_thread_csv_manager() {
        // 测试单线程收取数据
        init_logger_for_test();
        let path = PathBuf::from("/Users/yaohui/projects/pb_file_reader/test/1.csv");
        let mut reader = get_or_create_csv_reader::<TestCsvStruct1>(&path, true, EncType::UTF8);
        match reader {
            Ok(mut reader) => {
                let (cert_key, recv_chan) = reader.subscribe("0", |v, data| {
                    true
                });
                let mut cnt = 1;
                loop {
                    cnt += 1;
                    match recv_chan.recv() {
                        Ok(data) => {
                            println!("recv len {:?}", data.len());
                        }
                        Err(err) => {
                            println!("err:{:?}", err);
                        }
                    }
                    if cnt > 10 {
                        break;
                    }
                }

            }
            Err(err) => {
                print!("err:{:?}", err);
            }
        }
    }

    #[test]
    fn test_multiple_csv_manager() {
        //  开多个线程接收同一CSV的数据
        init_logger_for_test();
        let path_1 = PathBuf::from("/Users/yaohui/projects/pb_file_reader/test/1.csv");
        let path_1_c1 = path_1.clone();
        let path_1_c2 = path_1.clone();

        let h1 = std::thread::spawn(move || {
            let mut cnt = 1;
            let reader = get_or_create_csv_reader::<TestCsvStruct1>(&path_1, true, EncType::UTF8);
            if reader.is_err() {
                return;
            }
            let reader = reader.unwrap(); 
            let (cert_key, recv_chan) = reader.subscribe("0", |verify, data| -> bool {
                (data.a % 2).to_string() == verify
            });
            println!("get cert key: {}", cert_key);
            while cnt <= 10 {
                match recv_chan.recv() {
                    Ok(data) => {
                        println!("h1:time={};===> len={}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(), data.len());
                        cnt += 1;
                    },
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
            remove_csv_reader::<TestCsvStruct1>(cert_key, true, &path_1);
        });

        let h2 = std::thread::spawn(move || {
            let mut cnt = 1;
            let path_1 = path_1_c1;
            let reader = get_or_create_csv_reader::<TestCsvStruct1>(&path_1, true, EncType::UTF8);
            if reader.is_err() {
                return;
            }
            let reader = reader.unwrap(); 
            let (cert_key, recv_chan) = reader.subscribe("1", |verify, data| -> bool {
                (data.a % 2).to_string() == verify
            });
            println!("get cert key: {}", cert_key);
            while cnt <= 10 {
                match recv_chan.recv() {
                    Ok(data) => {
                        println!("h2:time={};===> len={}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(), data.len());
                        cnt += 1;
                    },
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
            remove_csv_reader::<TestCsvStruct1>(cert_key, true, &path_1);
        });
        let h3 = std::thread::spawn(move || {
            let mut cnt = 1;
            let path_1 = path_1_c2;
            let reader = get_or_create_csv_reader::<TestCsvStruct1>(&path_1, true, EncType::UTF8);
            if reader.is_err() {
                return;
            }
            let reader = reader.unwrap(); 
            let (cert_key, recv_chan) = reader.subscribe("1", |verify, data| -> bool {
                true
            });
            println!("get cert key: {}", cert_key);
            while cnt <= 10 {
                match recv_chan.recv() {
                    Ok(data) => {
                        println!("h3:time={};===> len={}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(), data.len());
                        cnt += 1;
                    },
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
            remove_csv_reader::<TestCsvStruct1>(cert_key, true, &path_1);
        });
        let _ = h1.join();
        let _ = h2.join();
        let _ = h3.join();
    }

    #[test]
    fn test_multile_csv_reader() {
        init_logger_for_test();
        let file_1 = PathBuf::from("/Users/yaohui/projects/pb_file_reader/test/1.csv");
        let file_2 = PathBuf::from("/Users/yaohui/projects/pb_file_reader/test/2.csv");

        let verify_1 = "0";
        let verify_2 = "1";

        let is_crement = true;
        let csv_reader_1 = get_or_create_csv_reader::<TestCsvStruct1>(&file_1, is_crement, EncType::UTF8);
        let csv_reader_2 = get_or_create_csv_reader::<TestCsvStruct2>(&file_2, is_crement, EncType::UTF8);
        if csv_reader_1.is_err() {
            // 错误处理
        }

        if csv_reader_2.is_err() {
            // 错误处理
        }

        let reader1 = csv_reader_1.unwrap(); // [safe] not err
        let reader2 = csv_reader_2.unwrap(); // [safe] not err

        let (cert_key1, recv_chan1) = reader1.subscribe(verify_1, |verify_1, data| -> bool {
            (data.a % 2).to_string() == verify_1.to_string() // 这里根据结构选择过滤, 直接为true, 代表接收所有数据
        });

        let (cert_key2, recv_chan2) = reader2.subscribe(verify_2, |verify_2, data| -> bool {
            (data.a % 2).to_string() == verify_2.to_string() // 这里根据结构选择过滤, 直接为true, 代表接收所有数据
        });
        let mut selector = crossbeam::channel::Select::new();
        let chan1_idx = selector.recv(&recv_chan1);
        let chan2_idx = selector.recv(&recv_chan2);

        while true { // [退出条件]
            let select_idx = selector.select(); // 阻塞等待数据
            match select_idx.index() {
                i if i == chan1_idx =>{
                    match select_idx.recv(&recv_chan1) {
                        Ok(data_list) => {
                            println!("chan1, recv_len={:?}", data_list.len());
                            for data in data_list {
                                if data.is_err() {
                                    // 处理错误
                                }
                                // 处理数据
                                // let data = data.unwrap(); // [safe]

                            }
                        }
                        Err(err) => {/* 错误处理 */}
                    }
                }
                i if i == chan2_idx => {
                    match select_idx.recv(&recv_chan2) {
                        Ok(data_list) => {
                            println!("chan2, recv_len={:?}", data_list.len());
                            for data in data_list {
                                if data.is_err() {
                                    // 处理错误
                                }
                                // 处理数据
                            }
                        }
                        Err(err) => {/* 错误处理 */}
                    }
                }
                _ => unreachable!()
            }
            
        }
        // 不再使用reader, 需提供key,扫单路径,是否增量读参数
        remove_csv_reader::<TestCsvStruct1>(cert_key1, is_crement, &file_1); 
        remove_csv_reader::<TestCsvStruct2>(cert_key2, is_crement, &file_2);
    }
}