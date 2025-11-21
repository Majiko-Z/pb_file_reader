
mod test {
    use std::path::PathBuf;

    use crate::reader::manager::*;
    use crate::utils::model::EncType;
    use serde::{Deserialize, Serialize};
    use crate::utils::init_logger_for_test;
    #[derive(Debug, Deserialize, Serialize, Clone)]
    struct TestCsvStruct1 {
        a: i32,
        b: i32,
        c: i64,
    }

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

}