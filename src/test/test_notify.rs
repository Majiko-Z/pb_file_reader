

mod test {
    use crate::notify::GLOBAL_LISTENER;
    use crate::utils::model::*;
    use std::path::PathBuf;

    #[test]
    fn test_listen() {
        println!("begin");
        let path_str = "/Users/yaohui/projects/pb_file_reader/test/data.csv";
        let mut file_path = PathBuf::new();
        file_path.push(path_str);
        match GLOBAL_LISTENER.add_watch(file_path) {
            Ok(meta) => {
                println!("begin listen");
                let mut count = 0; // 添加计数器
                while count <= 10086 {
                    count += 1; // 增加计数
                    
                    // 使用标准库转换时间戳
                    
                    match meta.receiver.recv() {
                        Ok(event) => {
                            println!("{:?}", event);
                        }
                        Err(e) => {
                            println!("{:?}", e);
                        }
                    }
                    println!("{:?}: 第{}次recv", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(), count); // 打印日期时间
                }
            }
            Err(e) => {
                print!("err: {:?}", e)
            }
        }
    }

}
