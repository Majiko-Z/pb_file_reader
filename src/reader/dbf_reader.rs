use super::subscribe_reader::ReadRunner;
use crate::utils::model::{DBF, NotifyEvent};
use crate::reader::subscribe_reader::SubsReader;

impl <T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static + Clone> SubsReader<T, DBF> {
    pub fn read_file_loop(&self) {
        let is_running = self.is_running.clone();
        let file_path = self.file_path.clone();
        let is_increment = self.is_increment;
        let seek_pos = self.seek_pos.clone();
        let _enc_type = self.enc_type;
        let dispatcher = self.msg_dispatcher.clone();

        let recv_signal_chan = self.notify_meta.receiver.clone();

        std::thread::spawn(move || {
            let mut _read_success = true; // 上次read是否成功
            let mut _last_read_size = 0; // 上次读取字节数
            let mut _last_read_time = 0_u64; // 上次读取时间 (避免read间隔太频繁)
            let mut first_read = true; // 是否第一次读取
            while is_running.load(std::sync::atomic::Ordering::Relaxed) {
                
                if !first_read {
                    match recv_signal_chan.recv() { // 阻塞等待事件
                        Ok(event_data) => {
                            match event_data.event {
                                NotifyEvent::ScheduleEvent | NotifyEvent::WriteEvent => {
                                    // 触发读取逻辑
                                }
                                NotifyEvent::StopEvent => {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            println!("recv signal error: {:?}", e);
                        }
                    }
                } else {
                    first_read = false;
                }

                match dbase::Reader::from_path(&file_path) {
                    Ok(mut reader) => {
                        if is_increment {
                            // 防止后面内存指令排序到该指令前面
                            // 这里seek指record数量
                            let _= reader.seek(seek_pos.load(std::sync::atomic::Ordering::Acquire) as _);
                        }

                        let res = reader
                            .iter_records_as::<T>()
                            .map(|e| match e {
                                Ok(r) => Ok(r),
                                Err(e) => Err(anyhow::anyhow!("{:?}", e)),
                            })
                            .collect::<Vec<_>>();
                        let length = res.len();
                        if is_increment {
                            seek_pos.fetch_add(length as _, std::sync::atomic::Ordering::Acquire);
                        }
                        if length > 0 {
                            match dispatcher.dispatch(res) {
                                Ok(_) => {},
                                Err(e) => {}
                            }
                        }
                    },
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
        });
    }
}

impl<T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static + Clone> ReadRunner for SubsReader<T, DBF> {
    fn run(&self) {
        self.read_file_loop(); // 调用 DBF 版本的具体实现
    }
}