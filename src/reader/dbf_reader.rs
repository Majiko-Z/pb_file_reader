use std::path::PathBuf;

use anyhow::bail;
use dbase::read;

use super::subscribe_reader::{ReadRunner};
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

        let recv_notify_signal_chan = self.notify_meta.receiver.clone();
        let recv_read_signal_chan = self.inner_chan.1.clone();

        std::thread::spawn(move || {
            let mut _read_success = true; // 上次read是否成功
            let mut _last_read_size = 0; // 上次读取字节数
            let mut _last_read_time = 0_u64; // 上次读取时间 (避免read间隔太频繁)

            let mut selector = crossbeam::channel::Select::new();
            let notify_idx = selector.recv(&recv_read_signal_chan);
            let read_idx = selector.recv(&recv_read_signal_chan);

            while is_running.load(std::sync::atomic::Ordering::Relaxed) {
                
                let select_idx = selector.select();
                match select_idx.index() {
                    i if i == notify_idx => {
                        // 文件在监听之前有数据,这种情况处理在其他地方完成
                        match recv_notify_signal_chan.recv() { // 阻塞等待事件
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

                        println!("ready reading file: {}", file_path.display());
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
                                println!("{} records read", length);
                                if is_increment {
                                    seek_pos.fetch_add(length as _, std::sync::atomic::Ordering::Acquire);
                                }
                                if length > 0 {
                                    match dispatcher.dispatch(res) {
                                        Ok(_) => {
                                            println!("send success")
                                        },
                                        Err(e) => {
                                            println!("send data error: {:?}", e)
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                println!("{:?}", e);
                            }
                        }
                    },
                    i if i == read_idx => {
                        match recv_read_signal_chan.recv() { // 接受该事件
                            Ok((cert_key, seek_pos)) => {
                                let from_zero_data = read_to_seek::<T>(&file_path, seek_pos);
                                if from_zero_data.is_err() {
                                    // 读失败则不尝试重试
                                    continue;
                                }
                                let from_zero_data = from_zero_data.unwrap(); // [safe] not err
                                let _= dispatcher.dispatch_single(from_zero_data, cert_key);
                            }
                            Err(e) => {
                                println!("recv signal error: {:?}", e);
                            }
                        }
                    }
                    _ => unreachable!()
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

pub fn read_to_seek<T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static + Clone>(file_path:&PathBuf, seek_pos: u64) -> anyhow::Result<Vec<anyhow::Result<T>>> {
    // 调用 DBF 读取逻辑
    match dbase::Reader::from_path(file_path) {
        Ok(mut reader) => {
            let _ = reader.seek(0);
            let res = reader
                .iter_records_as::<T>()
                .take(seek_pos as _) // 读取前N条
                .map(|e| match e {
                    Ok(r) => Ok(r),
                    Err(e) => Err(anyhow::anyhow!("{:?}", e)),
                })
                .collect::<Vec<_>>();

            return Ok(res);
        }
        Err(e) => {
            bail!("{}", e)
        }
    }
}