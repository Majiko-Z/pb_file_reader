use std::path::PathBuf;
use anyhow::bail;
use super::subscribe_reader::{ReadRunner};
use crate::common::model::{DBF, NotifyEvent, READ_FROM_HEAD_FLAG};
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
            let notify_idx = selector.recv(&recv_notify_signal_chan);
            let read_idx = selector.recv(&recv_read_signal_chan);

            while is_running.load(std::sync::atomic::Ordering::Relaxed) {
                
                let select_idx = selector.select();
                match select_idx.index() {
                    i if i == notify_idx => {
                        // 文件在监听之前有数据,这种情况处理在其他地方完成
                        match select_idx.recv(&recv_notify_signal_chan) { // 阻塞等待事件
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

                        ::ftlog::trace!("ready reading file: {}", file_path.display());
                        
                        let begin_seek = if is_increment {
                            seek_pos.load(std::sync::atomic::Ordering::Acquire)
                        } else {
                            0
                        };
                        match read_from_seek::<T>(&file_path, begin_seek as _) {
                            Ok(data) => {
                                let length = data.len();
                                if is_increment {
                                    seek_pos.fetch_add(length as _, std::sync::atomic::Ordering::Acquire);
                                }
                                if length > 0 {
                                    match dispatcher.dispatch(data) {
                                        Ok(_) => {
                                            println!("send success")
                                        },
                                        Err(e) => {
                                            println!("send data error: {:?}", e)
                                        }
                                    }
                                }

                            }
                            Err(e) => {
                                println!("{:?}", e);
                            }
                        }
                    
                    },
                    i if i == read_idx => {
                        match select_idx.recv(&recv_read_signal_chan) { // 接受该事件
                            Ok((cert_key, _seek_pos)) => {
                                if _seek_pos == READ_FROM_HEAD_FLAG { // 从0到文件尾部
                                    let begin_seek = if is_increment {
                                        seek_pos.load(std::sync::atomic::Ordering::Acquire)
                                    } else {
                                        0
                                    };
                                    match read_from_seek::<T>(&file_path, begin_seek as _) {
                                        Ok(data) => { // read 成功
                                            let length = data.len();
                                            if is_increment {
                                                seek_pos.fetch_add(length as _, std::sync::atomic::Ordering::Acquire);
                                            }
                                            if length > 0 {
                                                match dispatcher.dispatch(data) {
                                                    Ok(_) => {
                                                        ::ftlog::info!("{} read {} data success", file_path.display(), length)
                                                    },
                                                    Err(e) => {
                                                        ::ftlog::error!("send data error: {:?}", e)
                                                    }
                                                }
                                            }

                                        }
                                        Err(e) => {
                                            ::ftlog::error!("{:?}", e);
                                            #[cfg(feature = "reset_seek_when_err")] {
                                                ::ftlog::info!("{} retry read error. reset seek pos", file_path.display());
                                                seek_pos.store(0, std::sync::atomic::Ordering::Release);
                                            }
                                        }
                                    }
                    
                                } else {
                                    let from_zero_data = read_to_seek::<T>(&file_path, _seek_pos);
                                    if from_zero_data.is_err() {
                                        // 读失败则不尝试重试
                                        continue;
                                    }
                                    let from_zero_data = from_zero_data.unwrap(); // [safe] not err
                                    let _= dispatcher.dispatch_single(from_zero_data, cert_key);
                                }

                            }
                            Err(e) => {
                                ::ftlog::error!("recv signal error: {:?}", e);
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

fn read_from_seek<T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static + Clone>(file_path:&PathBuf, begin_seek: u64) -> anyhow::Result<Vec<anyhow::Result<T>>> {
    match dbase::Reader::from_path(&file_path) {
        Ok(mut reader) => {
            let _= reader.seek(begin_seek as _);

            let res = reader
                .iter_records_as::<T>()
                .map(|e| match e {
                    Ok(r) => Ok(r),
                    Err(e) => Err(anyhow::anyhow!("{:?}", e)),
                })
                .collect::<Vec<_>>();
            Ok(res)
        },
        Err(e) => {
            bail!("{}", e)
        }
    }
}