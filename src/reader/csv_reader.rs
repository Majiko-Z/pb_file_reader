use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{PathBuf};
use serde::{Deserialize};
use std::sync::atomic::Ordering;
use anyhow::{Result};
use encoding_rs_io::{DecodeReaderBytesBuilder};
use crate::utils::model::{*};
use crate::utils::timer::get_coarse_timestamp_ms;
use super::{subscribe_reader::*};

impl <T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> SubsReader<T, CSV> {

    // 启动
    fn read_file_loop(&self) {
        let is_running = self.is_running.clone();
        let file_path = self.file_path.clone();
        let is_increment = self.is_increment;
        let seek_pos = self.seek_pos.clone();
        let enc_type = self.enc_type;
        let dispatcher = self.msg_dispatcher.clone();

        let recv_signal_chan = self.notify_meta.receiver.clone();

        std::thread::spawn(move || {
            let mut _read_success = true; // 上次read是否成功
            let mut retry_time = 0; // 重试次数
            let mut _last_read_size = 0; // 上次读取字节数
            let mut _last_read_time = 0_u64; // 上次读取时间 (避免read间隔太频繁)
            let mut first_read = true; // 是否第一次读取

            while is_running.load(Ordering::Relaxed) {
                println!("csv read file loop");
                let cur_seek_pos = seek_pos.load(Ordering::Relaxed); // 当前文件seek位置
                let mut need_read_data = false;
                let mut cur_read_time = 0_u64;

                if !first_read && retry_time == 0 { // 首次读取/retry清空 -> 阻塞等待写入
                    match recv_signal_chan.recv() { // 阻塞等待事件
                        Ok(event_data) => {
                            match event_data.event {
                                NotifyEvent::ScheduleEvent | NotifyEvent::WriteEvent => {
                                    need_read_data = true;
                                    cur_read_time = event_data.last_notify_time;
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
                } else { // 重试    
                    need_read_data = true;
                    first_read = false; // 标记首次读取已完成
                }

                if need_read_data {
                    if cur_read_time  < _last_read_time + MIN_READ_INTERVAL { // 避免read间隔太频繁
                        std::thread::sleep( std::time::Duration::from_millis(1));
                    }
                    _last_read_time = get_coarse_timestamp_ms();

                    match read_csv_data::<T>(&file_path, cur_seek_pos, enc_type) {
                        Ok((new_seek_pos, datas, is_read_success)) => {
                            _read_success = is_read_success;
                            if new_seek_pos == cur_seek_pos ||
                                datas.is_empty() || (!is_read_success && retry_time < MAX_READ_RETRY_TIME) {
                                    retry_time += 1;
                                    if retry_time >= MAX_READ_RETRY_TIME {
                                        retry_time = 0; // 清空
                                    }
                                    println!("retry read csv file: {:?}", file_path.display());
                                    std::thread::sleep( std::time::Duration::from_millis(500));
                                    continue;
                                }
                            if is_increment { // 增量读; 需要更新POS 
                                _last_read_size = new_seek_pos - cur_seek_pos;
                                seek_pos.store(new_seek_pos, Ordering::Relaxed);
                            }
                            retry_time = 0;
                            let data_len = datas.len();
                            println!("read data len: {}", data_len);
                            match dispatcher.dispatch(datas) { // 分发数据
                                Ok(_) => {
                                    println!("dispatch success;len={}", data_len);
                                }
                                Err(e) => {
                                    println!("dispatch error: {:?}", e);
                                }
                            }

                        }
                        Err(e) => {
                            println!("read error: {:?}", e)
                        }
                    }
                }

            }
        });
    }

}


impl<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> ReadRunner for SubsReader<T, CSV> {
    fn run(&self) {
        self.read_file_loop(); // 调用 CSV 版本的具体实现
    }
}

// 从当前seek读取后续所有数据,并返回seek和数据,不改变记录的seek变量
pub fn read_csv_data<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static>(file_path: &PathBuf, seek_pos: u64, enc_type: EncType) -> anyhow::Result<(u64, Vec<anyhow::Result<T>>, bool)> {
    let ret_data : Vec<Result::<T>> = vec![];
    let read_success = true;

    let mut fd= File::open(file_path)?;
    fd.seek(SeekFrom::Start(seek_pos))?; // seek

    let (read_size, data) = match enc_type { // 1. 读数据并转为string
        EncType::GBK => {
            let mut buf = vec![];
            // 不能直接转gbk字节序列为utf8字节序列，会多增加一些字节，导致seek不对
            let size = fd.read_to_end(&mut buf)? as u64;
            let mut data = String::default();
            DecodeReaderBytesBuilder::new().encoding(Some(encoding_rs::GBK)).build(&buf[..]).read_to_string(&mut data)?;
            (size, data)
        }
        EncType::UTF8 => {
            let mut buf = String::default();
            let size = fd.read_to_string(&mut buf)? as u64;
            (size, buf)
        }
    };

    if read_size == 0 { // 无数据变化
        return Ok((seek_pos, ret_data, read_success))
    }

    let new_pos = seek_pos + read_size; // 读取后的位置数据

    match deserialize_from_str(&data, seek_pos == 0) {
        Ok((data, read_success)) => {
            Ok((new_pos, data, read_success))
        }

        Err(e) => {
            Ok((new_pos, vec![], false))
        }
    }
}

// 从正确格式字符串中解析构建csv reader; 读数据
fn deserialize_from_str<T: for<'a> Deserialize<'a> + Send + Sync + 'static>(data: &str, have_head: bool) -> anyhow::Result<(Vec<anyhow::Result<T>>, bool)> {
    let mut ret_data : Vec<Result::<T>> = vec![];
    let mut read_success = true;
    let mut reader = csv::ReaderBuilder::new().flexible(true).has_headers(have_head).from_reader(data.trim().as_bytes());

    for record in reader.records() {
        match record {
            Ok(record) =>  match record.deserialize::<T>(None) {
                Ok(val) => {
                    ret_data.push(Ok(val));
                }
                Err(e) => {
                    ret_data.push(Err(anyhow::anyhow!(
                        "Error decoding data: {:?}, error: {:?}",
                        record,
                        e
                    )));
                    read_success = false;
                }
            }
            Err(e) => {
                ret_data.push(Err(e.into()));
            }

        }
    }
    Ok((ret_data, read_success))
}

