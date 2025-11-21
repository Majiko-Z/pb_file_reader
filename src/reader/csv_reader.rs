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
use crossbeam::channel::Select;

impl <T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> SubsReader<T, CSV> {

    // 启动
    fn read_file_loop(&self) {
        let is_running = self.is_running.clone();
        let file_path = self.file_path.clone();
        let is_increment = self.is_increment;
        let seek_pos = self.seek_pos.clone();
        let enc_type = self.enc_type;
        let dispatcher = self.msg_dispatcher.clone();

        let recv_notify_signal_chan = self.notify_meta.receiver.clone();
        let recv_read_signal_chan = self.inner_chan.1.clone();
        
        std::thread::spawn(move || {
            ::ftlog::info!("{} csv_reader thread start", file_path.display());
            let mut _last_read_size = 0; // 上次读取字节数
            let mut _last_read_time = 0_u64; // 上次读取时间 (避免read间隔太频繁)
            let mut selector = Select::new();
            let notify_idx = selector.recv(&recv_notify_signal_chan);
            let read_idx = selector.recv(&recv_read_signal_chan);

            while is_running.load(Ordering::Relaxed) {
                
                let select_idx = selector.select();

                match select_idx.index() {
                   /* 文件变动事件 */ 
                   i if i == notify_idx => {
                        // 文件变动通知
                        let mut need_read_data = false;
                        let mut cur_read_time = 0_u64;
                        match select_idx.recv(&recv_notify_signal_chan) { // 接受该事件
                            Ok(event_data) => {
                                match event_data.event {
                                    NotifyEvent::ScheduleEvent | NotifyEvent::WriteEvent => {
                                        need_read_data = true;
                                        cur_read_time = event_data.last_notify_time;
                                        ::ftlog::debug!("csv_reader:{}; recv notify event", file_path.display())
                                    }
                                    NotifyEvent::StopEvent => {
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                ::ftlog::error!("recv signal error: {:?}", e);
                            }
                        }
                        
                        let cur_seek_pos = seek_pos.load(Ordering::Relaxed); // 当前文件seek位置
                        if !need_read_data {
                            continue;
                        }

                        if cur_read_time  < _last_read_time + MIN_READ_INTERVAL { // 避免read间隔太频繁
                            ::ftlog::debug!("sleep 1ms for read");
                            std::thread::sleep( std::time::Duration::from_millis(1));
                        }
                        _last_read_time = get_coarse_timestamp_ms();

                        match retry_read_from_seek::<T>(&file_path, cur_seek_pos, enc_type, MAX_READ_RETRY_TIME) {
                            Ok((new_seek_pos, datas, is_read_success)) => {
                                if !is_read_success {
                                    ::ftlog::info!("{} retry read error.skip;", file_path.display());
                                    continue;
                                }
                                if is_increment { // 增量读; 需要更新POS 
                                    _last_read_size = new_seek_pos - cur_seek_pos;
                                    seek_pos.store(new_seek_pos, Ordering::Relaxed);
                                }
                                let data_len = datas.len();
                                ::ftlog::info!("read data len: {}", data_len);
                                match dispatcher.dispatch(datas) { // 分发数据
                                    Ok(_) => {
                                        ::ftlog::debug!("dispatch success;len={}", data_len);
                                    }
                                    Err(e) => {
                                        ::ftlog::error!("dispatch error: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                ::ftlog::error!("read error: {:?}", e);
                            }
                        }
                    
                    }
                   /* 读请求事件 */ 
                   i if i == read_idx => {
                        match select_idx.recv(&recv_read_signal_chan) { // 接受该事件
                            Ok((cert_key, _seek_pos)) => {
                                if _seek_pos == READ_FROM_HEAD_FLAG { // 用MAX代表
                                    ::ftlog::info!("{} read from head", file_path.display());
                                    let cur_seek_pos = seek_pos.load(Ordering::Relaxed); // 当前文件seek位置;
                                    match retry_read_from_seek::<T>(&file_path, cur_seek_pos, enc_type, MAX_READ_RETRY_TIME) {
                                        Ok((new_seek_pos, datas, is_read_success)) => {
                                            if !is_read_success {
                                                ::ftlog::error!("{} read error", file_path.display());
                                                continue;
                                            }
                                            if is_increment { // 增量读; 需要更新POS 
                                                _last_read_size = new_seek_pos - cur_seek_pos;
                                                seek_pos.store(new_seek_pos, Ordering::Relaxed);
                                            }
                                            let data_len = datas.len();
                                            ::ftlog::info!("read data len: {}", data_len);
                                            match dispatcher.dispatch(datas) { // 分发数据
                                                Ok(_) => {
                                                    ::ftlog::debug!("dispatch success;len={}", data_len);
                                                }
                                                Err(e) => {
                                                    ::ftlog::error!("dispatch error: {:?}", e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            ::ftlog::error!("read error: {:?}", e);
                                        }
                                    }
                    
                                }
                                else {
                                    ::ftlog::info!("{} read from 0 to {}", file_path.display(), _seek_pos);
                                    let from_zero_data = read_to_seek::<T>(&file_path, _seek_pos, enc_type);
                                    if from_zero_data.is_err() {
                                        // 读失败则不尝试重试
                                        ::ftlog::error!("read error: {:?}", from_zero_data.err());
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

fn retry_read_from_seek<T: for<'a> Deserialize<'a> + Send + Sync + Clone +'static>(file_path: &PathBuf, seek_pos: u64, enc_type: EncType, retry_times:i32) -> anyhow::Result<(u64, Vec<anyhow::Result<T>>, bool)>  {
    let mut _read_success = false;
    let mut retry_time = 0;
    while !_read_success {
        match read_csv_data::<T>(&file_path, seek_pos, enc_type) {
            Ok((new_seek_pos, datas, is_read_success)) => {
                _read_success = is_read_success;
                if new_seek_pos == seek_pos ||
                    datas.is_empty() || (!is_read_success && retry_time < retry_times) {
                        retry_time += 1;
                        if retry_time >= retry_times {
                            break; // 跳出内层循环, 放弃本次读取
                        }
                        ::ftlog::info!("retry read csv file: {:?}", file_path.display());
                        std::thread::sleep( std::time::Duration::from_millis(500));
                        continue;
                    }
                return Ok((new_seek_pos, datas, is_read_success));

            }
            Err(e) => {
                ::ftlog::error!("read error: {:?}", e)
            }
        }
    }
    Ok((0, vec![], false)) // 读取失败
}

impl<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> ReadRunner for SubsReader<T, CSV> {
    fn run(&self) {
        self.read_file_loop(); // 调用 CSV 版本的具体实现
    }
}

// 从当前seek读取后续所有数据,并返回seek和数据,不改变记录的seek变量
fn read_csv_data<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static>(file_path: &PathBuf, seek_pos: u64, enc_type: EncType) -> anyhow::Result<(u64, Vec<anyhow::Result<T>>, bool)> {
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

// 新增函数：从0位置读取到指定位置的数据
fn read_csv_data_to_position<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static>(
    file_path: &PathBuf, 
    start_pos: u64, 
    end_pos: u64, 
    enc_type: EncType
) -> anyhow::Result<Vec<anyhow::Result<T>>> {    
    let mut fd = File::open(file_path)?;
    fd.seek(SeekFrom::Start(start_pos))?; // 从起始位置seek
    
    let read_size = end_pos - start_pos; // 计算需要读取的字节数
    
    let data = match enc_type { // 读取指定长度的数据并转为string
        EncType::GBK => {
            let mut buf = vec![0; read_size as usize];
            let size = fd.read(&mut buf)? as u64;
            let mut data = String::default();
            DecodeReaderBytesBuilder::new()
                .encoding(Some(encoding_rs::GBK))
                .build(&buf[..size as usize])
                .read_to_string(&mut data)?;
            data
        }
        EncType::UTF8 => {
            let mut buf = vec![0; read_size as usize];
            let size = fd.read(&mut buf)? as u64;
            String::from_utf8_lossy(&buf[..size as usize]).to_string()
        }
    };

    if data.is_empty() { // 无数据变化
        return Ok(vec![]);
    }

    match deserialize_from_str(&data, start_pos == 0) {
        Ok((data, _read_success)) => {
            Ok(data)
        }
        Err(e) => {
            Ok(vec![Err(e)])
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


fn read_to_seek<T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static + Clone>(file_path: &PathBuf, seek_pos: u64, enc_type: EncType) -> anyhow::Result<Vec<anyhow::Result<T>>> {
    // 调用 CSV 读取逻辑
    read_csv_data_to_position::<T>(file_path, 0, seek_pos, enc_type)
}