use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use dashmap::DashMap;
use serde::{Deserialize};
use serde::de::DeserializeOwned;   
use std::fs::File;
use std::sync::atomic::{AtomicBool};
use crossbeam::channel::{bounded,Receiver, Sender};
use super::msg_dispatcher::{MsgDispatcher, CertKeyT};
use crate::notify::GLOBAL_LISTENER;
use crate::common::model::{*};
use anyhow::{Result, bail};

pub type CsvReader<T> = SubsReader<T, CSV>;

pub type DbfReader<T> = SubsReader<T, DBF>;

pub trait ReadRunner {
    fn run(& self);
}

pub struct SubsReader<T: DeserializeOwned + Send + Sync + Clone + 'static,  F: FileType> {
    pub file_path: PathBuf, // 文件路径
    pub is_increment: bool, // 是否增量读
    pub seek_pos:  Arc<AtomicU64>, // 文件seek位置(byte_offset/record_offset)
    pub enc_type: EncType, // 编码类型
    pub fd: Option<File>, // 文件句柄
    pub msg_dispatcher:Arc<MsgDispatcher<T>>, // 消息分发器
    pub is_running: Arc<AtomicBool>, // 控制扫单线程运行
    pub notify_meta: NotifyMeta,
    pub inner_chan: (Sender<(CertKeyT, u64)>, Receiver<(CertKeyT, u64)>), // 内部通信通道
    register_before_pos: DashMap<CertKeyT, u64>, // 记录具体chan注册时的文件大小
    read_from_head: Arc<AtomicBool>, // 是否 已经文件头开始读过
    _phantom: std::marker::PhantomData<F>, // 占位防止编译出错
}


impl<T: DeserializeOwned + Send + Sync + Clone + 'static, F: FileType> SubsReader<T, F> {
    pub fn new(file_path: PathBuf, is_increment: bool, enc: EncType) -> Result<Self> {
        let notify_meta = GLOBAL_LISTENER.add_watch(file_path.clone())?;
        ::ftlog::info!("[INIT_READER];FILE_TYPE={},INCREMENT={},FILE_PATH={},ENC_TYPE={}", F::file_type(), is_increment, file_path.display(), enc);
        Ok(Self {
            file_path,
            is_increment,
            seek_pos: Arc::new(AtomicU64::new(0)),
            enc_type: enc,
            fd: None,
            msg_dispatcher: Arc::new(MsgDispatcher::new()),
            is_running: Arc::new(AtomicBool::new(false)),
            notify_meta,
            inner_chan: bounded(4), // 第一次register时读取
            read_from_head: Arc::new(AtomicBool::new(false)),
            register_before_pos: DashMap::new(),
            _phantom: std::marker::PhantomData,
        })
    }

    /// 重置文件读取位置
    pub fn reset_seek_pos(&self) { // 严格同步
        self.seek_pos.store(0, Ordering::Release)
    }

    /// 订阅 返回一个cert和chan
    pub fn subscribe(&self, verify_data: &str, dispatcher_func: fn(&str, &T) -> bool) -> (CertKeyT, Receiver<Vec<Result<T>>>)
    where
        Self: ReadRunner,
    {
        // self.reset_seek_pos(); // 无需重置位置;通过其他逻辑单独弥补
        let (send_chan, recv_chan) = bounded(16);
        // 获取锁并调用方法
        let cert_key = self.msg_dispatcher.get_cert_and_subscribe(verify_data, dispatcher_func, send_chan); // 需要在获取文件seek前执行
        let current_pos = self.seek_pos.load(Ordering::Relaxed); // 记录当前位置(需要弥补数据)
        if self.is_running.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            ::ftlog::info!("start reader loop");
            self.run(); // 第一次运行,则启动扫单线程
        }
        self.register_before_pos.insert(cert_key, current_pos); // 存储用于后续查询
        #[cfg(feature = "before_register_data")]
        {   // 弥补注册之前数据,使其可以增量读
            if self.read_from_head.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                ::ftlog::info!("read from zero seek pos");
                self.read_from_head.store(true, Ordering::Relaxed); // reader仅会从头读一次
                let _ = self.inner_chan.0.send((cert_key, READ_FROM_HEAD_FLAG)); // 从头开始读到文件尾部
            } else {
                /*
                NOTE:获取seek前, 通信chan已经插入, READ_FROM_HEAD_FLAG会将数据同步给所有chan
                */
                ::ftlog::info!("read from last seek pos for new register");
                if current_pos != 0 { 
                    let _ = self.inner_chan.0.send((cert_key, current_pos)); // 从头读到当前文件位置
                } // 如果为0, 代表reader在处理READ_FROM_HEAD_FLAG, 会通过READ_FROM_HEAD_FLAG受到数据
                
            }
            
        }
        
        (cert_key, recv_chan)
    }


    /// 取消订阅
    pub fn unsubscribe(&self, cert_key: CertKeyT) -> anyhow::Result<()> {
        {
            self.msg_dispatcher.unsubscribe(cert_key);
        }

        let no_subscriber = {
            self.msg_dispatcher.no_subscriber()
        };

        if no_subscriber { //无人订阅, 则停止扫单
            self.is_running.store(false, Ordering::SeqCst);
            ::ftlog::info!("stop scan file");
            return Ok(());
        }
       bail!("Failed to get subscriber")
    }

    pub fn empty(&self) -> anyhow::Result<bool> {
        Ok(self.msg_dispatcher.no_subscriber())
    }
    
    /// 获取注册前的数据
    pub fn get_register_before_data(&self, cert_key: CertKeyT) -> Result<()> {
        ::ftlog::info!("{} reading register before data", cert_key);
        if let Some(seek_pos) = self.register_before_pos.get(&cert_key) {
            self.inner_chan.0.send((cert_key, *seek_pos))?; // 异步读取之前的数据
        } else {
            ::ftlog::error!("cert key {} no register before pos", cert_key);
            bail!("cert key no register")
        }
        Ok(())
    }
    /// 获取注册前的数据
    pub fn get_all_data(&self, cert_key: CertKeyT) -> Result<()> {
        ::ftlog::info!("get all data for cert key {}", cert_key);
        let pos = self.seek_pos.load(Ordering::Relaxed);
        self.inner_chan.0.send((cert_key, pos))?; // 异步读取数据
        Ok(())
    }


}

// 分发数据
pub fn dispatch_data<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static>(
    msg_dispatcher: &Arc<MsgDispatcher<T>>,
    data_list: Vec<anyhow::Result<T>>
) -> anyhow::Result<()> {
    msg_dispatcher.dispatch(data_list)
}