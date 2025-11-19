use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use serde::{Deserialize};
use serde::de::DeserializeOwned;   
use std::fs::File;
use std::sync::atomic::{AtomicBool};
use crossbeam::channel::{bounded,Receiver};
use super::msg_dispatcher::{MsgDispatcher, CertKeyT};
use crate::notify::GLOBAL_LISTENER;
use crate::utils::model::{*};
use anyhow::{Result, bail};
use std::sync::Mutex;

pub type CsvReader<T> = SubsReader<T, CSV>;

pub type DbfReader<T> = SubsReader<T, DBF>;

pub trait ReadRunner {
    fn run(& self);
}

pub struct SubsReader<T: DeserializeOwned + Send + Sync + Clone + 'static,  F: FileType> {
    pub file_path: PathBuf, // 文件路径
    pub is_increment: bool, // 是否增量读
    pub seek_pos:  Arc<AtomicU64>, // 文件seek位置
    pub enc_type: EncType, // 编码类型
    pub fd: Option<File>, // 文件句柄
    pub msg_dispatcher:Arc<MsgDispatcher<T>>, // 消息分发器
    pub is_running: Arc<AtomicBool>, // 控制扫单线程运行
    pub notify_meta: NotifyMeta,
    _phantom: std::marker::PhantomData<F>, // 占位防止编译出错
}


impl<T: DeserializeOwned + Send + Sync + Clone + 'static, F: FileType> SubsReader<T, F> {
    pub fn new(file_path: PathBuf, is_increment: bool, enc: EncType) -> Result<Self> {
        let notify_meta = GLOBAL_LISTENER.add_watch(file_path.clone())?;
        Ok(Self {
            file_path,
            is_increment,
            seek_pos: Arc::new(AtomicU64::new(0)),
            enc_type: enc,
            fd: None,
            msg_dispatcher: Arc::new(MsgDispatcher::new()),
            is_running: Arc::new(AtomicBool::new(false)),
            notify_meta,
            _phantom: std::marker::PhantomData,
        })
    }

    // 判断文件是否有新数据,通过对比文件大小判断
    pub fn have_new_data(&self) -> bool {
        // 除非文件不存在, 或者无read权限
         match std::fs::metadata(&self.file_path) {
            Ok(m) => {
                m.len() > self.seek_pos.load(Ordering::Relaxed)  // 只在一个read线程中调用

            },
            Err(e) => {
                // todo need log
                false
            }
        }
    }

    // 获取文件句柄 - 懒加载方式
    pub fn get_fd(&mut self) -> anyhow::Result<&mut File> {
        if self.fd.is_none() {
            let file = File::open(&self.file_path)?;
            self.fd = Some(file);
        }
        self.fd.as_mut().ok_or_else(|| {
            anyhow::anyhow!("File handle is None for path: {:?}", self.file_path)
        })
    }

    // 重置文件读取位置
    pub fn reset_seek_pos(&self) { // 严格同步
        self.seek_pos.store(0, Ordering::SeqCst)
    }

    // 订阅 返回一个cert和chan
    pub fn subscribe(&self, verify_data: &str, dispatcher_func: fn(&str, &T) -> bool) -> (CertKeyT, Receiver<Vec<Result<T>>>)
    where
        Self: ReadRunner,
    {
        self.reset_seek_pos(); // 重置文件读取位置
        let (send_chan, recv_chan) = bounded(16);

        // 获取锁并调用方法
        let cert_key = self.msg_dispatcher.get_cert_and_subscribe(verify_data, dispatcher_func, send_chan);

        if self.is_running.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            self.run(); // 第一次,则启动
        }
        (cert_key, recv_chan)
    }


    // 取消订阅
    pub fn unsubscribe(&self, cert_key: CertKeyT) -> anyhow::Result<()> {
        {
            self.msg_dispatcher.unsubscribe(cert_key);
        }

        let no_subscriber = {
            self.msg_dispatcher.no_subscriber()
        };

        if no_subscriber { //无人订阅, 则停止扫单
            self.is_running.store(false, Ordering::SeqCst);
            return Ok(());
        }
       bail!("Failed to get subscriber")
    }

    pub fn empty(&self) -> anyhow::Result<bool> {
        Ok(self.msg_dispatcher.no_subscriber())
    }

}

// 分发数据
pub fn dispatch_data<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static>(
    msg_dispatcher: &Arc<Mutex<MsgDispatcher<T>>>,
    data_list: Vec<anyhow::Result<T>>
) -> anyhow::Result<()> {
    let mut dispatcher = msg_dispatcher.lock()
        .map_err(|e| anyhow::anyhow!("Failed to acquire dispatcher lock: {}", e))?;
    dispatcher.dispatch(data_list)
}