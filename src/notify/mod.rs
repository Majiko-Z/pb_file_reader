use std::path::PathBuf;
use crate::utils::model::{NotifyMeta};
use anyhow::Result;
use once_cell::sync::Lazy;

pub mod cmon_listener;
pub mod noop_listener;
#[cfg(feature = "windows_iocp_listener")]
pub mod iocp_listener;



pub trait FileListener {
    /// 添加监听文件
    fn add_watch(&self, f_path: PathBuf) -> Result<NotifyMeta>;
    /// 移除监听文件
    fn remove_watch(&self, meta: &NotifyMeta) -> Result<()>;

    /// 启动
    fn init(&self) -> Result<()>;
}

pub static GLOBAL_LISTENER: Lazy<Box<dyn FileListener + Send + Sync>> = Lazy::new(|| {
    create_and_init_platform_listener()
});

#[cfg(feature = "windows_iocp_listener")]
fn create_and_init_platform_listener() -> Box<dyn FileListener + Send + Sync> {
    use iocp_listener::IOCPListener;
    let listener = IOCPListener::new().expect("Failed to create IOCP listener");
    listener.init().expect("Failed to initialize IOCP listener");
    Box::new(listener)
}

#[cfg(feature = "common_listener")]
fn create_and_init_platform_listener() -> Box<dyn FileListener + Send + Sync> {
    // 其他平台的实现
    use cmon_listener::CmonListener;
    let listener = CmonListener::new().expect("Failed to create Cmon listener");
    listener.init().expect("Failed to initialize Common listener");
    Box::new(listener)
}

#[cfg(not(any(feature = "windows_iocp_listener", feature = "common_listener")))]
fn create_and_init_platform_listener() -> Box<dyn FileListener + Send + Sync> {
    Box::new(NoopListener) // 空实现, 会报错
}

pub fn get_global_listener() -> &'static dyn FileListener {
    &**GLOBAL_LISTENER
}