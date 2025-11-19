// src/platform/windows.rs
use std::os::windows::ffi::OsStrExt;
use windows::core::{PCWSTR};
use windows::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, ERROR_IO_PENDING},
    Storage::FileSystem::{
    CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OVERLAPPED, FILE_LIST_DIRECTORY, FILE_SHARE_DELETE, 
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING, FILE_NOTIFY_CHANGE_SIZE,FILE_FLAG_BACKUP_SEMANTICS,
    ReadDirectoryChangesW,
    },
    System::IO::{
        CancelIoEx, 
        CreateIoCompletionPort, 
        GetQueuedCompletionStatus, 
        OVERLAPPED,
    },
};
use coarsetime::{Clock};
use dashmap::{DashMap};
use std::path::PathBuf;
use anyhow::{bail, Result};
use crossbeam::channel::{bounded, Sender, Receiver};
use crate::utils::{model::{NotifyMeta, NotifyEvent, NotifyEventData, gen_uid}};
use crate::utils::timer::get_coarse_timestamp_ms;
use super::FileListener;
struct MonitorContext {
    buffer: Vec<u8>,
    overlapped: OVERLAPPED,
}
pub struct IOCPListener {
    iocp_handler: std::sync::Arc<SendableHandle>, // iocp 句柄
    watch_info: std::sync::Arc<DashMap<usize, Vec<NotifyMeta>>>, // 目录句柄 -> 监控文件列表(无需存储目录)
    dir_map: DashMap<PathBuf, HANDLE>, // 路径 -> 句柄
    monitor_contexts: std::sync::Arc::<DashMap<usize, MonitorContext>>, // 监控使用
    inner_channel: (Sender<usize>, Receiver<usize>), // 内部线程通信
    running: std::sync::Arc<std::sync::atomic::AtomicBool>, // 控制线程运行
}

impl IOCPListener {
    pub fn new() -> Result<Self> {
        println!("begin new");
        let iocp_handler = match unsafe {
            CreateIoCompletionPort(
                INVALID_HANDLE_VALUE, // 传入 INVALID_HANDLE_VALUE 来创建一个新的端口
                None, // 没有现有的端口
                0,                 // 没有完成键
                0,                 // 使用默认的线程数
            )} {
            Ok(h) => h,
            Err(e) => {
                println!("failed create io completion port:{:?}", e);
                bail!(e);
            }
        };
        // 内部线程通信
        let (send_c, recv_c) = bounded(16);
        Clock::update();
        println!("INIT LISTENER SUCCESS");
        Ok(Self {
            iocp_handler: std::sync::Arc::new(SendableHandle(iocp_handler)),
            watch_info: std::sync::Arc::new(DashMap::default()),
            dir_map: DashMap::default(),
            monitor_contexts: std::sync::Arc::new(DashMap::default()),
            inner_channel: (send_c, recv_c),
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// 添加监控文件
    fn __add_watch(&self, f_path: PathBuf) -> Result<NotifyMeta> { 
        if !f_path.is_file() {
            bail!("Not a file: {}", f_path.display());
        }

        let dir_path = match f_path.parent() { // iocp实际监控目录
            Some(path) => path.to_path_buf(),
            None => bail!("Invalid path: {}", f_path.display()),
        };
        if !dir_path.is_dir() {
            println!("Not a dir:{}", dir_path.display());
        }
        let (send_c,recv_c) = bounded(3);

        let notify_meta = NotifyMeta {
            uid: gen_uid(),
            file_path: f_path,
            sender: send_c,
            receiver: recv_c,
            cur_bytes: 0,
            last_bytes: 0,
        };

        if !self.dir_map.contains_key(&dir_path) { // 尚未被监控的目录

            let wide_path: Vec<u16> = dir_path.as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))  // 添加 null 终止符
                .collect();
            println!("dir_path: {:?}", dir_path);
            // 创建目录句柄，使用异步模式(FILE_FLAG_OVERLAPPED)和目录监听标志(FILE_LIST_DIRECTORY)
            let dir_handle = unsafe {
                CreateFileW(
                    PCWSTR::from_raw(wide_path.as_ptr()),
                    // 第2个参数: FILE_ACCESS_RIGHTS
                    FILE_LIST_DIRECTORY.0, // 直接使用枚举变体
                    // 第3个参数: FILE_SHARE_MODE (修正点1)
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, // 直接对枚举变体进行位或运算
                    // 第4个参数: Option<*const SECURITY_ATTRIBUTES> (修正点3)
                    None, // 使用 None 而不是 null_mut()
                    // 第5个参数: FILE_CREATION_DISPOSITION
                    OPEN_EXISTING, // 直接使用枚举变体
                    // 第6个参数: FILE_FLAGS_AND_ATTRIBUTES (修正点2)
                    FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL|FILE_FLAG_BACKUP_SEMANTICS, // 直接对枚举变体进行位或运算
                    // 第7个参数: HANDLE
                    Some(HANDLE::default()),
                )
            };
            if dir_handle.is_err() {
                println!("CreateFileW path:{:?}; error: {:?}", dir_path, dir_handle);
            }
            let dir_handle =  dir_handle.unwrap();

            // 将目录句柄关联到IOCP完成端口，以便接收异步IO完成通知
            unsafe {
                CreateIoCompletionPort(
                    dir_handle,
                    Some(self.iocp_handler.0),
                    dir_handle.0 as usize, // 使用目录句柄作为完成键
                    0,
                )
            }?;
            // 第一次加目录则开启监控
            Self::start_monitoring(&self.monitor_contexts, dir_handle)?;
            self.dir_map.insert(dir_path.clone(), dir_handle);
            // 记录目录句柄与路径的映射关系，用于后续事件处理
            println!("insert handle:{:?}",dir_handle.0 as usize);
            self.watch_info.insert(dir_handle.0 as usize, vec![notify_meta.clone()]);
        } else {
            // 如果目录已被监控，获取已存在的句柄
            if let Some(dir_handle) = self.dir_map.get(&dir_path) {
                // 更新watch_info中的监控元数据列表
                if let Some(mut watch_entry) = self.watch_info.get_mut(&(dir_handle.0 as usize)) {
                    watch_entry.push(notify_meta.clone());
                }
            }
        }

        // 将通知元数据添加到监控映射中，支持同一目录下多个文件的监控

        Ok(notify_meta)
    }

    /// 删除监控文件
    fn __remove_watch(&self, meta: &NotifyMeta) -> Result<()> {
        // 获取文件对应的目录路径
        let dir_path = match meta.file_path.parent() {
            Some(path) => path.to_path_buf(),
            None => bail!("Invalid path: {}", meta.file_path.display()),
        };

        // 通过目录路径找到对应的句柄
        if let Some(dir_handle) = self.dir_map.get(&dir_path) {
            let handle = *dir_handle;

            // 从 watch_info 中移除对应的 NotifyMeta
            if let Some(mut watch_entry) = self.watch_info.get_mut(&(handle.0 as usize)) {
                // 根据 uid 移除对应的元素
                watch_entry.retain(|existing_meta| existing_meta.uid != meta.uid);

                // 如果移除后 vec 为空，则清理相关资源
                if watch_entry.is_empty() {
                    // 从 watch_info 中移除空的 vec
                    self.watch_info.remove(&(handle.0 as usize));
                    // 从 dir_map 中移除目录映射
                    self.dir_map.remove(&dir_path);

                    // 取消所有IO操作并关闭句柄
                    unsafe {
                        // 取消所有待处理的IO操作
                        let _ = CancelIoEx(handle, None);
                        // 关闭句柄
                        let _ = CloseHandle(handle);
                    };
                }
            }
        }

        Ok(())
    }

    /// 开始监控指定目录的变化
    fn start_monitoring(monitor_contexts: &DashMap<usize, MonitorContext>, dir_handle: HANDLE) -> Result<()> {
        // 为目录变化准备缓冲区
        const BUFFER_SIZE: usize = 4096;
        let buffer = vec![0u8; BUFFER_SIZE];
        let overlapped = unsafe { std::mem::zeroed::<OVERLAPPED>() };
        
        let context = MonitorContext {
            buffer,
            overlapped,
        };
        
        let handle_key = dir_handle.0 as usize;
        
        // 存储监控上下文
        monitor_contexts.insert(handle_key, context);
        
        // 获取存储的上下文的可变引用
        if let Some(mut context) = monitor_contexts.get_mut(&handle_key) {
            // 开始监控目录变化
            let result = unsafe {
                ReadDirectoryChangesW(
                    dir_handle,
                    context.buffer.as_mut_ptr() as *mut std::ffi::c_void,
                    context.buffer.len() as u32,
                    true,  // 监控子目录
                    FILE_NOTIFY_CHANGE_SIZE,
                    None,
                    Some(&mut context.overlapped),
                    None,
                )
            };
                    
            // 检查是否成功启动监控
            if let Err(e) = result {
                // 如果是 ERROR_IO_PENDING，表示异步操作已启动，这是正常情况
                if e.code().0 != ERROR_IO_PENDING.0 as i32 {
                    // 清理已存储的上下文
                    monitor_contexts.remove(&handle_key);
                    bail!("Failed to start monitoring directory: {}", e);
                }
            }
        } else {
            bail!("Failed to get monitoring context");
        }
        println!("monitor end");
        Ok(())
    }

    /// 启动IOCP事件循环线程
    fn start_event_loop(&self) -> Result<()> {
        let iocp_handler = self.iocp_handler.clone(); // 复制 SendableHandle
        let inner_sender = self.inner_channel.0.clone();
        let running_flag = self.running.clone(); // 克隆 AtomicBool 引用

        std::thread::spawn(move || {
            println!("start event loop");
            while running_flag.load(std::sync::atomic::Ordering::Relaxed) {
                let mut bytes_transferred: u32 = 0;
                let mut completion_key: usize = 0;
                let mut overlapped: *mut OVERLAPPED = std::ptr::null_mut();
                println!("wait for GetQueuedCompletionStatus");
                // 等待IO完成事件，设置超时以允许检查运行状态
                let result = unsafe {
                    GetQueuedCompletionStatus(
                        iocp_handler.0,
                        &mut bytes_transferred,
                        &mut completion_key,
                        &mut overlapped,
                        0xFFFFFFFF, // 无限等待
                    )
                };

                // 检查是否仍在运行
                if !running_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                // 处理完成事件
                if result.is_ok() && bytes_transferred > 0 {
                    println!("send");
                    // 发给其他线程处理
                    let _ = inner_sender.send(completion_key);
                } else {
                    println!("err:{:?}", result);
                }
            }
        });
        Ok(())
    }

    /// 检查和SEND线程
    fn start_check_thread(&self) -> Result<()> {
        let inner_receiver = self.inner_channel.1.clone();
        let watch_info = self.watch_info.clone();
        let running_flag = self.running.clone();
        let monitor_context = self.monitor_contexts.clone();
        std::thread::spawn(move || {
            println!("start check thread");
            while running_flag.load(std::sync::atomic::Ordering::Relaxed) {
                // 阻塞接收，但设置超时以允许检查运行状态
                match inner_receiver.recv() {
                    Ok(complete_key) => {
                        println!("recv complete key:{}", complete_key);
                        for entry in watch_info.iter() {
                            let key = entry.key(); // 获取 key 的引用 &K
                            println!("Key: {}", key);
                        }
                        if let Some(mut watch_entries) = watch_info.get_mut(&complete_key) {
                            for notify_meta_data in watch_entries.iter_mut() {
                                // 通知具体的文件READER
                                if let Ok(file_meta_data) = std::fs::metadata(&notify_meta_data.file_path) {
                                    let cur_size = file_meta_data.len();
                                    if notify_meta_data.cur_bytes != cur_size {
                                        // 文件不一致,发送通知
                                        notify_meta_data.last_bytes = notify_meta_data.cur_bytes;
                                        notify_meta_data.cur_bytes = cur_size;
                                        println!("send notify");
                                        let _ = notify_meta_data.sender.send(NotifyEventData {
                                            event: NotifyEvent::WriteEvent,
                                            last_notify_time: get_coarse_timestamp_ms(),
                                        });
                                    } else {
                                        println!("not send notify");
                                    }
                                }
                            }
                        } else {
                            println!("not found combine key from watch info");
                        }
                        // 重新启动监控
                        let dir_handle = HANDLE(complete_key as *mut std::ffi::c_void);
                        match Self::start_monitoring(&monitor_context, dir_handle) {
                            Ok(()) =>{}
                            Err(_) => {}
                        }
                        // self.start_monitoring(dir_handle);
                        
                    }
                    Err(_) => {
                        // 通道关闭，退出循环
                        break;
                    }
                }
            }
        });
        Ok(())
}
    
    fn __init(&self) {
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        
        // 启动检查线程
        let _ = self.start_check_thread();
        // 启动事件循环线程
        let _ = self.start_event_loop();
    }
}


#[repr(transparent)]
#[derive(Copy, Clone)]
struct SendableHandle(HANDLE);

unsafe impl Send for SendableHandle {}
unsafe impl Sync for SendableHandle {}
unsafe impl Send for MonitorContext {}
unsafe impl Sync for MonitorContext {}
unsafe impl Send for IOCPListener {}
unsafe impl Sync for IOCPListener {}

impl FileListener for IOCPListener { 
    fn add_watch(&self, f_path: PathBuf) -> Result<NotifyMeta> {
        self.__add_watch(f_path)
    }
    fn remove_watch(&self, meta: &NotifyMeta) -> Result<()> {
        self.__remove_watch(meta)
    }
    fn init(&self) -> Result<()> {
        self.__init();
        Ok(())
    }
}
