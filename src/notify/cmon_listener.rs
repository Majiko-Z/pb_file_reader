use std::path::PathBuf;
use super::FileListener;
use anyhow::{Result,bail};
use notify::{
    Config, Event, RecommendedWatcher, RecursiveMode, Watcher, event::{DataChange, EventKind, ModifyKind}
};
use dashmap::DashMap;
use crossbeam::channel::{Receiver, Sender, bounded};
use crate::utils::{model::{NotifyEvent, NotifyEventData, NotifyMeta, gen_uid}, timer::get_coarse_timestamp_ms};
pub struct CmonListener {
    watcher: std::sync::Arc<std::sync::Mutex<RecommendedWatcher>>, // watcher自身不是线程安全的
    running: std::sync::Arc<std::sync::atomic::AtomicBool>, // 控制线程运行
    path_map: std::sync::Arc<DashMap<PathBuf,Vec<NotifyMeta>>>, // 实际文件监控路径
    inner_chan: (Sender<Event>, Receiver<Event>),
}

impl CmonListener {
    pub fn new() -> Result<Self> {
        println!("common listener init");
        let (send_c, recv_c) = bounded(16);
        let send_c_clone = send_c.clone();
        let watcher = std::sync::Arc::new(std::sync::Mutex::new(
            RecommendedWatcher::new(move |res: Result<Event, notify::Error>| {
                if let Ok(eve) = res { // 仅监控文件大小变化
                    println!("recv raw event={:?},path_len={}", eve, eve.paths.len());
                    match eve.kind {
                        EventKind::Modify(ModifyKind::Data(DataChange::Content)) |
                        EventKind::Modify(ModifyKind::Data(DataChange::Size)) => {
                            let _ = send_c.send(eve);
                            println!("send event");
                        }
                        _ => {}
                    }
                }
            }, Config::default())?
        ));
        Ok(Self {
            watcher,
            running:  std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            path_map: std::sync::Arc::new(DashMap::new()),
            inner_chan: (send_c_clone, recv_c),
        })

    }
    pub fn __add_watch(&self, path: PathBuf) -> Result<NotifyMeta> {

        let (send_c,recv_c) = bounded(3);
        let meta = NotifyMeta {
            uid: gen_uid(),
            file_path: path.clone(),
            sender: send_c,
            receiver: recv_c,
            cur_bytes: 0,
            last_bytes: 0,
        };
        if self.path_map.contains_key(&path) {
            // 路径已经在监控下
            self.path_map.entry(path).and_modify(|v| v.push(meta.clone()));
        } else {
            println!("add watch for {:?}", path.display());
            self.watcher.lock().map_err(|_| anyhow::anyhow!("Failed to acquire watcher lock"))?
                .watch(&path, RecursiveMode::NonRecursive)?;
            self.path_map.insert(path.clone(), vec![meta.clone()]);
            // 并且添加到实际的文件监视器中
           
        }
        Ok(meta)
    }

    pub fn __remove_watch(&self, meta: &NotifyMeta) -> Result<()> { 
        let path = &meta.file_path;
        // 从 path_map 中查找对应路径的 Vec<NotifyMeta>
        if let Some(mut entry) = self.path_map.get_mut(path) {
            // 从 Vec 中找到匹配 uid 的元素并删除
            entry.retain(|existing_meta| existing_meta.uid != meta.uid);
            
            // 如果 Vec 为空，则移除 key 并调用 unwatch
            if entry.is_empty() {
                drop(entry); // 释放可变引用
                self.path_map.remove(path);
                self.watcher.lock()
                    .map_err(|_| anyhow::anyhow!("Failed to acquire watcher lock"))?
                    .unwatch(path)?;
            }
        } else {
            bail!("Path:{} not watched", path.display())
        }
        Ok(())
    }
    pub fn event_loop(&self) -> Result<()> { 
        println!("event loop begin");
        let is_running = self.running.clone();
        let recv_chan = self.inner_chan.1.clone();
        let path_map = self.path_map.clone();
        std::thread::spawn(move || {
            while is_running.load(std::sync::atomic::Ordering::Relaxed) {
                println!("event loop running");
                match recv_chan.recv() {
                    Ok(event) => {
                        println!("recv event:{:?},path_len={}", event, event.paths.len());
                        match event.kind {
                            _ => { // 已经在send前过滤事件类型
                                for path in event.paths {
                                    println!("handle path:{:?}", path.display());
                                    if let Some(entries) = path_map.get(&path) {
                                        for entry in entries.iter() { // 通知所有chan
                                           let _= entry.sender.send(NotifyEventData { 
                                            event: NotifyEvent::WriteEvent,
                                            last_notify_time:get_coarse_timestamp_ms(),
                                           });
                                        }
                                    }
                                }

                            }
                        }
                    }
                    Err(e) => {
                        println!("Error:{}", e);
                    }
                }
            }
        });
        Ok(())
    }
    pub fn __init(&self) -> Result<()> {
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        self.event_loop()?;
        Ok(())
    }
}

impl FileListener for CmonListener { 
    fn add_watch(&self, f_path: PathBuf) -> Result<NotifyMeta> {
        self.__add_watch(f_path)
    }
    fn remove_watch(&self, meta: &NotifyMeta) -> Result<()>{
        self.__remove_watch(meta)
    }
    fn init(&self) -> Result<()> {
        self.__init()
    }
}
