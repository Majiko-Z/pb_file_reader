use std::sync::{atomic::{AtomicI32, AtomicBool, Ordering}, Arc};
use crossbeam::channel::{Sender};
use serde::Deserialize;
use dashmap::DashMap;
use rustc_hash::FxHashMap;
use anyhow::Result;
/*
DispatcherCert: 数据分流凭证
*/
struct DispatcherCert<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> {
    pub cert_key: i32, // 唯一凭证
    pub dispatcher_func: fn(&str, &T) -> bool, // 匹配函数(verify_data, data)
    pub verify_data: String,
    pub is_running: Arc<AtomicBool>, // 删除凭据
    pub send_channel: Sender<Vec<Result<T>>>, // 发送通道
}

/*
MsgDispatcher: 消息分发
*/
pub type CertKeyT = i32;

pub struct MsgDispatcher<T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> {
    dispatcher_certs: DashMap<CertKeyT, DispatcherCert<T>>,
    pub subscriber_count: Arc<AtomicI32>, // 订阅者数量只增不减
}

impl <T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> MsgDispatcher<T> {
    pub fn new() -> Self {
        MsgDispatcher {
            subscriber_count: Arc::new(AtomicI32::new(1)),
            dispatcher_certs: DashMap::new(),
        }
    }

    /// 获取凭证;凭证仅用于移除channel
    pub fn get_cert(&self) -> CertKeyT {
        self.subscriber_count.fetch_add(1, Ordering::Relaxed) as CertKeyT
    }

    /// 注册channel
    pub fn subscribe(&self, verify_data: &str, dispatcher_func: fn(&str, &T) -> bool, sender: Sender<Vec<Result<T>>>, cert_key: CertKeyT) {
        self.dispatcher_certs.insert(cert_key, DispatcherCert {
            cert_key,
            verify_data: verify_data.to_string(),
            dispatcher_func,
            send_channel: sender,
            is_running: Arc::new(AtomicBool::new(true)),
        });
    }

    ///  获取凭证并注册, 返回凭证id,后续拿凭证id移除channel
    pub fn get_cert_and_subscribe(&self, verify_data: &str, dispatcher_func: fn(&str, &T) -> bool, sender: Sender<Vec<Result<T>>>) -> CertKeyT {
        let cert_key = self.get_cert();
        self.subscribe(verify_data, dispatcher_func, sender, cert_key);
        cert_key
    }

    /// 分发数据
    pub fn dispatch(&self, msgs: Vec<Result<T>>) -> anyhow::Result<()> {
        println!("ready dispatch:len={}",msgs.len());
        // 使用局部的 FxHashMap 作为缓冲区
        let mut dispatcher_buff: FxHashMap<CertKeyT, Vec<Result<T>>> = FxHashMap::default();

        for msg in msgs.into_iter() {
            for entry in self.dispatcher_certs.iter() {
                let cert = entry.value();
                if cert.is_running.load(Ordering::Relaxed) { //只发给运行中的chan
                    match &msg {
                        Ok(data) => {
                            if (cert.dispatcher_func)(&cert.verify_data, &data) {  // 传递 &T 而不是 &Result<T>
                                // 将数据添加到对应cert的缓冲区中
                                println!("insert to cert:{}",cert.cert_key);
                                dispatcher_buff.entry(cert.cert_key)
                                    .or_insert_with(Vec::new)
                                    .push(Ok(data.clone()));
                            }
                        },
                        Err(e) => {
                            // 给所有chan发送该错误消息的克隆版本
                            dispatcher_buff.entry(cert.cert_key)
                                .or_insert_with(Vec::new)
                                .push(Err(anyhow::format_err!("{}", e))); // 使用format_err克隆错误
                        }
                    }
                }
            }
        }
        

        // 批量发送并清空buffer; 减少一次mem copy
        for (key, buffer) in dispatcher_buff.drain() { // send
            let data_len = buffer.len();
            if let Some(cert) = self.dispatcher_certs.get(&key) {
                if cert.is_running.load(Ordering::Relaxed) {
                    if let Err(e) = cert.send_channel.send(buffer) {
                        println!("send error:{:?}", e);
                    } else {
                        println!("send to cert={} success, len={}",key, data_len);
                    }
                }
            }
        }

        Ok(())
    }

    /// 取消订阅
    pub fn unsubscribe(&self, cert_key_t: CertKeyT) {
        if let Some((_, cert)) = self.dispatcher_certs.remove(&cert_key_t) {
            cert.is_running.store(false, Ordering::Relaxed); // 标记不可用
            // 移除操作已经在上面完成，无需再次remove
        }
        // 不需要else分支，因为DashMap的remove方法在键不存在时返回None
    }

    /// 判断是否无订阅者
    pub fn no_subscriber(&self) -> bool {
        self.dispatcher_certs.is_empty()
    }

    /// 发送给单个订阅者
    pub fn dispatch_single(&self, msgs: Vec<Result<T>>, cert_key: CertKeyT) -> anyhow::Result<()> {
        println!("ready dispatch:len={}",msgs.len());
        if let Some(cert_data) = self.dispatcher_certs.get(&cert_key) {
            if cert_data.is_running.load(Ordering::Relaxed) {
                cert_data.send_channel.send(msgs)?;
            }
        }

        Ok(())
    }

    
}


mod test {
    use super::*;
    use serde_derive::{Serialize, Deserialize};
    use crossbeam::channel::{Sender, Receiver, bounded, Select};
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct ReportOrderAlgo {
        pub order_id: String,
    }
    // 引入 Mutex
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_dispatcher() {
        // 创建带 Mutex 的 Arc
        let dispatcher = Arc::new(MsgDispatcher::<ReportOrderAlgo>::new());

        for i in [2, 3, 5, 7, 9] {
            let dispatcher_clone = Arc::clone(&dispatcher);
            let cur_id = i;
            std::thread::spawn(move || {
                let (send_c, recv_c) = bounded(1024);
                let cert_key = {
                    dispatcher_clone.get_cert_and_subscribe(
                        &i.to_string(),
                        |verify_data, msg| msg.order_id.contains(verify_data),
                        send_c,
                    )
                };

                let mut count = 0;
                let mut selector = Select::new();
                let recv_c_selector = selector.recv(&recv_c);

                loop {
                    let ready_id = selector.ready();
                    match ready_id {
                        recv_c_selector => {
                            match recv_c.recv() {
                                Ok(r) => {
                                    println!("thread_id={};recv:{:?} ...", cur_id, r);
                                    count += 1;
                                    if count == 20 {
                                        println!("thread_id={};exit...", cur_id);
                                        break;
                                    }
                                }

                                Err(_) => {
                                    println!("cur_id={};recv failed", cur_id);
                                }
                            }
                        }
                    }
                }

                dispatcher_clone.unsubscribe(cert_key);
            });
        }

        let mut cnt = 1;
        loop {
            let mut msg = vec![];
            for i in cnt..cnt + 10 {
                msg.push(Ok(ReportOrderAlgo {
                    order_id: i.to_string(),
                }));
            }

            // 锁定并调用 dispatch
            match dispatcher.dispatch(msg) {
                Ok(()) => {
                    println!("dispatch success:{}", cnt);
                }
                Err(e) => {
                    println!("dispatch error: {:?}", e);
                }
            }

            cnt += 10;
            std::thread::sleep(std::time::Duration::from_millis(100));

            if dispatcher.no_subscriber() {
                break;
            }
        }
    }

}