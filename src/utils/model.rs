use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use crossbeam::channel::{Receiver, Sender};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NotifyEvent {
    WriteEvent = 1,
    ScheduleEvent = 2,
    StopEvent = 3,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NotifyEventData {
    pub event: NotifyEvent,
    pub last_notify_time: u64,
}


pub type NotifyMetaUid = i64;
static UID_COUNTER: AtomicI64 = AtomicI64::new(1);

type NotifyEventSender = Sender<NotifyEventData>;
type NotifyEventReceiver = Receiver<NotifyEventData>;
pub fn gen_uid() -> NotifyMetaUid {
    UID_COUNTER.fetch_add(1, Ordering::Relaxed)
}
#[derive(Debug, Clone)]
pub struct NotifyMeta {
    pub uid: NotifyMetaUid,
    pub file_path: PathBuf,
    pub last_bytes: u64,
    pub cur_bytes: u64,
    pub sender: NotifyEventSender,
    pub receiver: NotifyEventReceiver,
}


#[derive(Debug, Clone, Copy)]
pub enum EncType {
    GBK,
    UTF8,
}

pub struct CSV;
pub struct DBF;

unsafe impl Send for CSV {}
unsafe impl Sync for CSV {}

unsafe impl Send for DBF {}
unsafe impl Sync for DBF {}

pub trait FileType {
    fn file_type () -> &'static str;
}

impl FileType for CSV {
    fn file_type () -> &'static str {
        "csv"
    }
}

impl FileType for DBF {
    fn file_type () -> &'static str {
        "dbf"
    }
}

pub const MAX_READ_RETRY_TIME: i32 = 5;
pub const MIN_READ_INTERVAL: u64 = 1;

pub const DEFAULT_INCR_POLL_INTERVAL: Duration = Duration::from_millis(256); // 256ms

pub const DEFAULT_FULL_POLL_INTERVAL: Duration = Duration::from_millis(512); // 512ms
pub const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_millis(1024); // 1024ms