use dashmap::DashMap;

use serde::Deserialize;

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use rustc_hash::FxHashMap;
use std::any::{Any, TypeId};
use std::path::{PathBuf};
use std::sync::{Arc, Mutex};
use super::{subscribe_reader::*};
use crate::reader::msg_dispatcher::CertKeyT;
use crate::common::model::*;


static CSV_READER_INSTANCES: OnceCell<
    DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>>,
> = OnceCell::new();


static DBF_READER_INSTANCES: OnceCell<
    DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>>,
> = OnceCell::new();


// 为每个路径创建一个锁
static CSV_PATH_LOCKS: OnceCell<DashMap<PathBuf, Arc<Mutex<()>>>> = OnceCell::new();

// 获取路径锁的函数
fn get_csv_path_lock(path: &PathBuf) -> Arc<Mutex<()>> {
    let locks = CSV_PATH_LOCKS.get_or_init(|| DashMap::new());
    locks.entry(path.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
}

// 为每个路径创建一个锁
static DBF_PATH_LOCKS: OnceCell<DashMap<PathBuf, Arc<Mutex<()>>>> = OnceCell::new();

// 获取路径锁的函数
fn get_dbf_path_lock(path: &PathBuf) -> Arc<Mutex<()>> {
    let locks = DBF_PATH_LOCKS.get_or_init(|| DashMap::new());
    locks.entry(path.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
}


/// Initializes the CSV reader global map.
fn init_csv_map() -> DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>> {
    DashMap::new()
}

/// Gets a reference to the CSV reader global map.
fn get_csv_map() -> &'static DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>> {
    CSV_READER_INSTANCES.get_or_init(init_csv_map)
}


/// Initializes the DBF reader global map.
fn init_dbf_map() -> DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>> {
    DashMap::new()
}

/// Gets a reference to the DBF reader global map.
fn get_dbf_map() -> &'static DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>> {
    DBF_READER_INSTANCES.get_or_init(init_dbf_map)
}

/// 创建或者返回已有的reader
pub fn get_or_create_csv_reader<T>(path: &PathBuf, increment: bool, enc_type: EncType) -> Result<Arc<CsvReader<T>>>
where
    T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static
{
    let type_id = TypeId::of::<T>();
    let map = get_csv_map();

    let path_lock = get_csv_path_lock(path);
    let _guard = path_lock.lock().map_err(|e| anyhow::anyhow!("Failed to acquire path lock: {:?}", e))?;
    // 尝试读取已有 reader
    if let Some(mut path_map) = map.get_mut(&type_id) {
        if let Some(entry) = path_map.get_mut(path) {
            let reader_arc = if increment {
                &entry.0
            } else {
                &entry.1
            };

            if let Some(reader) = reader_arc {
                if reader.is::<CsvReader<T>>() {
                    let typed_arc: Arc<CsvReader<T>> = reader.clone().downcast().map_err(|_| {
                        anyhow!("CSV reader instance type conversion failed for path: {:?}", path.display())
                    })?;
                    ::ftlog::info!("{} return exist reader", path.display());
                    return Ok(typed_arc);
                }
                return Err(anyhow!("CSV reader instance type mismatch for path: {:?}", path.display()));
            }
        }
    }

    // 创建新 reader 并存入全局 map
    let csv_reader = CsvReader::new(path.clone(), increment, enc_type)?;
    let reader_arc: Arc<CsvReader<T>> = Arc::new(csv_reader);

    // 转换为 trait object 存储
    let any_arc: Arc<dyn Any + Send + Sync> = reader_arc.clone();

    // 获取或初始化当前类型的子映射表
    let mut inner_map = map.entry(type_id).or_insert_with(FxHashMap::default);

    // 更新指定路径的 reader 条目
    let entry = inner_map.entry(path.clone()).or_insert((None, None));
    if increment {
        entry.0 = Some(any_arc);
    } else {
        entry.1 = Some(any_arc);
    }
    ::ftlog::info!("create new reader for {}", path.display());
    Ok(reader_arc)
}

/// 移除 CSV reader
pub fn remove_csv_reader<T>(cert_key: CertKeyT, increment: bool, path: &PathBuf) -> Result<()> 
where
    T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static
{
    let type_id = TypeId::of::<T>();
    let map = get_csv_map();

    let path_lock = get_csv_path_lock(path);
    let _guard = path_lock.lock().map_err(|e| anyhow::anyhow!("Failed to acquire path lock: {:?}", e))?;

    // 获取类型对应的路径映射
    if let Some(mut path_map) = map.get_mut(&type_id) {
        // 查找指定路径的 reader 条目
        if let Some(entry) = path_map.get_mut(path) {
            // 根据 increment 参数确定要操作的 reader
            let reader_arc = if increment {
                &mut entry.0
            } else {
                &mut entry.1
            };

            // 如果存在 reader 实例
            if let Some(reader) = reader_arc {
                if reader.is::<CsvReader<T>>() {
                    // 尝试转换为具体类型并调用 unsubscribe
                    let typed_arc: Arc<CsvReader<T>> = reader.clone().downcast().map_err(|_| {
                        anyhow!("CSV reader instance type conversion failed for path: {:?}", path.display())
                    })?;
                    
                    // 调用 unsubscribe 方法移除订阅
                    typed_arc.unsubscribe(cert_key)?;
                    
                    // 如果这是最后一个订阅者，可以考虑清理资源
                    // 这里不清除
                    
                    return Ok(());
                }
                return Err(anyhow!("CSV reader instance type mismatch for path: {:?}", path.display()));
            }
        }
    }

    Err(anyhow!("CSV reader not found for path: {:?}", path.display()))
}

/// 创建或者返回已有的reader
pub fn get_or_create_dbf_reader<T>(path: &PathBuf, increment: bool, enc_type: EncType) -> Result<Arc<DbfReader<T>>>
where
    T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static
{
    let type_id = TypeId::of::<T>();
    let map = get_dbf_map();

    let path_lock = get_dbf_path_lock(path);
    let _guard = path_lock.lock().map_err(|e| anyhow::anyhow!("Failed to acquire path lock: {:?}", e))?;
    // 尝试读取已有 reader
    if let Some(mut path_map) = map.get_mut(&type_id) {
        if let Some(entry) = path_map.get_mut(path) {
            let reader_arc = if increment {
                &entry.0
            } else {
                &entry.1
            };

            if let Some(reader) = reader_arc {
                if reader.is::<DbfReader<T>>() {
                    let typed_arc: Arc<DbfReader<T>> = reader.clone().downcast().map_err(|_| {
                        anyhow!("DBF reader instance type conversion failed for path: {:?}", path.display())
                    })?;
                    ::ftlog::info!("return exist reader:{}", path.display());
                    return Ok(typed_arc);
                }
                return Err(anyhow!("DBF reader instance type mismatch for path: {:?}", path.display()));
            }
        }
    }

    // 创建新 reader 并存入全局 map
    let dbf_reader = DbfReader::new(path.clone(), increment, enc_type)?;
    let reader_arc: Arc<DbfReader<T>> = Arc::new(dbf_reader);

    // 转换为 trait object 存储
    let any_arc: Arc<dyn Any + Send + Sync> = reader_arc.clone();

    // 获取或初始化当前类型的子映射表
    let mut inner_map = map.entry(type_id).or_insert_with(FxHashMap::default);

    // 更新指定路径的 reader 条目
    let entry = inner_map.entry(path.clone()).or_insert((None, None));
    if increment {
        entry.0 = Some(any_arc);
    } else {
        entry.1 = Some(any_arc);
    }
    ::ftlog::info!("create new reader for {}", path.display());
    Ok(reader_arc)
}


/// 移除 DBF reader
pub fn remove_dbf_reader<T>(cert_key: CertKeyT, increment: bool, path: &PathBuf) -> Result<()> 
where
    T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static
{
    let type_id = TypeId::of::<T>();
    let map = get_dbf_map();

    let path_lock = get_dbf_path_lock(path);
    let _guard = path_lock.lock().map_err(|e| anyhow::anyhow!("Failed to acquire path lock: {:?}", e))?;

    // 获取类型对应的路径映射
    if let Some(mut path_map) = map.get_mut(&type_id) {
        // 查找指定路径的 reader 条目
        if let Some(entry) = path_map.get_mut(path) {
            // 根据 increment 参数确定要操作的 reader
            let reader_arc = if increment {
                &mut entry.0
            } else {
                &mut entry.1
            };

            // 如果存在 reader 实例
            if let Some(reader) = reader_arc {
                if reader.is::<DbfReader<T>>() {
                    // 尝试转换为具体类型并调用 unsubscribe
                    let typed_arc: Arc<DbfReader<T>> = reader.clone().downcast().map_err(|_| {
                        anyhow!("DBF reader instance type conversion failed for path: {:?}", path.display())
                    })?;
                    
                    // 调用 unsubscribe 方法移除订阅
                    typed_arc.unsubscribe(cert_key)?;
                    
                    // 如果这是最后一个订阅者，可以考虑清理资源
                    // 这里不清除
                    
                    return Ok(());
                }
                return Err(anyhow!("DBF reader instance type mismatch for path: {:?}", path.display()));
            }
        }
    }

    Err(anyhow!("DBF reader not found for path: {:?}", path.display()))
}