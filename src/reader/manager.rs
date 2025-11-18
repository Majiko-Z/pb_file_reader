use dashmap::DashMap;

use serde::Deserialize;

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use rustc_hash::FxHashMap;
use std::any::{Any, TypeId};
use std::path::{PathBuf};
use std::sync::{Arc};
use super::{subscribe_reader::*};
use crate::utils::model::*;


static CSV_READER_INSTANCES: OnceCell<
    DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>>,
> = OnceCell::new();

/// Initializes the CSV reader global map.
fn init_csv_map() -> DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>> {
    DashMap::new()
}

/// Gets a reference to the CSV reader global map.
fn get_csv_map() -> &'static DashMap<TypeId, FxHashMap<PathBuf, (Option<Arc<dyn Any + Send + Sync + 'static>>, Option<Arc<dyn Any + Send + Sync + 'static>>)>> {
    CSV_READER_INSTANCES.get_or_init(init_csv_map)
}

pub fn get_or_create_csv_reader<T>(path: PathBuf, increment: bool, enc_type: EncType) -> Result<Arc<CsvReader<T>>>
where
    T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static
{
    let type_id = TypeId::of::<T>();
    let map = get_csv_map();

    // 尝试读取已有 reader
    if let Some(mut path_map) = map.get_mut(&type_id) {
        if let Some(entry) = path_map.get_mut(&path) {
            let reader_arc = if increment {
                &entry.0
            } else {
                &entry.1
            };

            if let Some(reader) = reader_arc {
                if reader.is::<CsvReader<T>>() {
                    let typed_arc: Arc<CsvReader<T>> = reader.clone().downcast().map_err(|_| {
                        anyhow!("CSV reader instance type conversion failed for path: {:?}", path)
                    })?;
                    return Ok(typed_arc);
                }
                return Err(anyhow!("CSV reader instance type mismatch for path: {:?}", path));
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
    let entry = inner_map.entry(path).or_insert((None, None));
    if increment {
        entry.0 = Some(any_arc);
    } else {
        entry.1 = Some(any_arc);
    }

    Ok(reader_arc)
}