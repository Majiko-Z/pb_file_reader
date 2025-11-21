use super::FileListener;
use std::path::PathBuf;
use anyhow::Result;
use crate::utils::model::NotifyMeta;

#[allow(dead_code)]
struct NoopListener;
impl FileListener for NoopListener {
    fn add_watch(&self, _f_path: PathBuf) -> Result<NotifyMeta> {
        Err(anyhow::anyhow!("No file listener implemented"))
    }
    
    fn remove_watch(&self, _meta: &NotifyMeta) -> Result<()> {
        Ok(())
    }
    
    fn init(&self) -> Result<()> {
        Ok(())
    }
}