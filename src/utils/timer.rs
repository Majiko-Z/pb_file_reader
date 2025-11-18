use coarsetime::{Clock};
pub fn get_coarse_timestamp_ms() -> u64 {
    // 使用 Clock::recent_since_epoch() 获取自 Unix 纪元以来的时间
    let duration = Clock::recent_since_epoch();
    // 转换为毫秒
    duration.as_millis() as u64
}