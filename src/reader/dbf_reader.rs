use serde::Deserialize;
use crate::utils::model::{DBF};
use crate::reader::subscribe_reader::SubsReader;

impl <T: for<'a> Deserialize<'a> + Clone + Send + Sync + 'static> SubsReader<T, DBF> {
}