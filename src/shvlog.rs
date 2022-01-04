use std::collections::BTreeMap;
use bitflags::bitflags;
use chainpack::{DateTime, List, Map, MetaMap, RpcValue, rpcvalue, Value};
use chainpack::rpcvalue::IMap;

#[allow(unused_macros)]
macro_rules! logShvLogE {
    ($($arg:tt)+) => (
        log!(target: "ShvLog", log::Level::Error, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvLogW {
    ($($arg:tt)+) => (
        log!(target: "ShvLog", log::Level::Warn, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvLogI {
    ($($arg:tt)+) => (
        log!(target: "ShvLog", log::Level::Info, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvLogD {
    ($($arg:tt)+) => (
        log!(target: "ShvLog", log::Level::Debug, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvLogT {
    ($($arg:tt)+) => (
        log!(target: "ShvLog", log::Level::Trace, $($arg)+)
    )
}

pub const DOMAIN_VAL_CHANGE: &str = "chng";

pub enum LogRecordColumn {
    DateTime = 0,
    UpTime,
    Path,
    Value,
    ShortTime,
    Domain,
    ValueFlags,
    UserId,
}

bitflags! {
    pub struct EntryValueFlags: u8 {
        const SNAPSHOT     = 0b00000001;
        const SPONTANEOUS  = 0b00000010;
    }
}
impl EntryValueFlags {
    pub fn clear(&mut self) {
        *self = EntryValueFlags::empty();
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub datetime: DateTime,
    pub path: String,
    pub value: RpcValue,
    pub short_time: Option<i32>,
    pub domain: String,
    pub value_flags: EntryValueFlags,
    pub user_id: String,
}
impl Entry {
    pub fn new(datetime: Option<DateTime>, path: &str, value: RpcValue) -> Entry {
        Entry {
            datetime: match datetime {
                None => {  DateTime::now() }
                Some(dt) => { dt }
            },
            path: path.to_string(),
            value,
            short_time: None,
            domain: DOMAIN_VAL_CHANGE.to_string(),
            value_flags: EntryValueFlags::empty(),
            user_id: "".to_string()
        }
    }
    pub fn is_value_node_drop(&self) -> bool {
        // NIY
        false
    }
    pub fn from_rpcvalue(record: &RpcValue) -> crate::Result<Self> {
        let record = record.as_list();
        let datetime = match record.get(LogRecordColumn::DateTime as usize) {
            None => { return Err("Record does not contain DateTime column".into()) }
            Some(rv) => { rv.as_datetime() }
        };
        // uptime is not used anymore
        let _uptime = record.get(LogRecordColumn::UpTime as usize);
        let path = match record.get(LogRecordColumn::Path as usize) {
            None => { return Err("Record does not contain Path column".into()) }
            Some(rv) => { rv.to_string() }
        };
        let value = match record.get(LogRecordColumn::Value as usize) {
            None => { return Err("Record does not contain Value column".into()) }
            Some(rv) => { rv }
        };
        let short_time = match record.get(LogRecordColumn::ShortTime as usize) {
            None => { None }
            Some(rv) => { Some(rv.as_int() as i32) }
        };
        let domain = match record.get(LogRecordColumn::Domain as usize) {
            None => { DOMAIN_VAL_CHANGE.to_string() }
            Some(rv) => { rv.to_string() }
        };
        let value_flags = match record.get(LogRecordColumn::ValueFlags as usize) {
            None => { EntryValueFlags::empty() }
            Some(rv) => { EntryValueFlags::from_bits_truncate(rv.as_u64() as u8 ) }
        };
        let user_id = match record.get(LogRecordColumn::UserId as usize) {
            None => { "".to_string() }
            Some(rv) => { rv.to_string() }
        };
        Ok(Entry {
            datetime,
            path,
            value: value.clone(),
            short_time,
            domain,
            value_flags,
            user_id
        })
    }
}

pub const DEFAULT_GET_LOG_RECORD_COUNT_LIMIT: usize = 100 * 1000;
pub const MAX_GET_LOG_RECORD_COUNT_LIMIT: usize = 1000 * 1000;

#[derive(Debug, Clone)]
pub enum GetLogSince {
    Some(DateTime),
    LastEntry,
    None,
}

#[derive(Debug, Clone)]
pub struct GetLogParams {
    pub since: GetLogSince,
    pub until: Option<DateTime>,
    pub path_pattern: Option<String>,
    pub domain_pattern: Option<String>,
    pub record_count_limit: Option<usize>,
    pub with_snapshot: bool,
    pub with_path_dict: bool,
}
impl Default for GetLogParams {
    fn default() -> Self {
        GetLogParams {
            since: GetLogSince::None,
            until: None,
            path_pattern: None,
            domain_pattern: None,
            record_count_limit: None,
            with_snapshot: false,
            with_path_dict: true,
        }
    }
}
impl GetLogParams {
    pub fn since(mut self, since: DateTime) -> Self { self.since = GetLogSince::Some(since); self }
    pub fn since_last_entry(mut self) -> Self { self.since = GetLogSince::LastEntry; self }
    pub fn until(mut self, until: DateTime) -> Self { self.until = Some(until); self }
    pub fn record_count_limit(mut self, n: usize) -> Self { self.record_count_limit = Some(n); self }
    pub fn with_snapshot(mut self, b: bool) -> Self { self.with_snapshot = b; self }
    pub fn with_path_dict(mut self, b: bool) -> Self { self.with_path_dict = b; self }
    pub fn from_map(map: &Map) -> Self {
        pub fn get_map<'a>(map: &'a Map, key: &str, default: &'a RpcValue) -> &'a RpcValue {
            match map.get(key) {
                None => { default }
                Some(rv) => { rv }
            }
        }
        let since = match map.get("since") {
            None => { GetLogSince::None }
            Some(rv) => {
                match rv.value() {
                    Value::DateTime(dt) => GetLogSince::Some(*dt),
                    Value::String(str) if &str[..] == "last" => GetLogSince::LastEntry,
                    _ => GetLogSince::None,
                }
            }
        };
        let until = map.get("until").map(|rv| rv.to_datetime()).flatten();
        let path_pattern = map.get("pathPattern").map(|rv| rv.to_string());
        Self {
            since,
            until,
            path_pattern,
            domain_pattern: map.get("domainPattern").map(|rv| rv.to_string()),
            record_count_limit: map.get("recordCountLimit").map(|rv| rv.as_usize()),
            with_snapshot: get_map(map, "withSnapshot", &RpcValue::from(false)).as_bool(),
            with_path_dict: get_map(map, "withPathsDict", &RpcValue::from(false)).as_bool(),
        }
    }
    pub fn to_map(&self) -> Map {
        let mut map = Map::new();
        match &self.since {
            GetLogSince::Some(dt) => {
                map.insert("since".into(), (*dt).into());
            }
            GetLogSince::LastEntry => {
                map.insert("since".into(), "last".into());
            }
            GetLogSince::None => {}
        }
        if let Some(dt) = &self.until {
            map.insert("until".into(), (*dt).into());
        }
        if let Some(s) = &self.path_pattern {
            map.insert("pathPattern".into(), s.clone().into());
            map.insert("pathPatternType".into(), "regex".into());
        }
        if let Some(s) = &self.domain_pattern {
            map.insert("domainPattern".into(), s.clone().into());
        }
        if let Some(n) = &self.record_count_limit {
            map.insert("recordCountLimit".into(), (*n).into());
        }
        map.insert("withSnapshot".into(), self.with_snapshot.into());
        map.insert("withPathsDict".into(), self.with_path_dict.into());
        map
    }
}

#[derive(Debug, Clone)]
pub struct LogHeaderField {
    pub name: String,
}
impl LogHeaderField {
    pub fn to_rpcvalue(&self) -> RpcValue {
        let mut m = Map::new();
        m.insert("name".into(), self.name.clone().into());
        m.into()
    }
}
pub type PathDict = BTreeMap<i32, String>;
#[derive(Debug, Clone)]
pub struct  LogHeader {
    pub log_version: i32,
    pub device_id: String,
    pub device_type: String,
    pub log_params: GetLogParams,
    pub datetime: DateTime,
    pub since: Option<DateTime>,
    pub until: Option<DateTime>,
    pub record_count: usize,
    pub snapshot_count: usize,
    pub record_count_limit: usize,
    pub record_count_limit_hit: bool,
    pub with_snapshot: bool,
    pub path_dict: Option<PathDict>,
    pub fields: Vec<LogHeaderField>,
}
impl LogHeader {
    pub(crate) fn to_meta_map(self) -> MetaMap {
        let mut mm = MetaMap::new();
        mm.insert("logVersion", RpcValue::from(self.log_version));
        let mut device = Map::new();
        if !self.device_id.is_empty() {
            device.insert("id".into(), self.device_id.into());
        }
        if !self.device_type.is_empty() {
            device.insert("type".into(), self.device_type.into());
        }
        if !device.is_empty() {
            mm.insert("device", device.into());
        }
        mm.insert("since", if let Some(dt) = self.since { dt.into() } else { ().into() });
        mm.insert("until", if let Some(dt) = self.until { dt.into() } else { ().into() });
        mm.insert("logParams", self.log_params.to_map().into());
        mm.insert("recordCount", self.record_count.into());
        mm.insert("snapshotCount", self.snapshot_count.into());
        mm.insert("recordCountLimit", self.record_count_limit.into());
        mm.insert("recordCountLimitHit", self.record_count_limit_hit.into());
        mm.insert("withSnapshot", self.with_snapshot.into());
        mm.insert("withPathsDict", self.path_dict.is_some().into());
        if let Some(path_dict) = self.path_dict {
            let mut pd = IMap::new();
            for (id, path) in path_dict {
                pd.insert(id,path.into());
            }
            mm.insert("pathsDict", pd.into());
        }
        let fields: List = self.fields.iter().map(|i| i.to_rpcvalue()).collect();
        mm.insert("fields", fields.into());
        mm
    }
    pub(crate) fn from_meta_map(mm: &MetaMap) -> Self {
        let (device_id, device_type) = if let Some(device) = mm.get("device") {
            let m = device.as_map();
            (
                m.get("id").unwrap_or(&RpcValue::null()).to_string(),
                m.get("type").unwrap_or(&RpcValue::null()).to_string()
            )
        } else {
            ("".to_string(), "".to_string())
        };
        let path_dict = if let Some(rv) = mm.get("pathsDict") {
            let dict = rv.as_imap();
            let mut path_dict: PathDict = PathDict::new();
            for (k, v) in dict {
                path_dict.insert(*k, v.to_string());
            }
            Some(path_dict)
        } else {
            None
        };
        Self {
            log_version: 0,
            device_id,
            device_type,
            log_params: GetLogParams::from_map(mm.get("logParams").unwrap_or(&RpcValue::null()).as_map()),
            datetime: mm.get("dateTime").unwrap_or(&RpcValue::null()).as_datetime(),
            since: mm.get("since").map(|rv| rv.to_datetime()).flatten(),
            until: mm.get("until").map(|rv| rv.to_datetime()).flatten(),
            snapshot_count: mm.get("snapshotCount").unwrap_or(&RpcValue::null()).as_usize(),
            record_count: mm.get("recordCount").unwrap_or(&RpcValue::null()).as_usize(),
            record_count_limit: mm.get("recordCountLimit").unwrap_or(&RpcValue::null()).as_usize(),
            record_count_limit_hit: mm.get("recordCountLimitHit").unwrap_or(&RpcValue::null()).as_bool(),
            with_snapshot: mm.get("withSnapshot").unwrap_or(&RpcValue::null()).as_bool(),
            path_dict,
            fields: if let Some(rv) = mm.get("fields") {
                let mut fields: Vec<LogHeaderField> = Vec::new();
                for field in rv.as_list() {
                    fields.push(LogHeaderField {
                        name: field.get("name").unwrap_or(&RpcValue::from("")).to_string(),
                    })
                }
                fields
            } else {
                vec![]
            },
        }
    }
}

pub struct LogReader {
}
impl LogReader {
    pub fn new(log: &RpcValue) -> crate::Result<LogEntries> {
        let records = log.as_list();
        Ok(LogEntries {
            records,
            pos: 0,
        })
    }
}
#[derive(Debug)]
pub struct LogEntries<'a> {
    records: &'a rpcvalue::List,
    pos: usize,
}

impl Iterator for LogEntries<'_> {
    type Item = crate::Result<Entry>;

    fn next(&mut self) -> Option<crate::Result<Entry>> {
        return match self.records.get(self.pos) {
            None => { None }
            Some(rv) => {
                match Entry::from_rpcvalue(rv) {
                    Ok(e) => {
                        self.pos += 1;
                        Some(Ok(e))
                    }
                    Err(err) => { Some(Err(err.into())) }
                }
            }
        }
    }
}