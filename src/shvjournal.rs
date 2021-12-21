use std::{cmp, fs};
use std::cmp::min;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::Bound::{Included, Unbounded};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek};
use std::num::ParseIntError;
use std::path::{Path, PathBuf};
use std::str::from_utf8;
//use std::str::pattern::Pattern;
use bitflags::bitflags;
use regex::Regex;
use log::log;
use chainpack::{DateTime, RpcValue, MetaMap, Map, List, ReadResult};

macro_rules! logShvJournalD {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Debug, $($arg)+)
    )
}

pub const DEFAULT_GET_LOG_RECORD_COUNT_LIMIT: usize = 100 * 1000;
pub const MAX_GET_LOG_RECORD_COUNT_LIMIT: usize = 1000 * 1000;
pub const DOMAIN_VAL_CHANGE: &str = "chng";

pub struct Options {
    pub journal_dir: PathBuf,
    pub file_size_limit: usize,
    pub dir_size_limit: usize,
}

impl Options {
}

bitflags! {
    struct EntryValueFlags: u8 {
        const SNAPSHOT     = 0b00000001;
        const SPONTANEOUS  = 0b00000010;
    }
}
impl EntryValueFlags {
    pub fn clear(&mut self) -> &mut EntryValueFlags {
        self.bits = 0;
        self
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    datetime: DateTime,
    path: String,
    value: RpcValue,
    short_time: Option<i32>,
    domain: String,
    value_flags: EntryValueFlags,
    user_id: String,
}
impl Entry {
    fn is_value_node_drop(&self) -> bool {
        // NIY
        false
    }
}
#[derive(Debug, Clone)]
pub struct GetLogParams {
    since: Option<DateTime>,
    until: Option<DateTime>,
    path_pattern: Option<String>,
    domain_pattern: Option<String>,
    record_count_limit: Option<usize>,
    with_snapshot: bool,
    with_path_dict: bool,
    is_since_last_entry: bool,
}
#[derive(Debug, Clone)]
pub struct LogHeader {
    log_version: i32,
    device_id: String,
    device_type: String,
    log_params: GetLogParams,
    datetime: DateTime,
    since: Option<DateTime>,
    until: Option<DateTime>,
    record_count: usize,
    record_count_limit: usize,
    record_count_limit_hit: bool,
    with_snapshot: bool,
    path_dict: Option<Map>,
    fields: List,
}
impl LogHeader {
    fn to_meta_map(self) -> MetaMap {
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
        mm.insert("recordCount", self.record_count.into());
        mm.insert("recordCountLimit", self.record_count_limit.into());
        mm.insert("recordCountLimitHit", self.record_count_limit_hit.into());
        mm.insert("withSnapshot", self.with_snapshot.into());
        mm.insert("withPathsDict", self.path_dict.is_some().into());
        if let Some(path_dict) = self.path_dict {
            mm.insert("pathsDict", path_dict.into());
        }
        mm.insert("fields", self.fields.into());
        mm
    }
}

struct JournalState {
    files: Vec<i64>,
    journal_dir_size: Option<usize>,
    last_file_size: usize,
    recent_epoch_msec: Option<i64>,
}

pub struct Journal {
    options: Options,
    state: JournalState,
}
impl JournalState {
    fn new() -> Self {
        JournalState {
            files: Vec::new(),
            journal_dir_size: None,
            last_file_size: 0,
            recent_epoch_msec: None,
        }
    }
    fn is_consistent(&self) -> bool {
        self.journal_dir_size.is_some()
    }
}
impl Journal {
    pub fn new(options: Options) -> crate::Result<Self> {
        fs::create_dir_all(&options.journal_dir)?;
        Ok (Self {
            options,
            state: JournalState::new(),
        })
    }
    pub fn append(&self, entry: &Entry) {
        match self.try_append(entry) {
            Ok(_) => {},
            Err(err) => {

            }
        }
    }
    pub fn getLog(&self, params: &GetLogParams) -> crate::Result<RpcValue> {
        logShvJournalD!("========================= getLog ==================");
        logShvJournalD!("params: {:?}", params);

        struct SnapshotContext {
            snapshot: BTreeMap<String, Entry>,
            last_entry_datetime: Option<DateTime>,
        };
        let mut snapshot_ctx = SnapshotContext {
            snapshot: BTreeMap::new(),
            last_entry_datetime: None,
        };

        let mut log = List::new();
        let mut path_dict = Map::new();
        let mut first_file_datetime: Option<DateTime> = None;
        let mut last_entry_datetime: Option<DateTime> = None;
        let mut is_snapshot_written = false;
        let record_count_limit = match params.record_count_limit {
            None => {DEFAULT_GET_LOG_RECORD_COUNT_LIMIT}
            Some(n) => {n}
        };
        let record_count_limit = min(record_count_limit, MAX_GET_LOG_RECORD_COUNT_LIMIT);
        let mut record_count_limit_hit = false;

        /// this ensure that there be only one copy of each path in memory
        let mut max_path_id = 0u32;
        let mut make_path_shared = |path: &str| -> RpcValue {
            match path_dict.get(path) {
                None => {
                    max_path_id += 1;
                    path_dict.insert(path.into(), max_path_id.into());
                    RpcValue::from(max_path_id)
                }
                Some(n) => {n.clone()}
            }
        };
        fn add_to_snapshot(snapshot_ctx: &mut SnapshotContext, entry: Entry) {
            if &entry.domain != DOMAIN_VAL_CHANGE {
                //shvDebug() << "remove not CHNG from snapshot:" << RpcValue(entry.toRpcValueMap()).toCpon();
                return;
            }
            snapshot_ctx.last_entry_datetime = Some(entry.datetime);
            if entry.is_value_node_drop() {
                let mut drop_keys: Vec<String> = vec![];
                let entry_path = entry.path.clone();
                let range = snapshot_ctx.snapshot.range(entry_path..);
                for (path, e) in range {
                    if path_starts_with(path, &entry.path) {
                        // it.key starts with key, then delete it from snapshot
                        drop_keys.push(path.into());
                    } else {
                        break;
                    }
                }
                for key in drop_keys {
                    snapshot_ctx.snapshot.remove(&key);
                }
            }
            else if entry.value.is_default_value() {
                // writing default value to the snapshot must erase previous value if any
                if let Some(_) = snapshot_ctx.snapshot.get(&entry.path) {
                    snapshot_ctx.snapshot.remove(&entry.path);
                }
            }
            else {
                //snapshot_ctx.last_entry_datetime = Some(*&entry.datetime);
                snapshot_ctx.snapshot.insert((&entry.path).clone(), entry);
            }
        };
        let mut append_log_entry = |entry: &Entry| {
            if log.len() >= record_count_limit {
                record_count_limit_hit = true;
                return false;
            }
            let mut rec = List::new();
            rec.push(entry.datetime.into());
            rec.push(make_path_shared(&entry.path));
            rec.push(entry.value.clone());
            rec.push(match entry.short_time {None => RpcValue::from(()), Some(n) => RpcValue::from(n)});
            rec.push(if entry.domain.is_empty() { RpcValue::from(DOMAIN_VAL_CHANGE) } else { RpcValue::from(&entry.domain) });
            rec.push((entry.value_flags.bits() as i32).into());
            rec.push(if entry.user_id.is_empty() { RpcValue::from(()) } else { RpcValue::from(&entry.user_id) });
            log.push(RpcValue::from(rec));
            last_entry_datetime = Some(entry.datetime);
            return true;
        };
        let write_snapshot = || -> crate::Result<bool> {
            //shvWarning() << "write_snapshot, snapshot size:" << snapshot_ctx.snapshot.size();
            logShvJournalD!("\t writing snapshot, record count: {}", &snapshot_ctx.snapshot.len());
            if snapshot_ctx.snapshot.is_empty() {
                return Ok(true);
            }
            //snapshot_ctx.is_snapshot_written = true;
            let snapshot_dt = if params.is_since_last_entry {
                snapshot_ctx.last_entry_datetime
            } else {
                params.since
            };
            let snapshot_dt = match snapshot_dt {
                None => { return Err("Cannot create snapshot datetime.".into()) }
                Some(dt) => { dt }
            };
            for (path, e) in &snapshot_ctx.snapshot {
                let mut entry = e.clone();
                entry.datetime = snapshot_dt;
                entry.value_flags.set(EntryValueFlags::SNAPSHOT, true);
                // erase EVENT flag in the snapshot values,
                // they can trigger events during reply otherwise
                entry.value_flags.set(EntryValueFlags::SPONTANEOUS, false);
                if !append_log_entry(&entry) {
                    return Ok(false);
                }
            }
            return Ok(true);
        };
        if !self.state.files.is_empty () {
            //std::vector<int64_t>::const_iterator first_file_it = journal_context.files.begin();
            //journal_start_msec = *first_file_it;
            let mut first_file_ix = 0;
            if self.state.files.len() > 0 {
                if let Some(params_since) = &params.since {
                    let params_since_msec = params_since.epoch_msec();
                    logShvJournalD!("since: {:?}", &params.since);
                    first_file_ix = match self.state.files.binary_search(&params_since_msec) {
                        Ok(i) => {
                            /// take exactly this file
                            logShvJournalD!("\texact match: {} {}", &i, &self.state.files[i]);
                            i
                        }
                        Err(i) => {
                            let i = i - 1;
                            logShvJournalD!("\tnot found, taking previous file: {} {}", i, &self.state.files[i]);
                            i
                        }
                    };
                    let first_file_millis = self.state.files[first_file_ix];
                    first_file_datetime = Some(DateTime::from_epoch_msec(first_file_millis));
                }
            }
            let path_pattern_regex = match &params.path_pattern {
                None => {None}
                Some(pattern) => {
                    Some(Regex::new(pattern)?)
                }
            };
            let domain_pattern_regex = match &params.domain_pattern {
                None => {None}
                Some(pattern) => {
                    Some(Regex::new(pattern)?)
                }
            };
            let is_path_match = |path: &str| {
                if let Some(rx) = &path_pattern_regex {
                    return rx.is_match(path);
                }
                return false;
            };
            let is_domain_match = |domain: &str| {
                if let Some(rx) = &domain_pattern_regex {
                    return rx.is_match(domain);
                }
                return false;
            };
            'scan_files: for file_ix in first_file_ix .. self.state.files.len() {
                let file_name = Self::epoch_msec_to_filename(&self.state.files[file_ix])?;
                logShvJournalD!("-------- opening file: {}", file_name);
                let reader = ShvJournalFileReader::new(&file_name)?;
                for entry in reader {
                    if let Err(err) = entry {
                        return Err(err)
                    }
                    let entry = entry.unwrap();
                    if !is_path_match(&entry.path) {
                        continue;
                    }
                    let before_since = params.since.is_some() && entry.datetime < params.since.unwrap();
                    let after_until = params.until.is_some() && params.until.unwrap() < entry.datetime;
                    if before_since {
                        //logShvJournalD!("\t SNAPSHOT entry: {}", entry);
                        add_to_snapshot(&mut snapshot_ctx, entry);
                    }
                    else if after_until {
                        break 'scan_files;
                    }
                    else {
                        if !is_snapshot_written {
                            is_snapshot_written = true;
                            if !write_snapshot()? {
                                break 'scan_files;
                            }
                        }
                        //logShvJournalD!("\t LOG entry: {}", &entry);
                        if !append_log_entry(&entry) {
                            break 'scan_files;
                        }
                    }
                }
            }
        }
        // snapshot should be written already
        // this is only case, when log is empty and
        // only snapshot shall be returned
        if !is_snapshot_written {
            is_snapshot_written = true;
            write_snapshot()?;
        }
        // if since is not specified in params
        // then use TS of first file used
        let log_since = if params.is_since_last_entry {
            snapshot_ctx.last_entry_datetime
        } else if let Some(dt) = params.since {
            params.since
        } else if let Some(dt) = first_file_datetime {
            first_file_datetime
        } else {
            None
        };
        let log_until = if record_count_limit_hit {
            last_entry_datetime
        } else if let Some(dt) = params.until {
            Some(dt)
        } else {
            last_entry_datetime
        };
        let mut ret = RpcValue::new(log);
        let log_header = LogHeader {
            log_version: 2,
            device_id: "".into(),
            device_type: "".into(),
            datetime: DateTime::now(),
            log_params: params.clone(),
            since: log_since,
            until: log_until,
            record_count: log.len(),
            record_count_limit,
            record_count_limit_hit,
            with_snapshot: is_snapshot_written,
            path_dict: if params.with_path_dict { Some(path_dict) } else { None },
            fields: vec!["timestamp".into(), "path".into(), "value".into(), "shortTime".into(), "domain".into(), "valueFlags".into(), "userId".into()],
        };
        ret.set_meta(log_header.to_meta_map());
        //logIShvJournal() << "result record cnt:" << log.size();
        Ok(ret)
    }
    fn try_append(&self, entry: &Entry) -> crate::Result<()> {
        panic!("NIY")
    }
    fn check_journal_dir(&self) -> crate::Result<()> {
        fs::create_dir_all(&self.options.journal_dir)?;
        Ok(())
    }
    fn check_journal_state(&mut self) -> crate::Result<()> {
        if self.state.is_consistent() {
            return Ok(());
        }
        self.state = JournalState::new();
        self.check_journal_dir()?;
        if self.options.journal_dir.is_dir() {
            let mut journal_dir_size = 0usize;
            for entry in fs::read_dir(&self.options.journal_dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    let filename = path.file_name().ok_or("Cannot convert Path to OsStr")?;
                    let filename = filename.to_str().ok_or("Cannot convert OsStr to &str")?;
                    let millis = Self::filename_to_epoch_msec(filename)?;
                    self.state.files.push(millis);
                    let md = fs::metadata(path)?;
                    journal_dir_size += md.len() as usize;
                    self.state.last_file_size = md.len() as usize;
                }
            }
            self.state.journal_dir_size = Some(journal_dir_size);
            self.state.files.sort();
        }
        Err(format!("Journal dir '{}' is not dir", self.options.journal_dir.to_str().unwrap_or("???")).into())
    }
    fn filename_to_epoch_msec(filename: &str) -> crate::Result<i64> {
        let dt= chrono::NaiveDateTime::parse_from_str(&filename[0 .. 23], "%Y-%m-%dT%H-%M-%S-%f")?;
        Ok(dt.timestamp_millis())
    }
    fn epoch_msec_to_filename(milis: &i64) -> crate::Result<String> {
        let dt= chrono::NaiveDateTime::from_timestamp_opt(milis / 1000, ((milis % 1000) * 1000) as u32)
            .ok_or(format!("Invalid epoch milis value: {}", milis))?;
        return Ok(dt.format("%Y-%m-%dT%H-%M-%S-%f").to_string());
    }
    fn find_last_entry_milis(file: &Path, millis: i64) -> crate::Result<i64> {
        const SPC: u8 = ' ' as u8;
        const LF: u8 = '\n' as u8;
        const CR: u8 = '\r' as u8;
        const TAB: u8 = '\t' as u8;
        const CHUNK_SIZE: usize = 1024;
        const TIMESTAMP_SIZE: usize = "2021-12-13T12-13-14-456Z".len() as usize;
        println!("file: {:?}", file);
        let mut buffer = vec![0u8; CHUNK_SIZE + TIMESTAMP_SIZE];
        let mut f = File::open(file)?;
        let file_size = f.metadata()?.len() as usize;
        if file_size == 0 {
            // empty file without data
            return Ok(millis);
        }
        let mut start_pos = if file_size < CHUNK_SIZE { 0 } else { file_size - CHUNK_SIZE } as usize;
        loop {
            f.seek(std::io::SeekFrom::Start(start_pos as u64))?;
            let mut read_count = f.read(&mut buffer)? as usize;
            // remove trailing blanks, like trailing '\n' in log file
            let mut pos = read_count;
            while pos > 0 {
                let c = buffer[pos - 1];
                if !(c == LF || c == CR || c == TAB || c == SPC || c == 0) {
                    break;
                }
                pos -= 1;
            }
            if pos < TIMESTAMP_SIZE {
                return Err(format!("Corrupted log file, cannot find complete timestamp from pos: {} of file size: {} in file: {:?}", start_pos, file_size, file).into())
            }
            while pos > 0 {
                let c = buffer[pos - 1];
                if c == LF || pos == 1 {
                    if pos == 1  {
                        pos = 0;
                    }
                    // try to read timestamp
                    let dt_str = from_utf8(&buffer[pos..pos + TIMESTAMP_SIZE])?;
                    println!("dt str: {}", dt_str);
                    let dt = chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S%.3fZ")
                        .map_err(|err| format!("Invalid date-time string: '{}', {}", dt_str, err))?;
                    return Ok(dt.timestamp_millis())
                }
                pos -= 1;
            }
            if start_pos == 0 {
                return Err(format!("Corrupted log file, cannot find complete timestamp from pos: {} of file size: {} in file: {:?}", start_pos, file_size, file).into())
            }
            start_pos -= cmp::max(CHUNK_SIZE, start_pos);
        }
    }
    pub fn test() {
        let res = Journal::find_last_entry_milis(&PathBuf::from("shvapp/tests/shvjournal/corrupted1.log2"), 0);
        match res {
            Ok(_) => {},
            Err(err) => {
                println!("Error: {}", err)
            }
        }
    }
}
struct ShvJournalFileReader {
}
impl ShvJournalFileReader {
    fn new(file_name: &str) -> crate::Result<Entries<BufReader<File>>> {
        let file = File::open(file_name)?;
        let reader = BufReader::new(file);
        Ok(Entries {
            buf: reader,
        })
    }
}
#[derive(Debug)]
pub struct Entries<B> {
    buf: B,
}

impl<B: BufRead> Iterator for Entries<B> {
    type Item = crate::Result<Entry>;

    fn next(&mut self) -> Option<crate::Result<Entry>> {
        //let next_field = |split: &mut str::Split<char>, err: &str| -> crate::Result<&str> {
        //    match split.next() {
        //        Some(str) => { Ok(str) }
        //        None => { if err.is_empty() { Ok("") } else { Err(err.into()) }}
        //    }
        //};
        let mut line = String::new();
        loop {
            match self.buf.read_line(&mut line) {
                Ok(0) => return None,
                Ok(_n) => {
                    while line.ends_with('\n') || line.ends_with('\0') {
                        line.pop();
                    }
                    if line.is_empty() {
                        // skip empty lines
                        continue;
                    }
                    let mut fields = line.split('\t');
                    let datetime = match fields.next() {
                        None => { return Some(Err("TimeStamp field missing".into())) }
                        Some(s) => {
                            match RpcValue::from_cpon(s) {
                                Ok(dt) => { dt.as_datetime() }
                                Err(err) => { return Some(Err(err.into())) }
                            }
                        }
                    };
                    let _uptime = fields.next();
                    let path = match fields.next() {
                        None => { return Some(Err("Path field missing".into())) }
                        Some(s) => {
                            if s.is_empty() { return Some(Err(format!("Path is empty, line: {}", line).into())); }
                            s.to_string()
                        }
                    };
                    let value = match fields.next() {
                        None => { return Some(Err("Value field missing".into())) }
                        Some(s) => { match RpcValue::from_cpon(s) {
                            Ok(v) => { v }
                            Err(err) => { return Some(Err(err.into())) }
                        }}
                    };
                    let short_time = fields.next().unwrap_or("");
                    let short_time = if short_time.is_empty() {
                        None
                    } else {
                        match short_time.parse::<i32>() {
                            Ok(t) => { Some(t) }
                            Err(err) => { return Some(Err(err.into())) }
                        }
                    };
                    let domain = fields.next().unwrap_or(DOMAIN_VAL_CHANGE);
                    let domain = if domain.is_empty() { DOMAIN_VAL_CHANGE } else { domain };
                    let domain = domain.to_string();
                    let value_flags = fields.next().unwrap_or("0");
                    let value_flags = match value_flags.parse::<u32>() {
                        Ok(i) => {
                            EntryValueFlags::from_bits_truncate(i as u8 )
                        }
                        Err(err) => { return Some(Err(err.into())) }
                    };
                    let user_id = fields.next().unwrap_or("").to_string();
                    return Some(Ok(Entry {
                        datetime,
                        path,
                        value,
                        short_time,
                        domain,
                        value_flags,
                        user_id,
                    }));
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

fn path_starts_with(path: &str, with: &str) -> bool {
    if path.starts_with(with) {
        let path_bytes = path.as_bytes();
        let with_bytes = with.as_bytes();
        if path_bytes.len() == with_bytes.len() {
            return true
        }
        if path_bytes[with_bytes.len()] == '/' as u8 {
            return true
        }
    }
    return false
}
#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;
    use crate::shvjournal::Journal;

    #[test]
    fn find_last_entry_milis_handles_empty_files() {
        let millis = Journal::find_last_entry_milis(&PathBuf::from("tests/shvjournal/empty.log2"), 123).unwrap();
        assert_eq!(millis, 123);
        let millis = Journal::find_last_entry_milis(&PathBuf::from("tests/shvjournal/oneline.log2"), 1234).unwrap();
        assert_eq!(millis, 1234);
        // "2021-11-21T16:54:35.023Z" == 1637513675023
        let millis0 = 1637513675023i64;
        let millis = Journal::find_last_entry_milis(&PathBuf::from("tests/shvjournal/regular.log2"), millis0).unwrap();
        assert_eq!(millis, millis0);
        let res = Journal::find_last_entry_milis(&PathBuf::from("tests/shvjournal/corrupted1.log2"), millis0).unwrap();
        //assert!(res.is_err());
    }
}