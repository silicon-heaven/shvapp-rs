use std::{cmp, fs};
use std::cmp::{max, min};
use std::collections::{BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use bitflags::bitflags;
use regex::Regex;
use log::log;
use chainpack::{DateTime, RpcValue, MetaMap, Map, List, Value};
use chainpack::rpcvalue::IMap;

#[allow(unused_macros)]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);

        // Find and cut the rest of the path
        match &name[..name.len() - 3].rfind(':') {
            Some(pos) => &name[pos + 1..name.len() - 3],
            None => &name[..name.len() - 3],
        }
    }};
}

#[allow(unused_macros)]
macro_rules! logShvJournalE {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Error, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvJournalW {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Warn, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvJournalI {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Info, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvJournalD {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Debug, $($arg)+)
    )
}
#[allow(unused_macros)]
macro_rules! logShvJournalT {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Trace, $($arg)+)
    )
}

pub const DEFAULT_GET_LOG_RECORD_COUNT_LIMIT: usize = 100 * 1000;
pub const MAX_GET_LOG_RECORD_COUNT_LIMIT: usize = 1000 * 1000;
pub const DOMAIN_VAL_CHANGE: &str = "chng";

bitflags! {
    struct EntryValueFlags: u8 {
        const SNAPSHOT     = 0b00000001;
        const SPONTANEOUS  = 0b00000010;
    }
}
impl EntryValueFlags {
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
impl GetLogParams {
    pub fn new() -> Self {
        GetLogParams {
            since: None,
            until: None,
            path_pattern: None,
            domain_pattern: None,
            record_count_limit: None,
            with_snapshot: false,
            with_path_dict: false,
            is_since_last_entry: false
        }
    }
    pub fn since(mut self, since: DateTime) -> Self {
        self.since = Some(since);
        self
    }
    pub fn since_last(mut self) -> Self {
        self.is_since_last_entry = true;
        self
    }
    pub fn until(mut self, until: DateTime) -> Self {
        self.until = Some(until);
        self
    }
    pub fn from_map(map: &Map) -> Self {
        pub fn get_map<'a>(map: &'a Map, key: &str, default: &'a RpcValue) -> &'a RpcValue {
            match map.get(key) {
                None => { default }
                Some(rv) => { rv }
            }
        }
        let (since, is_since_last_entry) = match map.get("since") {
            None => { (None, false) }
            Some(rv) => {
                match rv.value() {
                    Value::DateTime(dt) => { (Some(*dt), false) }
                    Value::String(str) if &str[..] == "last" => (None, true),
                    _ => { (None, false) }
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
            is_since_last_entry,
        }
    }
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
    path_dict: Option<BTreeMap<String, i32>>,
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
            let mut pd = IMap::new();
            for (path, key) in path_dict {
                pd.insert(key,path.into());
            }
            mm.insert("pathsDict", pd.into());
        }
        mm.insert("fields", self.fields.into());
        mm
    }
    fn from_meta_map(mm: &MetaMap) -> Self {
        let (device_id, device_type) = if let Some(device) = mm.value("device") {
            let m = device.as_map();
            (
                m.get("id").unwrap_or(&RpcValue::null()).to_string(),
                m.get("type").unwrap_or(&RpcValue::null()).to_string()
            )
        } else {
            ("".to_string(), "".to_string())
        };
        Self {
            log_version: 0,
            device_id: "".to_string(),
            device_type: "".to_string(),
            log_params: GetLogParams::from_map(mm.value("logParams").unwrap_or(&RpcValue::null()).as_map()),
            datetime: mm.value("dateTime").unwrap_or(&RpcValue::null()).as_datetime(),
            since: mm.value("since").map(|rv| rv.to_datetime()).flatten(),
            until: mm.value("until").map(|rv| rv.to_datetime()).flatten(),
            record_count: mm.value("recordCount").unwrap_or(&RpcValue::null()).as_usize(),
            record_count_limit: mm.value("recordCountLimit").unwrap_or(&RpcValue::null()).as_usize(),
            record_count_limit_hit: mm.value("recordCountLimitHit").unwrap_or(&RpcValue::null()).as_bool(),
            with_snapshot: mm.value("withSnapshot").unwrap_or(&RpcValue::null()).as_bool(),
            path_dict: None,
            fields: vec![]
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    pub journal_dir: String,
    pub file_size_limit: u64,
    pub dir_size_limit: u64,
}

struct JournalState {
    journal_dir: Option<PathBuf>,
    files: Vec<i64>,
    journal_dir_size: Option<u64>,
    last_file_size: u64,
    recent_epoch_datetime: Option<DateTime>,
}
impl JournalState {
    fn is_journal_dir_ok(&self) -> bool {
        self.journal_dir.is_some()
            && self.journal_dir_size.is_some()
    }
    fn is_consistent(&self) -> bool {
        self.is_journal_dir_ok() && self.recent_epoch_datetime.is_some()
    }
}

pub struct Journal {
    pub options: Options,
    state: JournalState,
}
impl Journal {
    pub fn new(options: Options) -> crate::Result<Self> {
        fs::create_dir_all(&options.journal_dir)?;
        Ok (Self {
            options,
            state: JournalState{
                journal_dir: None,
                files: vec![],
                journal_dir_size: None,
                last_file_size: 0,
                recent_epoch_datetime: None
            },
        })
    }
    pub fn append(&mut self, entry: &Entry) -> crate::Result<()> {
        let mut datetime = entry.datetime;
        if let Some(dt) = self.state.recent_epoch_datetime {
            datetime = max(datetime, dt);
        }
        if !self.state.is_consistent() {
            logShvJournalD!("Journal state not consistent, creating new one");
            self.create_journal_state(Some(datetime))?;
        }
        match self.try_append(entry) {
            Ok(_) => {Ok(())},
            Err(err) => {
                logShvJournalE!("Append log error: {}, trying to fix it by creating new journal state", err);
                self.create_journal_state(Some(datetime))?;
                self.try_append(entry)
            }
        }
    }
    fn try_append(&mut self, entry: &Entry) -> crate::Result<()> {
        if !self.state.is_consistent() {
            logShvJournalE!("Append log: Inconsistent journal state");
            return Err("Inconsistent journal state".into());
        }
        let datetime = max(entry.datetime, self.state.recent_epoch_datetime.unwrap());
        if self.state.files.is_empty() || self.state.last_file_size > self.options.file_size_limit {
            // create new file
            self.create_new_log_file(&datetime)?;
        }
        let last_file_epoch_msec = *self.state.files.last().unwrap();
        let last_file_path = self.datetime_to_path(&DateTime::from_epoch_msec(last_file_epoch_msec))?;
        //let mut f = File::open(last_file_path)?;
        //logShvJournalT!("writing kkt");
        //f.write_all("kkt\n".as_bytes())?;
        //logShvJournalT!("writing kkt OK");
        let mut f = OpenOptions::new()
            .write(true)
            .append(true)
            .open(last_file_path)?;
        let orig_file_size = f.metadata()?.len();
        let mut write_file = |data: &[u8], is_last_field: bool| -> crate::Result<()> {
            //logShvJournalT!("writing {:?}", data);
            f.write_all(data)?;
            //line_size += data.len();
            let tab: [u8; 1] = ['\t' as u8];
            let lf: [u8; 1] = ['\n' as u8];
            f.write_all(if is_last_field { &lf } else { &tab })?;
            //line_size += 1;
            Ok(())
        };
        let b = datetime.to_cpon_string();
        write_file(b.as_bytes(), false)?;
        let b: [u8; 0] = []; // uptime skipped
        write_file(&b, false)?;
        write_file(entry.path.as_bytes(), false)?;
        let b = entry.value.to_cpon();
        write_file(b.as_bytes(), false)?;
        if let Some(time) = entry.short_time {
            let b = time.to_string();
            write_file(b.as_bytes(), false)?;
        } else {
            let b: [u8; 0] = []; // no short time
            write_file(&b, false)?;
        }
        write_file(entry.domain.as_bytes(), false)?;
        let b = entry.value_flags.bits.to_string();
        write_file(b.as_bytes(), false)?;
        write_file(entry.user_id.as_bytes(), true)?;
        f.flush()?;
        let file_size = f.metadata()?.len();
        let dir_size = self.state.journal_dir_size.unwrap_or(0);
        let dir_size = dir_size + (file_size - orig_file_size);
        self.state.journal_dir_size = Some(dir_size);
        self.state.last_file_size = file_size;
        self.state.recent_epoch_datetime = Some(datetime);
        if file_size > self.options.file_size_limit {
            self.rotate_journal()?;
        }
        Ok(())
    }
    pub fn create_new_log_file(&mut self, datetime: &DateTime) -> crate::Result<()> {
        let file_path = self.datetime_to_path(datetime)?;
        File::create(file_path)?;
        Ok(())
    }
    fn rotate_journal(&mut self) -> crate::Result<()> {
        //logMShvJournal() << "Rotating journal of size:" << m_journalContext.journalSize;
        let mut file_cnt = self.state.files.len();
        let mut journal_size = self.state.journal_dir_size.unwrap_or(0);
        for file_msec in self.state.files.iter() {
            if file_cnt == 1 {
                // keep at least one file in case of bad limits configuration
                break;
            }
            if journal_size < self.options.dir_size_limit {
                break;
            }
            let file_path = self.datetime_to_path(&DateTime::from_epoch_msec(*file_msec))?;
            //logMShvJournal() << "\t deleting file:" << fn;
            let sz = fs::metadata(&file_path)?.len();
            fs::remove_file(&file_path)?;
            journal_size -= sz;
            file_cnt -= 1;
        }
        //logMShvJournal() << "New journal of size:" << m_journalContext.journalSize;
        self.create_journal_state(None)
    }
    pub fn get_log(&self, params: &GetLogParams) -> crate::Result<RpcValue> {
        logShvJournalD!("========================= getLog ==================");
        logShvJournalD!("params: {:?}", params);

        let record_count_limit = match params.record_count_limit {
            None => {DEFAULT_GET_LOG_RECORD_COUNT_LIMIT}
            Some(n) => {n}
        };
        let record_count_limit = min(record_count_limit, MAX_GET_LOG_RECORD_COUNT_LIMIT);
        struct SnapshotContext<'a> {
            params: &'a GetLogParams,
            snapshot: BTreeMap<String, Entry>,
            is_snapshot_written: bool,
            last_entry_datetime: Option<DateTime>,
        }
        struct LogContext<'a> {
            params: &'a GetLogParams,
            log: List,
            first_file_datetime: Option<DateTime>,
            last_entry_datetime: Option<DateTime>,
            record_count_limit: usize,
            record_count_limit_hit: bool,
            path_dict: BTreeMap<String, i32>,
            max_path_id: i32,
        }
        let mut snapshot_context = SnapshotContext {
            params,
            snapshot: BTreeMap::new(),
            is_snapshot_written: false,
            last_entry_datetime: None,
        };
        let mut log_context = LogContext {
            params,
            log: List::new(),
            first_file_datetime: None,
            last_entry_datetime: None,
            record_count_limit,
            record_count_limit_hit: false,
            path_dict: BTreeMap::new(),
            max_path_id: 0,
        };
        /// this ensure that there be only one copy of each path in memory
        fn make_path_shared(log_ctx: &mut LogContext, path: &str) -> i32 {
            match log_ctx.path_dict.get(path) {
                None => {
                    log_ctx.max_path_id += 1;
                    log_ctx.path_dict.insert(path.into(), log_ctx.max_path_id);
                    log_ctx.max_path_id
                }
                Some(n) => {*n}
            }
        }
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
                for (path, _) in range {
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
        }
        fn append_log_entry(log_ctx: &mut LogContext, entry: &Entry) -> bool {
            if log_ctx.log.len() >= log_ctx.record_count_limit {
                log_ctx.record_count_limit_hit = true;
                return false;
            }
            let mut rec = List::new();
            rec.push(entry.datetime.into());
            let path: RpcValue = if log_ctx.params.with_path_dict {
                make_path_shared(log_ctx, &entry.path).into()
            } else {
                RpcValue::from(&entry.path)
            };
            rec.push(path);
            rec.push(entry.value.clone());
            rec.push(match entry.short_time {None => RpcValue::from(()), Some(n) => RpcValue::from(n)});
            rec.push(if entry.domain.is_empty() { RpcValue::from(DOMAIN_VAL_CHANGE) } else { RpcValue::from(&entry.domain) });
            rec.push((entry.value_flags.bits() as i32).into());
            rec.push(if entry.user_id.is_empty() { RpcValue::from(()) } else { RpcValue::from(&entry.user_id) });
            log_ctx.log.push(RpcValue::from(rec));
            log_ctx.last_entry_datetime = Some(entry.datetime);
            return true;
        }
        fn write_snapshot(snapshot_ctx: &mut SnapshotContext, log_ctx: &mut LogContext) -> crate::Result<bool> {
            //shvWarning() << "write_snapshot, snapshot size:" << snapshot_ctx.snapshot.size();
            logShvJournalD!("\t writing snapshot, record count: {}", snapshot_ctx.snapshot.len());
            if snapshot_ctx.snapshot.is_empty() {
                return Ok(true);
            }
            if snapshot_ctx.snapshot.len() > log_ctx.record_count_limit {
                return Err(format!("Snapshot is larger than record count limit: {}", log_ctx.record_count_limit).into());
            };
            let snapshot_dt = if snapshot_ctx.params.is_since_last_entry {
                snapshot_ctx.last_entry_datetime
            } else {
                snapshot_ctx.params.since
            };
            let snapshot_dt = match snapshot_dt {
                None => { return Err("Cannot create snapshot datetime.".into()) }
                Some(dt) => { dt }
            };
            for (_, e) in &snapshot_ctx.snapshot {
                let mut entry = e.clone();
                entry.datetime = snapshot_dt;
                entry.value_flags.set(EntryValueFlags::SNAPSHOT, true);
                // erase EVENT flag in the snapshot values,
                // they can trigger events during reply otherwise
                entry.value_flags.set(EntryValueFlags::SPONTANEOUS, false);
                if !append_log_entry(log_ctx, &entry) {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
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
                            // take exactly this file
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
                    log_context.first_file_datetime = Some(DateTime::from_epoch_msec(first_file_millis));
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
                    rx.is_match(path)
                } else {
                    true
                }
            };
            let is_domain_match = |domain: &str| {
                if let Some(rx) = &domain_pattern_regex {
                    rx.is_match(domain)
                } else {
                    true
                }
            };
            'scan_files: for file_ix in first_file_ix .. self.state.files.len() {
                let dt = DateTime::from_epoch_msec(self.state.files[file_ix]);
                let file_path = self.datetime_to_path(&dt)?;
                logShvJournalD!("-------- opening file: {:?}", &file_path);
                let reader = ShvJournalFileReader::new(&file_path)?;
                for entry in reader {
                    if let Err(err) = entry {
                        return Err(err)
                    }
                    let entry = entry.unwrap();
                    if !is_path_match(&entry.path) {
                        continue;
                    }
                    if !is_domain_match(&entry.domain) {
                        continue;
                    }
                    let before_since = params.since.is_some() && entry.datetime < params.since.unwrap();
                    let after_until = params.until.is_some() && params.until.unwrap() < entry.datetime;
                    if before_since {
                        //logShvJournalD!("\t SNAPSHOT entry: {}", entry);
                        add_to_snapshot(&mut snapshot_context, entry);
                    }
                    else if after_until {
                        break 'scan_files;
                    }
                    else {
                        if !snapshot_context.is_snapshot_written {
                            snapshot_context.is_snapshot_written = true;
                            if !write_snapshot(&mut snapshot_context, &mut log_context)? {
                                break 'scan_files;
                            }
                        }
                        //logShvJournalD!("\t LOG entry: {}", &entry);
                        if !append_log_entry(&mut log_context, &entry) {
                            break 'scan_files;
                        }
                    }
                }
            }
        }
        // snapshot should be written already
        // this is only case, when log is empty and
        // only snapshot shall be returned
        if !snapshot_context.is_snapshot_written {
            snapshot_context.is_snapshot_written = true;
            write_snapshot(&mut snapshot_context,&mut log_context)?;
        }
        // if since is not specified in params
        // then use TS of first file used
        let log_since = if params.is_since_last_entry {
            log_context.last_entry_datetime
        } else if params.since.is_some() {
            params.since
        } else if log_context.first_file_datetime.is_some() {
            log_context.first_file_datetime
        } else {
            None
        };
        let log_until = if log_context.record_count_limit_hit {
            log_context.last_entry_datetime
        } else if let Some(dt) = params.until {
            Some(dt)
        } else {
            log_context.last_entry_datetime
        };
        let log_header = LogHeader {
            log_version: 2,
            device_id: "".into(),
            device_type: "".into(),
            datetime: DateTime::now(),
            log_params: params.clone(),
            since: log_since,
            until: log_until,
            record_count: log_context.log.len(),
            record_count_limit,
            record_count_limit_hit: log_context.record_count_limit_hit,
            with_snapshot: snapshot_context.is_snapshot_written,
            path_dict: if params.with_path_dict { Some(log_context.path_dict) } else { None },
            fields: vec!["timestamp".into(), "path".into(), "value".into(), "shortTime".into(), "domain".into(), "valueFlags".into(), "userId".into()],
        };
        let mut ret = RpcValue::from(log_context.log);
        ret.set_meta(log_header.to_meta_map());
        //logIShvJournal() << "result record cnt:" << log.size();
        Ok(ret)
    }

    fn create_journal_state(&mut self, first_file_timestamp: Option<DateTime>) -> crate::Result<()> {
        logShvJournalD!("{}", function!());
        self.state = JournalState {
            journal_dir: None,
            files: Vec::new(),
            journal_dir_size: None,
            last_file_size: 0,
            recent_epoch_datetime: None,
        };
        let journal_dir: PathBuf = self.options.journal_dir.clone().into();
        fs::create_dir_all(&journal_dir)?;
        let mut journal_dir_size: u64 = 0;
        for entry in fs::read_dir(&journal_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                let millis = self.path_to_datetime(&path)?.epoch_msec();
                logShvJournalT!("adding journal file: {:?} -> millis: {}", path, millis);
                self.state.files.push(millis);
                let md = fs::metadata(path)?;
                journal_dir_size += md.len();
                self.state.last_file_size = md.len();
            }
        }
        self.state.journal_dir = Some(journal_dir);
        self.state.journal_dir_size = Some(journal_dir_size);
        self.state.files.sort();
        if let Some(n) = self.state.files.last() {
            let last_file = self.datetime_to_path(&DateTime::from_epoch_msec(*n))?;
            logShvJournalT!("last file: {:?} -> millis: {}", last_file, *n);
            let recent_datetime = Self::find_last_entry_datetime(&last_file)?.unwrap_or(DateTime::from_epoch_msec(*n));
            self.state.recent_epoch_datetime = Some(recent_datetime);
        } else {
            if let Some(dt) = first_file_timestamp {
                logShvJournalD!("Journal dir is empty, creating first file: {:?}", self.datetime_to_path(&dt));
                self.create_new_log_file(&dt)?;
                logShvJournalD!("Creating journal state again");
                return self.create_journal_state(None);
            } else {
                return Err("Cannot create first journal file, timestamp was not specified".into());
            }
        }
        Ok(())
    }
    fn path_to_datetime(&self, path: &std::path::Path) -> crate::Result<DateTime> {
        let base_name = path.file_stem().ok_or(format!("Path '{:?}' is not valid log file path.", path))?;
        let base_name = base_name.to_str().ok_or(format!("Cannot convert OsStr '{:?}' to &str", base_name))?;
        Self::file_base_name_to_datetime(base_name)
    }
    fn datetime_to_path(&self, datetime: &DateTime) -> crate::Result<PathBuf> {
        let file_name = Self::datetime_to_file_base_name(datetime)? + ".log2";
        let mut path = self.state.journal_dir.clone().ok_or("Journal dir is not set!")?;
        path.push(file_name);
        Ok(path)
    }
    fn file_base_name_to_datetime(filename: &str) -> crate::Result<DateTime> {
        let dt= chrono::NaiveDateTime::parse_from_str(&filename[0 .. 23], "%Y-%m-%dT%H-%M-%S-%3f")?;
        Ok(DateTime::from_naive_datetime(&dt))
    }
    fn datetime_to_file_base_name(datetime: &DateTime) -> crate::Result<String> {
        let millis = datetime.epoch_msec();
        let dt= chrono::NaiveDateTime::from_timestamp_opt(millis / 1000, ((millis % 1000) * 1000000) as u32)
            .ok_or(format!("Invalid epoch millis value: {}", millis))?;
        return Ok(dt.format("%Y-%m-%dT%H-%M-%S-%3f").to_string());
    }
    fn find_last_entry_datetime(file: &Path) -> crate::Result<Option<DateTime>> {
        const SPC: u8 = ' ' as u8;
        const LF: u8 = '\n' as u8;
        const CR: u8 = '\r' as u8;
        const TAB: u8 = '\t' as u8;
        const CHUNK_SIZE: usize = 1024;
        const TIMESTAMP_SIZE: usize = "2021-12-13T12-13-14-456Z".len() as usize;
        let mut buffer = vec![0u8; CHUNK_SIZE + TIMESTAMP_SIZE];
        let mut f = File::open(file)?;
        let file_size = f.metadata()?.len() as usize;
        //println!("DDD path: {:?}, file_size: {:?}", file, file_size); 
        if file_size == 0 {
            // empty file without data
            return Ok(None);
        }
        let mut start_pos = if file_size < CHUNK_SIZE { 0 } else { file_size - CHUNK_SIZE } as usize;
        loop {
            f.seek(std::io::SeekFrom::Start(start_pos as u64))?;
            let read_count = f.read(&mut buffer)? as usize;
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
                    let dt = DateTime::from_iso_str(dt_str)?;
                    //println!("{}:{} DDD dt str: {}", file!(), line!(), dt_str);
                    //let dt = chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S%.3fZ")
                     //   .map_err(|err| format!("Invalid date-time string: '{}', {}", dt_str, err))?;
                    return Ok(Some(dt))
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
        let res = Journal::find_last_entry_datetime(&PathBuf::from("shvapp/tests/shvjournal/corrupted1.log2"));
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
    fn new(file_path: &Path) -> crate::Result<Entries<BufReader<File>>> {
        let file = File::open(file_path)?;
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
    use std::fs::remove_dir_all;
    use std::path::PathBuf;
    use flexi_logger::{colored_default_format, colored_detailed_format, default_format, Logger};
    //use env_logger::Logger;
    use log::{debug, error, info, trace, warn};
    use chainpack::{DateTime, make_map, rpcvalue, RpcValue};
    use crate::shvjournal::{Entry, GetLogParams, Journal, Options};

    fn init_log() {
        //let _ = env_logger::builder().is_test(true).try_init();
        let is_detailed = false;
        match Logger::try_with_env() {
            Ok(logger) => {
                //println!("flexi logger init OK");
                match logger.format(if is_detailed {colored_detailed_format} else {colored_default_format}).start() {
                    Ok(_) => {
                        //println!("flexi logger started OK")
                    }
                    Err(_err) => {
                        // ignore double init error
                        //println!("flexi logger started ERROR: {:?}", err)
                    }
                }
            }
            Err(err) => { println!("flexi logger started ERROR: {:?}", err) }
        }
    }
    fn create_journal() -> crate::Result<Journal> {
        let journal_dir = "/tmp/shv-rs/journal-test";
        remove_dir_all(journal_dir)?;
        Journal::new(Options {
            journal_dir: journal_dir.to_string(),
            file_size_limit: 1024,
            dir_size_limit: 1024 * 5,
        })
    }

    #[test]
    fn tst_find_last_entry_datetime() {
        init_log();
        info!("=== Starting test: {}", function!());
        //trace!("{}:{} DDD trace", file!(), line!());
        //debug!("{}:{} DDD trace", file!(), line!());
        //info!("{}:{} DDD trace", file!(), line!());
        //warn!("{}:{} DDD trace", file!(), line!());
        //error!("{}:{} DDD T1", file!(), line!());
        let dt = Journal::find_last_entry_datetime(&PathBuf::from("tests/shvjournal/empty.log2")).unwrap();
        assert_eq!(dt, None);
        let dt = Journal::find_last_entry_datetime(&PathBuf::from("tests/shvjournal/oneline.log2")).unwrap();
        assert_eq!(dt, Some(DateTime::from_epoch_msec(1234)));
        let millis0 = 1637513675023i64;
        let dt = Journal::find_last_entry_datetime(&PathBuf::from("tests/shvjournal/regular.log2")).unwrap();
        assert_eq!(dt, Some(DateTime::from_epoch_msec(millis0)));
        let dt = Journal::find_last_entry_datetime(&PathBuf::from("tests/shvjournal/corrupted1.log2"));
        assert!(dt.is_err());
    }

    #[test]
    fn tst_write_log() -> crate::Result<()> {
        init_log();
        info!("=== Starting test: {}", function!());
        let mut journal = create_journal()?;
        let entry = Entry::new(None, "tc/TC01/status/occupied", true.into());
        journal.append(&entry)?;
        let entry = Entry::new(None, "tc/TC01/status/error", false.into());
        journal.append(&entry)?;
        let entry = Entry::new(None, "tc/TC02/status/occupied", true.into());
        journal.append(&entry)?;
        let entry = Entry::new(None, "tc/TC02/status/error", false.into());
        journal.append(&entry)?;
        let vehicle_detected = make_map![ "vehicleId" => 1234, "direction" => "left" ];
        let entry = Entry::new(None, "vetra/VET01/vehicleDetected", vehicle_detected.into());
        journal.append(&entry)?;
        let params = GetLogParams::new();//.since(DateTime::now().add_days(-1));
        let log = journal.get_log(&params)?;
        Ok(())
    }
}