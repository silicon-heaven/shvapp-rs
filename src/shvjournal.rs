use std::{cmp, fs};
use std::cmp::{max, min};
use std::collections::{BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
//use std::str::pattern::Pattern;
use bitflags::bitflags;
use regex::Regex;
use log::log;
use chainpack::{DateTime, RpcValue, MetaMap, Map, List};
use chainpack::rpcvalue::IMap;

macro_rules! logShvJournalD {
    ($($arg:tt)+) => (
        log!(target: "ShvJournal", log::Level::Debug, $($arg)+)
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
}

pub struct Options {
    pub journal_dir: String,
    pub file_size_limit: usize,
    pub dir_size_limit: usize,
}

struct JournalState {
    journal_dir: Option<PathBuf>,
    files: Vec<i64>,
    journal_dir_size: Option<usize>,
    last_file_size: usize,
    recent_epoch_msec: Option<i64>,
}

pub struct Journal {
    pub options: Options,
    state: JournalState,
}
impl JournalState {
    fn is_consistent(&self) -> bool {
        self.journal_dir.is_some()
            && self.journal_dir_size.is_some()
            && self.recent_epoch_msec.is_some()
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
    pub fn append(&mut self, entry: &Entry) -> crate::Result<()> {
        match self.try_append(entry, false) {
            Ok(_) => {Ok(())},
            Err(_) => {
                self.try_append(entry, true)
            }
        }
    }
    fn try_append(&mut self, entry: &Entry, force_create_state: bool) -> crate::Result<()> {
        if force_create_state || !self.state.is_consistent() {
            self.create_journal_state()?;
        }
        if !self.state.is_consistent() {
            return Err("Inconsistent journal state".into());
        }
        let epoch_msec = max(entry.datetime.epoch_msec(), self.state.recent_epoch_msec.unwrap_or(0));
        if self.state.files.empty() || self.state.last_file_size > self.options.file_size_limit {
            // create new file
            self.create_new_log_file(epoch_msec)?;
        }
        let last_file_epoch_msec = self.state.files.last().unwrap();
        let last_file_name = Self::epoch_msec_to_filename(last_file_epoch_msec)?;
        let mut f = File::open(last_file_name)?;

        let mut line_size: usize = 0;
        let write_file = |data: &[u8], is_last_field: bool| -> crate::Result<()> {
            f.write_all(data)?;
            line_size += data.len();
            let tab: [u8; 1] = ['\t' as u8];
            let lf: [u8; 1] = ['\n' as u8];
            f.write_all(if is_last_field { &lf } else { &tab })?;
            line_size += 1;
            Ok(())
        };
        let b = DateTime::from_epoch_msec(epoch_msec).to_cpon_string().as_bytes();
        write_file(b, false)?;
        let b: [u8; 1] = [0]; // uptime skipped
        write_file(&b, false)?;
        write_file(entry.path.as_bytes(), false)?;
        let b = entry.value.to_cpon().as_bytes();
        write_file(b, false)?;
        if let Some(time) = entry.short_time {
            let b = time.to_string().as_bytes();
            write_file(b, false)?;
        } else {
            let b: [u8; 0] = []; // no short time
            write_file(&b, false)?;
        }
        write_file(entry.domain.as_bytes(), false)?;
        let b = entry.value_flags.bits.to_string().as_bytes();
        write_file(b, false)?;
        write_file(entry.user_id.as_bytes(), true)?;
        f.flush();
        self.state.recent_epoch_msec = Some(epoch_msec);
        self.state.last_file_size += line_size;
        self.state.journal_dir_size += line_size;
        //if(m_journalContext.journalSize > m_journalSizeLimit) {
        //    rotateJournal();
        //}
    }
    fn create_new_log_file(&mut self, millis: i64) -> crate::Result<RpcValue> {

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
            record_count_limit: usize,
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
            record_count_limit,
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
            //snapshot_ctx.is_snapshot_written = true;
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
        } else if let Some(dt) = params.since {
            params.since
        } else if let Some(dt) = log_context.first_file_datetime {
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
        let mut ret = RpcValue::new(log_context.log);
        ret.set_meta(log_header.to_meta_map());
        //logIShvJournal() << "result record cnt:" << log.size();
        Ok(ret)
    }

    fn create_journal_state(&mut self) -> crate::Result<()> {
        self.state = JournalState {
            journal_dir: None,
            files: Vec::new(),
            journal_dir_size: None,
            last_file_size: 0,
            recent_epoch_msec: None,
        };
        fs::create_dir_all(&self.options.journal_dir)?;
        self.state.journal_dir = Some(self.options.journal_dir.into());
        let mut journal_dir_size = 0usize;
        for entry in fs::read_dir(&self.journal_dir)? {
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
        if let Some(n) = self.state.files.last() {
            let last_file = Self::epoch_msec_to_filename(n)?;
            let recent_millis = Self::find_last_entry_milis(&last_file)?.unwrap_or(*n);
            self.state.recent_epoch_msec = Some(recent_millis);
        }
        Ok(())
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
    fn find_last_entry_milis(file: &str) -> crate::Result<Option<i64>> {
        const SPC: u8 = ' ' as u8;
        const LF: u8 = '\n' as u8;
        const CR: u8 = '\r' as u8;
        const TAB: u8 = '\t' as u8;
        const CHUNK_SIZE: usize = 1024;
        const TIMESTAMP_SIZE: usize = "2021-12-13T12-13-14-456Z".len() as usize;
        //println!("file: {:?}", file);
        let mut buffer = vec![0u8; CHUNK_SIZE + TIMESTAMP_SIZE];
        let mut f = File::open(file)?;
        let file_size = f.metadata()?.len() as usize;
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
                    println!("dt str: {}", dt_str);
                    let dt = chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S%.3fZ")
                        .map_err(|err| format!("Invalid date-time string: '{}', {}", dt_str, err))?;
                    return Ok(Some(dt.timestamp_millis()))
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