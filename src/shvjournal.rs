use std::{cmp, fs};
use std::cmp::{max, min};
use std::collections::{BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use regex::Regex;
use log::log;
use chainpack::{DateTime, RpcValue, List};
use crate::shvlog::{DEFAULT_GET_LOG_RECORD_COUNT_LIMIT, DOMAIN_VAL_CHANGE, Entry, EntryValueFlags, GetLogSince, GetLogParams, LogHeader, LogHeaderField, MAX_GET_LOG_RECORD_COUNT_LIMIT, PathDict};

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
    recent_entry_datetime: Option<DateTime>,
}
impl JournalState {
    fn is_consistent(&self) -> bool {
        !self.files.is_empty() && self.recent_entry_datetime.is_some()
    }
}
struct JournalSnapshot(BTreeMap<String, Entry>);
impl JournalSnapshot {
    fn new() -> Self { Self(BTreeMap::new()) }
    fn len(&self) -> usize { self.0. len() }
    fn is_empty(&self) -> bool { self.len() == 0 }
    fn add_entry(&mut self, entry: &Entry) -> bool {
        if entry.domain != DOMAIN_VAL_CHANGE {
            //shvDebug() << "remove not CHNG from snapshot:" << RpcValue(entry.toRpcValueMap()).toCpon();
            return false;
        }
        //println!("snapshot entry: {:?}", &entry);
        if entry.is_value_node_drop() {
            let mut drop_keys: Vec<String> = vec![];
            let entry_path = entry.path.clone();
            let range = self.0.range(entry_path..);
            for (path, _) in range {
                if path_starts_with(path, &entry.path) {
                    // it.key starts with key, then delete it from snapshot
                    drop_keys.push(path.into());
                } else {
                    break;
                }
            }
            for key in drop_keys {
                self.0.remove(&key);
            }
        }
        else if entry.value.is_default_value() {
            // writing default value to the snapshot must erase previous value if any
            if let Some(_) = self.0.get(&entry.path) {
                self.0.remove(&entry.path);
            }
        }
        else {
            //snapshot_ctx.last_entry_datetime = Some(*&entry.datetime);
            self.0.insert((&entry.path).clone(), entry.clone());
        }
        true
    }
}
pub struct Journal {
    pub options: Options,
    state: JournalState,
    snapshot: JournalSnapshot,
}
impl Journal {
    pub fn new(options: Options) -> crate::Result<Self> {
        let mut journal = Journal {
            options,
            state: JournalState {
                journal_dir: None,
                files: vec![],
                journal_dir_size: None,
                last_file_size: 0,
                recent_entry_datetime: None
            },
            snapshot: JournalSnapshot::new(),
        };
        journal.create_state()?;
        Ok(journal)
    }
    pub fn append(&mut self, entry: &Entry) -> crate::Result<()> {
        if !self.state.is_consistent() {
            if self.state.files.is_empty() {
                logShvJournalD!("Journal dir is empty, creating new one");
                self.init_journal_dir(&entry.datetime)?;
            }
        }
        if !self.state.is_consistent() {
            return Err("Journal state is not consistent, cannot append log entry".into());
        }
        let mut datetime = entry.datetime;
        if let Some(dt) = self.state.recent_entry_datetime {
            datetime = max(datetime, dt);
        }
        if self.state.last_file_size > self.options.file_size_limit {
            if &datetime > self.state.recent_entry_datetime.as_ref().expect("must be some here") {
                // cannot create new file with the same datetime as previous file last entry
                self.create_new_log_file(&datetime)?;
            }
        }
        match self.try_append(&datetime, entry) {
            Ok(_) => {Ok(())},
            Err(err) => {
                logShvJournalE!("Append log error: {}, trying to fix it by re-init journal dir", err);
                self.init_journal_dir(&datetime)?;
                self.try_append(&datetime, entry)
            }
        }?;
        self.snapshot.add_entry(entry);
        Ok(())
    }
    fn try_append(&mut self, datetime: &DateTime, entry: &Entry) -> crate::Result<()> {
        if !self.state.is_consistent() {
            logShvJournalE!("Append log: Inconsistent journal state");
            return Err("Inconsistent journal state".into());
        }
        let last_file_epoch_msec = *self.state.files.last().unwrap();
        let last_file_path = self.datetime_to_path(&DateTime::from_epoch_msec(last_file_epoch_msec))?;
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
            Ok(())
        };
        let b = datetime.to_iso_string();
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
        let b = entry.value_flags.bits().to_string();
        write_file(b.as_bytes(), false)?;
        write_file(entry.user_id.as_bytes(), true)?;
        f.flush()?;
        let file_size = f.metadata()?.len();
        let dir_size = self.state.journal_dir_size.unwrap_or(0);
        let dir_size = dir_size + (file_size - orig_file_size);
        self.state.journal_dir_size = Some(dir_size);
        self.state.last_file_size = file_size;
        self.state.recent_entry_datetime = Some(*datetime);
        Ok(())
    }
    pub fn create_new_log_file(&mut self, datetime: &DateTime) -> crate::Result<()> {
        if let Some(recent_entry_datetime) = self.state.recent_entry_datetime {
            if datetime <= &recent_entry_datetime {
                return Err("New file datetime must be greater than recent_entry_datetime".into())
            }
        }
        if self.state.journal_dir_size.unwrap_or(0) > self.options.dir_size_limit {
            self.rotate_journal()?;
        }
        let file_path = self.datetime_to_path(datetime)?;
        File::create(file_path)?;
        self.create_state()?;
        self.write_snapshot(datetime)
    }
    fn rotate_journal(&mut self) -> crate::Result<()> {
        logShvJournalD!("Rotating journal of size: {:?}", self.state.journal_dir_size);
        let mut file_cnt = self.state.files.len();
        let mut journal_size = self.state.journal_dir_size.unwrap_or(0);
        for file_millis in self.state.files.iter() {
            if file_cnt == 1 {
                // keep at least one file in case of bad limits configuration
                break;
            }
            if journal_size < self.options.dir_size_limit {
                break;
            }
            let file_path = self.datetime_to_path(&DateTime::from_epoch_msec(*file_millis))?;
            logShvJournalT!("\tdeleting file: {:?}", &file_path);
            let sz = fs::metadata(&file_path)?.len();
            fs::remove_file(&file_path)?;
            journal_size -= sz;
            file_cnt -= 1;
        }
        logShvJournalD!("New journal of size: {:?}", self.state.journal_dir_size);
        Ok(())
    }
    fn write_snapshot(&mut self, datetime: &DateTime) -> crate::Result<()> {
        let snapshot = self.snapshot.0.clone();
        for (_, mut entry) in snapshot {
            entry.datetime = datetime.clone();
            entry.value_flags.set(EntryValueFlags::SNAPSHOT, true);
            // erase EVENT flag in the snapshot values,
            // they can trigger events during reply otherwise
            entry.value_flags.set(EntryValueFlags::SPONTANEOUS, false);
            self.try_append(datetime, &entry)?;
        }
        if self.state.last_file_size > self.options.file_size_limit {
           return Err(format!("Snapshot is larger than log file size limit: {}", self.options.file_size_limit).into())
        }
        Ok(())
    }
    pub fn get_log(&self, params: &GetLogParams) -> crate::Result<RpcValue> {
        logShvJournalD!("------------- get_log() ---------------------");
        logShvJournalD!("params: {:?}", params);

        let record_count_limit = match params.record_count_limit {
            None => {DEFAULT_GET_LOG_RECORD_COUNT_LIMIT}
            Some(n) => {n}
        };
        let record_count_limit = min(record_count_limit, MAX_GET_LOG_RECORD_COUNT_LIMIT);
        struct SnapshotContext<'a> {
            params: &'a GetLogParams,
            snapshot: JournalSnapshot,
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
            rev_path_dict: BTreeMap<String, i32>,
            max_path_id: i32,
        }
        let mut snapshot_context = SnapshotContext {
            params,
            snapshot: JournalSnapshot(BTreeMap::new()),
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
            rev_path_dict: BTreeMap::new(),
            max_path_id: 0,
        };
        /// this ensure that there be only one copy of each path in memory
        fn make_path_shared(log_ctx: &mut LogContext, path: &str) -> i32 {
            match log_ctx.rev_path_dict.get(path) {
                None => {
                    log_ctx.max_path_id += 1;
                    log_ctx.rev_path_dict.insert(path.into(), log_ctx.max_path_id);
                    log_ctx.max_path_id
                }
                Some(n) => {*n}
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
            logShvJournalD!("\t writing snapshot, record count: {}", snapshot_ctx.snapshot.len());
            if snapshot_ctx.snapshot.is_empty() {
                return Ok(true);
            }
            if snapshot_ctx.snapshot.len() > log_ctx.record_count_limit {
                return Err(format!("Snapshot is larger than record count limit: {}", log_ctx.record_count_limit).into());
            };
            let snapshot_dt = match snapshot_ctx.params.since {
                GetLogSince::Some(dt) => { dt }
                GetLogSince::LastEntry => {
                    snapshot_ctx.last_entry_datetime
                        .ok_or("Internal error: Cannot have snapshot without last entry set")?
                }
                GetLogSince::None => {
                    return Err("Internal error: Cannot have snapshot without since defined".into());
                }
            };
            for (_, e) in &snapshot_ctx.snapshot.0 {
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
            logShvJournalD!("since: {:?}", &params.since);
            let first_file_ix;
            match &params.since {
                GetLogSince::Some(since_dt) => {
                    let since_msec = since_dt.epoch_msec();
                    first_file_ix = match self.state.files.binary_search(&since_msec) {
                        Ok(i) => {
                            // take exactly this file
                            logShvJournalD!("\texact match: {} {}", &i, &self.state.files[i]);
                            i
                        }
                        Err(i) => {
                            let i = if i > 0 {
                                logShvJournalD!("\tnot found, taking previous file: {} {}", i, &self.state.files[i]);
                                i - 1
                            } else {
                                logShvJournalD!("\tnot found, taking first file: {} {}", i, &self.state.files[i]);
                                0
                            };
                            i
                        }
                    };
                    let first_file_millis = self.state.files[first_file_ix];
                    log_context.first_file_datetime = Some(DateTime::from_epoch_msec(first_file_millis));
                }
                GetLogSince::LastEntry => {
                    first_file_ix = self.state.files.len() - 1;
                    let first_file_millis = self.state.files[first_file_ix];
                    log_context.first_file_datetime = Some(DateTime::from_epoch_msec(first_file_millis));
                }
                GetLogSince::None => {
                    first_file_ix = 0;
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
                let reader = JournalReader::new(&file_path)?;
                for entry in reader {
                    let entry = match entry {
                        Ok(entry) => { entry }
                        Err(err) => { return Err(err) }
                    };
                    if !is_path_match(&entry.path) {
                        continue;
                    }
                    if !is_domain_match(&entry.domain) {
                        continue;
                    }
                    let before_since = match params.since {
                        GetLogSince::Some(dt) => { entry.datetime < dt }
                        GetLogSince::LastEntry => { true }
                        GetLogSince::None => { false }
                    };
                    let after_until = params.until.is_some() && params.until.unwrap() < entry.datetime;
                    if before_since {
                        //logShvJournalD!("\t SNAPSHOT entry: {}", entry);
                        if params.with_snapshot {
                            if snapshot_context.snapshot.add_entry(&entry) {
                                snapshot_context.last_entry_datetime = Some(entry.datetime);
                            }
                        }
                    }
                    else if after_until {
                        break 'scan_files;
                    }
                    else {
                        if params.with_snapshot && !snapshot_context.is_snapshot_written {
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
        if params.with_snapshot && !snapshot_context.is_snapshot_written {
            snapshot_context.is_snapshot_written = true;
            write_snapshot(&mut snapshot_context,&mut log_context)?;
        }
        // if since is not specified in params
        // then use TS of first file used
        let log_since = match params.since {
            GetLogSince::Some(since) => {
                match log_context.first_file_datetime {
                    None => { None }
                    Some(dt) => {
                        if dt <= since { Some(since) } else { Some(dt) }
                    }
                }
            }
            GetLogSince::LastEntry => { log_context.last_entry_datetime }
            GetLogSince::None => { log_context.first_file_datetime }
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
            snapshot_count: snapshot_context.snapshot.len(),
            record_count: log_context.log.len(),
            record_count_limit,
            record_count_limit_hit: log_context.record_count_limit_hit,
            with_snapshot: snapshot_context.is_snapshot_written,
            path_dict: if params.with_path_dict {
                let mut path_dict = PathDict::new();
                for (k, v) in log_context.rev_path_dict {
                    path_dict.insert(v, k);
                }
                Some(path_dict)
            } else { None },
            fields: vec![
                LogHeaderField{name: "timestamp".into()},
                LogHeaderField{name: "path".into()},
                LogHeaderField{name: "value".into()},
                LogHeaderField{name: "shortTime".into()},
                LogHeaderField{name: "domain".into()},
                LogHeaderField{name: "valueFlags".into()},
                LogHeaderField{name: "userId".into()},
            ],
        };
        let ret = RpcValue::from(log_context.log).set_meta(Some(log_header.to_meta_map()));
        //logIShvJournal() << "result record cnt:" << log.size();
        Ok(ret)
    }

    fn init_journal_dir(&mut self, new_file_datetime: &DateTime) -> crate::Result<()> {
        fs::create_dir_all(&self.options.journal_dir)?;
        self.create_state()?;
        self.create_new_log_file(new_file_datetime)
    }
    fn create_state(&mut self) -> crate::Result<()> {
        logShvJournalT!("{}", crate::function!());
        self.state = JournalState {
            journal_dir: None,
            files: Vec::new(),
            journal_dir_size: None,
            last_file_size: 0,
            recent_entry_datetime: None,
        };
        let journal_dir: PathBuf = self.options.journal_dir.clone().into();
        let mut journal_dir_size: u64 = 0;
        match fs::read_dir(&journal_dir) {
            Ok(dir) => {
                for entry in dir {
                    let entry = entry?;
                    let path = entry.path();
                    if !path.is_dir() {
                        let millis = Self::path_to_datetime(&path)?.epoch_msec();
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
                    self.state.recent_entry_datetime = Some(recent_datetime);
                }
            }
            Err(_) => {
                // dir not exists
            }
        }
        Ok(())
    }
    fn path_to_datetime(path: &std::path::Path) -> crate::Result<DateTime> {
        let base_name = path.file_stem().ok_or(format!("Path '{:?}' is not valid log file path.", path))?;
        let base_name = base_name.to_str().ok_or(format!("Cannot convert OsStr '{:?}' to &str", base_name))?;
        Self::file_base_name_to_datetime(base_name)
    }
    fn datetime_to_path(&self, datetime: &DateTime) -> crate::Result<PathBuf> {
        let file_name = Self::datetime_to_file_base_name(datetime)? + ".log2";
        let mut path = match &self.state.journal_dir {
            None => {
                return Err("Cannot convert datetime to journal file, journal dir is invalid".into())
            }
            Some(path) => { path.clone() }
        };
        path.push(file_name);
        Ok(path)
    }
    //fn dir_datetime_to_path(path: &Path, datetime: &DateTime) -> crate::Result<PathBuf> {
    //    let file_name = Self::datetime_to_file_base_name(datetime)? + ".log2";
    //    let mut path = path.to_path_buf();
    //    path.push(file_name);
    //    Ok(path)
    //}
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
pub struct JournalReader {
}
impl JournalReader {
    fn new(file_path: &Path) -> crate::Result<JournalEntries<BufReader<File>>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        Ok(JournalEntries {
            buf: reader,
        })
    }
}
#[derive(Debug)]
pub struct JournalEntries<B> {
    buf: B,
}

impl<B: BufRead> Iterator for JournalEntries<B> {
    type Item = crate::Result<Entry>;

    fn next(&mut self) -> Option<crate::Result<Entry>> {
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
                            match DateTime::from_iso_str(s) {
                                Ok(dt) => { dt }
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
    use std::thread;
    use flexi_logger::{colored_default_format, colored_detailed_format, Logger};
    use log::{info, log};
    use chainpack::{DateTime, make_map, rpcvalue, RpcValue};
    use crate::shvjournal::{Journal, JournalReader, Options};
    use crate::shvlog::{Entry, GetLogParams, LogHeader};

    const JOURNAL_DIR: &str = "/tmp/shv-rs/journal-test";

    fn init_log() -> crate::Result<()> {
        //eprintln!("INIT LOG !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        //let _ = env_logger::builder().is_test(true).try_init();
        let is_detailed = true;
        match Logger::try_with_env() {
            Ok(logger) => {
                //eprintln!("flexi logger init OK");
                match logger.format(if is_detailed {colored_detailed_format} else {colored_default_format}).start() {
                    Ok(_) => {
                        logShvJournalI!("flexi logger init OK");
                    }
                    Err(_err) => {
                        // ignore double init error
                        //eprintln!("flexi logger started ERROR: {:?}", _err)
                    }
                }
            }
            Err(err) => { eprintln!("flexi logger started ERROR: {:?}", err) }
        };
        Ok(())
    }
    fn create_journal() -> crate::Result<Journal> {
        const FILE_SIZE: u64 = 1024 * 10;
        Journal::new(Options {
            journal_dir: JOURNAL_DIR.to_string(),
            file_size_limit: FILE_SIZE,
            dir_size_limit: FILE_SIZE * 10,
        })
    }
    fn create_test_data() -> crate::Result<Journal> {
        logShvJournalI!("Creating test data");
        let try_create_test_data = || -> crate::Result<Journal> {
            let _ = remove_dir_all(JOURNAL_DIR);
            let mut journal = create_journal()?;
            let entry = Entry::new(None, "tc/TC01/status/occupied", true.into());
            journal.append(&entry)?;
            let entry = Entry::new(None, "tc/TC01/status/error", false.into());
            journal.append(&entry)?;
            thread::sleep(std::time::Duration::from_millis(10));
            let entry = Entry::new(None, "tc/TC02/status/occupied", true.into());
            journal.append(&entry)?;
            let entry = Entry::new(None, "tc/TC02/status/error", false.into());
            journal.append(&entry)?;
            let vehicle_detected = make_map![ "vehicleId" => 1234, "direction" => "left" ];
            let entry = Entry::new(None, "vetra/VET01/vehicleDetected", vehicle_detected.into());
            journal.append(&entry)?;
            let entry = Entry::new(None, "tc/TC01/status/occupied", false.into());
            journal.append(&entry)?;
            Ok(journal)
        };
        match try_create_test_data() {
            Ok(journal) => Ok(journal),
            Err(err) => {
                //let backtrace = err.backtrace();
                //panic!("Cannot create testing data: {:?}, backtrace: {:?}", err, backtrace)
                panic!("Cannot create testing data: {:?}", err)
            }
        }
    }

    #[test]
    fn tst_find_last_entry_datetime() -> crate::Result<()> {
        init_log()?;
        info!("=== Starting test: {}", crate::function!());
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
        Ok(())
    }

    #[test]
    fn tst_get_log() -> crate::Result<()> {
        init_log()?;
        let mut journal = create_test_data()?;
        tst_get_log_with_default_params(&mut journal)?;
        tst_get_log_since_last()?;
        tst_rotate_journal()?;
        Ok(())
    }
    fn tst_get_log_with_default_params(journal: &mut Journal) -> crate::Result<()> {
        info!("=== Starting test: {}", crate::function!());
        const EXPECTED_RECORD_COUNT: usize = 6;
        let params = GetLogParams::default();//.since(DateTime::now().add_days(-1));
        let log = journal.get_log(&params)?;
        logShvJournalT!("log: {}", log.to_cpon_indented("\t")?);
        //println!("log: {}", log.to_cpon_indented("\t")?);
        let header = LogHeader::from_meta_map(log.meta());
        assert!(!header.fields.is_empty());
        assert_eq!(header.record_count, EXPECTED_RECORD_COUNT);
        assert_eq!(header.record_count_limit_hit, false);
        assert_eq!(header.with_snapshot, false);
        assert_eq!(header.snapshot_count, 0);
        assert_eq!(header.path_dict.is_some(), true);
        assert_eq!(header.path_dict.unwrap().len(), EXPECTED_RECORD_COUNT - 1);
        assert_eq!(header.since.is_some(), true);
        assert_ne!(header.since.unwrap().epoch_msec(), 0);
        assert_eq!(header.until.is_some(), true);
        assert_ne!(header.until.unwrap().epoch_msec(), 0);
        let record_list = log.as_list();
        assert_eq!(record_list.len(), EXPECTED_RECORD_COUNT);
        let e1 = Entry::from_rpcvalue(record_list.first().unwrap())?;
        let e2 = Entry::from_rpcvalue(record_list.last().unwrap())?;
        assert_eq!(header.since.unwrap(), e1.datetime);
        assert_eq!(header.until.unwrap(), e2.datetime);
        Ok(())
    }
    fn tst_get_log_since_last() -> crate::Result<()> {
        info!("=== Starting test: {}", crate::function!());
        let journal = create_journal()?;
        const EXPECTED_RECORD_COUNT: usize = 2; // VET01 + TC02/occupied
        let params = GetLogParams::default()
            .with_snapshot(true)
            .with_path_dict(false)
            .since_last_entry();
        let log = journal.get_log(&params)?;
        logShvJournalT!("log: {}", log.to_cpon_indented("\t")?);
        let header = LogHeader::from_meta_map(log.meta());
        assert_eq!(header.record_count, EXPECTED_RECORD_COUNT);
        assert_eq!(header.with_snapshot, true);
        assert!(header.snapshot_count > 0);
        assert_eq!(header.path_dict.is_none(), true);
        let record_list = log.as_list();
        assert_eq!(record_list.len(), EXPECTED_RECORD_COUNT);
        for rec in record_list {
            let e = Entry::from_rpcvalue(rec)?;
            assert_eq!(header.since.unwrap(), e.datetime);
        }
        let last_log_entry = Entry::from_rpcvalue(record_list.last().unwrap())?;
        assert_eq!(last_log_entry.datetime, header.since.unwrap());
        assert_eq!(last_log_entry.datetime, header.until.unwrap());
        Ok(())
    }
    fn tst_rotate_journal() -> crate::Result<()> {
        info!("=== Starting test: {}", crate::function!());
        let mut journal = create_journal()?;
        let mut row_count: usize = 0;
        let mut first_log_entry_datetime = None;
        let mut last_log_entry_datetime = None;
        let now = DateTime::now();
        let reader = JournalReader::new(&PathBuf::from("tests/shvjournal/2k7-rows.log2"))?;
        for entry in reader {
            match entry {
                Ok(entry) => {
                    row_count += 1;
                    if first_log_entry_datetime.is_none() {
                        first_log_entry_datetime = Some(*&entry.datetime);
                    }
                    let millis_diff = *&entry.datetime.epoch_msec() - first_log_entry_datetime.unwrap().epoch_msec();
                    let datetime = now.add_millis(millis_diff);
                    let mut new_entry = entry.clone();
                    //eprintln!("dt: {}", &datetime);
                    new_entry.datetime = datetime;
                    new_entry.value_flags.clear();
                    journal.append(&new_entry)?;
                    last_log_entry_datetime = Some(new_entry.datetime);
                }
                Err(err) => { return Err(err) }
            };
        }
        let last_log_entry_datetime = last_log_entry_datetime.unwrap();
        logShvJournalD!("row count: {}", row_count);
        let params = GetLogParams::default()
            .since(first_log_entry_datetime.unwrap())
            .with_snapshot(true)
            .with_path_dict(false)
            .record_count_limit(100);
        let log = journal.get_log(&params)?;
        logShvJournalD!("last entry datetime: {}", &last_log_entry_datetime);
        logShvJournalT!("log: {}", log.to_cpon_indented("\t")?);
        let header = LogHeader::from_meta_map(log.meta());
        //logShvJournalT!("get_log header: {}", RpcValue::new(().into(), Some(log.meta().clone())).to_cpon_indented("\t")?);
        //assert_eq!(last_log_entry_datetime, header.until.unwrap());
        assert_eq!(header.with_snapshot, true);
        //assert!(header.snapshot_count > 0);
        Ok(())
    }
}
