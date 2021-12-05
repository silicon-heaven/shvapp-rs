use std::{cmp, fs};
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use chainpack::RpcValue;

pub struct Options {
    pub journal_dir: PathBuf,
    pub file_size_limit: usize,
    pub dir_size_limit: usize,
}

impl Options {
}

pub struct Entry {
    epoch_msec: i64,
    path: String,
    value: RpcValue,
    short_time: Option<i32>,
    domain: String,
    value_flags: u8,
    user_id: String,
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
    fn find_last_entry_milis(file: &Path, millis: i64) -> crate::Result<i64> {
        const SPC: u8 = ' ' as u8;
        const LF: u8 = '\n' as u8;
        const CR: u8 = '\r' as u8;
        const TAB: u8 = '\t' as u8;
        const CHUNK_SIZE: usize = 1024;
        const TIMESTAMP_SIZE: usize = "2021-12-13T12-13-14-456Z".len() as usize;
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
            while read_count > 0 {
                let c = buffer[read_count - 1 as usize];
                if !(c == LF || c == CR || c == TAB || c == SPC || c == 0) {
                    break;
                }
                read_count -= 1;
            }
            if read_count < TIMESTAMP_SIZE + 1 {
                return Err(format!("Corrupted log file, cannot find complete timestamp from pos: {} of file size: {} in file: {:?}", start_pos, file_size, file).into())
            }
            let mut pos = read_count - TIMESTAMP_SIZE;
            loop {
                if buffer[pos] == LF || pos == 0 {
                    if buffer[pos] == LF {
                        pos += 1;
                    }
                    // try to read timestamp
                    let dt_str = from_utf8(&buffer[pos..pos + TIMESTAMP_SIZE])?;
                    println!("dt str: {}", dt_str);
                    let dt = chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S%.3fZ")?;
                    return Ok(dt.timestamp_millis())
                }
                if pos > 0 {
                    pos -= 1;
                } else {
                    break;
                }
            }
            if start_pos == 0 {
                return Err(format!("Corrupted log file, cannot find complete timestamp from pos: {} of file size: {} in file: {:?}", start_pos, file_size, file).into())
            }
            start_pos -= cmp::max(CHUNK_SIZE, start_pos);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;
    use crate::shvjournal::Journal;

    #[test]
    fn find_last_entry_milis_handles_empty_files() {
        let millis = Journal::find_last_entry_milis(&PathBuf::from("tests/empty.log2"), 123).unwrap();
        assert_eq!(millis, 123);
        let millis = Journal::find_last_entry_milis(&PathBuf::from("tests/oneline.log2"), 1234).unwrap();
        assert_eq!(millis, 1234);
        // "2021-11-21T16:54:35.023Z" == 1637513675023
        let millis0 = 1637513675023i64;
        let millis = Journal::find_last_entry_milis(&PathBuf::from("tests/regular.log2"), millis0).unwrap();
        assert_eq!(millis, millis0);
    }
}