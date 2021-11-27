use std::collections::HashMap;
use time::format_description;
use ansi_term::Color;

// use chrono::{NaiveDateTime, TimeZone};
use flexi_logger::{DeferredNow, FlexiLoggerError, Level, Logger, LoggerHandle, Record};
use flexi_logger::filter::{LogLineFilter, LogLineWriter};

struct TopicVerbosity {
    default_level: log::Level,
    topic_levels: HashMap<String, log::Level>
}
impl TopicVerbosity {
    pub fn new(verbosity: &str) -> TopicVerbosity {
        let mut bc = TopicVerbosity {
            default_level: log::Level::Info,
            topic_levels: HashMap::new()
        };
        for level_str in verbosity.split(',') {
            let parts: Vec<&str> = level_str.split(':').collect();
            let (target, level_abbr) = if parts.len() == 1 {
                (parts[0], "T")
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                panic!("Cannot happen");
            };
            let level = match level_abbr {
                "D" => log::Level::Debug,
                "I" => log::Level::Info,
                "W" => log::Level::Warn,
                "E" => log::Level::Error,
                _ => log::Level::Trace,
            };
            if target.is_empty() {
                bc.default_level = level;
            } else {
                bc.topic_levels.insert(target.into(), level);
            }
        }
        bc
    }
    pub fn verbosity_to_string(&self) -> String {
        let s1 = format!(":{}", self.default_level);
        let s2 = self.topic_levels.iter()
            .map(|(target, level)| format!("{}:{}", target, level))
            .fold(String::new(), |acc, s| if acc.is_empty() { s } else { acc + "," + &s });
        format!("{},{}", s1, s2)
    }
}
impl LogLineFilter for TopicVerbosity {
    fn write(&self, now: &mut DeferredNow, record: &log::Record, log_line_writer: &dyn LogLineWriter) -> std::io::Result<()> {
        let level = if record.module_path().unwrap_or("") == record.target() {
            &self.default_level
        } else {
            match self.topic_levels.get(record.target()) {
                None => &self.default_level,
                Some(level) => level,
            }
        };
        if &record.level() <= level {
            log_line_writer.write(now, record)?;
        }
        Ok(())
    }
}

const TS_S: &str = "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]\
                    [offset_hour sign:mandatory]";//:[offset_minute]";
lazy_static::lazy_static! {
    static ref TS: Vec<format_description::FormatItem<'static>>
        = format_description::parse(TS_S).unwrap(/*ok*/);
}

fn log_format(w: &mut dyn std::io::Write, now: &mut DeferredNow, record: &Record) -> Result<(), std::io::Error> {
    // let sec = (now.now().unix_timestamp_nanos() / 1000_000_000) as i64;
    // let nano = (now.now().unix_timestamp_nanos() % 1000_000_000) as u32;
    // let ndt = NaiveDateTime::from_timestamp(sec, nano);
    // let dt = chrono::Local.from_utc_datetime(&ndt);
    let args = match record.level() {
        Level::Error => Color::Red.paint(format!("|E|{}", record.args())),
        Level::Warn => Color::Purple.paint(format!("|W|{}", record.args())),
        Level::Info => Color::Cyan.paint(format!("|I|{}", record.args())),
        Level::Debug => Color::Yellow.paint(format!("|D|{}", record.args())),
        Level::Trace => Color::White.dimmed().paint(format!("|T|{}", record.args())),
    };
    let target = if record.module_path().unwrap_or("") == record.target() { "".to_string() } else { format!("({})", record.target()) };
    write!(
        w,
        "{}{}{}{}",
        //dt.format("%Y-%m-%dT%H:%M:%S.%3f%z"),
        Color::Green.paint(
            now.now()
                .format(&TS)
                .unwrap_or_else(|_| "Timestamping failed".to_string())
        ),
        Color::Yellow.paint(format!("[{}:{}]", record.module_path().unwrap_or("<unnamed>"), record.line().unwrap_or(0))),
        Color::White.bold().paint(target),
        args,
    )
}

pub fn init(verbosity_string: &str) -> Result<(LoggerHandle, String), FlexiLoggerError> {
    let topic_verbosity = TopicVerbosity::new(verbosity_string);
    let verbosity_string_gen = topic_verbosity.verbosity_to_string();
    let handle = Logger::try_with_str("debug")?
        .filter(Box::new(topic_verbosity))
        .format(log_format)
        .set_palette("b1;3;2;4;6".into())
        .start()?;
    Ok((handle, verbosity_string_gen))
}
