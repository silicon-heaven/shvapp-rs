use crate::{Connection, Frame};

use tracing::{debug, instrument};

/// Represents an "unknown" command. This is not a real `Redis` command.
#[derive(Debug)]
pub struct Unknown {
    what: String,
}

impl Unknown {
    /// Create a new `Unknown` command which responds to unknown commands
    /// issued by clients
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            what: key.to_string(),
        }
    }

    /// Responds to the client, indicating the command is not recognized.
    ///
    /// This usually means the command is not yet implemented by `mini-redis`.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        Err(format!("ERR unknown request '{}'", self.what).into())
    }
}
