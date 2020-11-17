use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};
use chainpack::{RpcMessage, RpcValue};
use chainpack::RpcMessageMetaTags;

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct RpcCall {
    /// Name of the key to get
    frame: Frame,
}

impl RpcCall {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(frame: Frame) -> RpcCall {
        RpcCall {
            frame,
        }
    }

    /// Apply the `Get` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // echo for now
        match (&self.frame.message).create_response() {
            Ok(mut resp) => {
                resp.set_result(RpcValue::new(&format!("Method '{}' called on shvPath: {}"
                                                       , &self.frame.message.method().unwrap_or("")
                                                       , &self.frame.message.shv_path().unwrap_or("")
                )));
                debug!(?resp);
                // Write the response back to the client
                dst.write_frame(&Frame {protocol: self.frame.protocol, message: resp}).await?;
                Ok(())
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    // Converts the command into an equivalent `Frame`.
    //
    // This is called by the client when encoding a `Get` command to send to
    // the server.
    // pub(crate) fn into_frame(self) -> Frame {
    //     let mut frame = Frame::array();
    //     frame.push_bulk(Bytes::from("get".as_bytes()));
    //     frame.push_bulk(Bytes::from(self.rpcmsg.into_bytes()));
    //     frame
    // }
}
