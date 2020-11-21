//! Minimal Redis server implementation
//!
//! Provides an async `run` function that listens for inbound connections,
//! spawning a task per connection.

use crate::{Connection, Db, Shutdown, Frame};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{trace, debug, error, info, warn, instrument};
use std::sync::atomic::{AtomicI64, Ordering};
use chainpack::{RpcMessageMetaTags, RpcValue, RpcMessage};
//use rand::Rng;
use crate::frame::Protocol;
use rand::Rng;
use chainpack::rpcmessage::{RpcError, RpcErrorCode};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// Shared database handle.
    ///
    /// Contains the key / value store as well as the broadcast channels for
    /// pub/sub.
    ///
    /// This is a wrapper around an `Arc`. This enables `db` to be cloned and
    /// passed into the per connection state (`Handler`).
    db: Db,

    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,
    connection_count: AtomicI64,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
#[derive(Debug)]
struct Handler {
    client_id: i64,
    /// Shared database handle.
    ///
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementation of the command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    db: Db,

    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore. If
    /// the listener is waiting for connections to close, it will be notified of
    /// the newly available permit and resume accepting connections.
    limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,
}

const MAX_CONNECTIONS: usize = 250;

/// Run the mini-redis server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(listener: TcpListener, shutdown: impl Future) -> crate::Result<()> {
    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Initialize the listener state
    let mut server = Listener {
        listener,
        db: Db::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        connection_count: AtomicI64::new(0),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    // Concurrently run the server and listen for the `shutdown` signal. The
    // server task runs until an error is encountered, so under normal
    // circumstances, this `select!` statement runs until the `shutdown` signal
    // is received.
    //
    // `select!` statements are written in the form of:
    //
    // ```
    // <result of async op> = <async op> => <step to perform with result>
    // ```
    //
    // All `<async op>` statements are executed concurrently. Once the **first**
    // op completes, its associated `<step to perform with result>` is
    // performed.
    //
    // The `select! macro is a foundational building block for writing
    // asynchronous Rust. See the API docs for more details:
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;
    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Listener {
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections, MAX connection limit: {}", MAX_CONNECTIONS);

        loop {
            // Wait for a permit to become available
            //
            // `acquire` returns a permit that is bound via a lifetime to the
            // semaphore. When the permit value is dropped, it is automatically
            // returned to the semaphore. This is convenient in many cases.
            // However, in this case, the permit must be returned in a different
            // task than it is acquired in (the handler task). To do this, we
            // "forget" the permit, which drops the permit value **without**
            // incrementing the semaphore's permits. Then, in the handler task
            // we manually add a new permit when processing completes.
            self.limit_connections.acquire().await.forget();

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;

            let client_id = self.connection_count.fetch_add(1, Ordering::SeqCst) + 1;
            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                client_id,
                // Get a handle to the shared database. Internally, this is an
                // `Arc`, so a clone only increments the ref count.
                db: self.db.clone(),

                // Initialize the connection state. This allocates read/write
                // buffers to perform redis protocol frame parsing.
                connection: Connection::new(socket),

                // The connection state needs a handle to the max connections
                // semaphore. When the handler is done processing the
                // connection, a permit is added back to the semaphore.
                limit_connections: self.limit_connections.clone(),

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };
            info!("Client id: {} connected", handler.client_id);
            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                match handler.run().await {
                    Ok(r) => {
                        info!("Client id: {} disconnected", handler.client_id);
                    }
                    Err(err) => {
                        error!("Client id: {} connection error: {}", handler.client_id, err);
                        error!(cause = ?err, "connection error");
                    }
                }
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        match self.client_login().await {
            Ok(()) => (),
            Err(e) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                return Err(e.into())
            },
        }

        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let frame = tokio::select! {
                res = self.connection.read_frame() => {
                    let frame = res?;
                    frame
                }
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Err("Server shutdown".into());
                }
            };

            frame.apply(&self.db, &mut self.connection)
                .await?;
        }

        Ok(())
    }

    async fn client_login(&mut self) -> crate::Result<()> {
        let first_frame = self.connection.read_frame().await?;
        self.connection.protocol = Some(first_frame.protocol);
        let hello = first_frame.to_rpcmesage()?;
        debug!("hello: {}", hello);
        if let Some(method) = hello.method() {
            if method == "hello" {
                let nonce: u32 = rand::thread_rng().gen();
                let mut result = chainpack::rpcvalue::Map::new();
                result.insert("nonce".into(), RpcValue::new(nonce.to_string()));
                let mut resp = hello.create_response()?;
                resp.set_result(RpcValue::new(result));                // Write the response back to the client
                self.connection.write_message(&resp).await?;

                let login_rq = self.connection.read_frame().await?.to_rpcmesage()?;
                debug!("login: {}", login_rq);
                let login_error = loop {
                    if let Some(method) = login_rq.method() {
                        if method == "login" {
                            if let Some(params) = login_rq.params() {
                                if let Some(login_map) = params.as_map().get("login") {
                                    if let Some(user) = login_map.as_map().get("user") {
                                        let user = user.as_str();
                                        if let Some(password) = login_map.as_map().get("password") {
                                            let password = password.as_str();
                                            if user == "iot" && password == "iotpwd" {
                                                break None
                                            } else {
                                                break Some("Bad login user or password")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    break Some("Bad login")
                };
                let mut resp = login_rq.create_response()?;
                match login_error {
                    None => {
                        let mut result = chainpack::rpcvalue::Map::new();
                        result.insert("clientId".into(), RpcValue::new(self.client_id));
                        resp.set_result(RpcValue::new(result));                // Write the response back to the client
                    }
                    Some(err) => {
                        warn!("Client: {} login error {}", self.client_id, err);
                        resp.set_error(RpcError::new(RpcErrorCode::InvalidRequest, err));
                    }
                }
                self.connection.write_message(&resp).await?;
            }
        }
        Err("Bad login sequence".into())
    }
    // async fn read_frame(&mut self) -> crate::Result<Frame> {
    //     let frame = tokio::select! {
    //         res = self.connection.read_frame() => {
    //             let frame = res?;
    //             return Ok(frame)
    //         }
    //         _ = self.shutdown.recv() => {
    //             // If a shutdown signal is received, return from `run`.
    //             // This will result in the task terminating.
    //             return Err("Server shutdown".into());
    //         }
    //     };
    // }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        //
        // Doing so unblocks the listener if the max number of
        // connections has been reached.
        //
        // This is done in a `Drop` implementation in order to guarantee that
        // the permit is added even if the task handling the connection panics.
        // If `add_permit` was called at the end of the `run` function and some
        // bug causes a panic. The permit would never be returned to the
        // semaphore.
        self.limit_connections.add_permits(1);
    }
}
