//! Contains the TCP client to connect to an ADS server.

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::io::Read;
use std::mem::{self, size_of};
use std::net::{IpAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use roboplc::locking::{Condvar, Mutex, RawMutex};
use roboplc::{policy_channel, DataDeliveryPolicy, Error, Result};

use byteorder::{ByteOrder, ReadBytesExt as _, LE};
use itertools::Itertools;
use roboplc::comm::{CommReader, SessionGuard, Timeouts};
use roboplc::policy_channel::{Receiver, Sender};
use tracing::{debug, error, trace, warn};

use crate::errors::ads_error;
use crate::{notif, AdsMapping};
use crate::{AmsAddr, AmsNetId};

use zerocopy::byteorder::{U16, U32};
use zerocopy::{AsBytes, FromBytes};

struct AdsBuffer(Vec<u8>);

impl DataDeliveryPolicy for AdsBuffer {}

struct AdsCommResult(Result<Vec<u8>>);

impl DataDeliveryPolicy for AdsCommResult {}

const MAX_NOTIFICATION_QUEUE: usize = 16384;
const MAX_BUF_QUEUE: usize = 1024;

type DataCell<P> = rtsc::cell::DataCell<P, RawMutex, Condvar>;
type ReplyMap = Arc<Mutex<BTreeMap<u32, DataCell<AdsCommResult>>>>;

/// An ADS protocol command.
// https://infosys.beckhoff.com/content/1033/tc3_ads_intro/115847307.html?id=7738940192708835096
#[repr(u16)]
#[derive(Clone, Copy, Debug)]
pub enum Command {
    /// Return device info
    DevInfo = 1,
    /// Read some data
    Read = 2,
    /// Write some data
    Write = 3,
    /// Write some data, then read back some data
    /// (used as a poor-man's function call)
    ReadWrite = 9,
    /// Read the ADS and device state
    ReadState = 4,
    /// Set the ADS and device state
    WriteControl = 5,
    /// Add a notification for a given index
    AddNotification = 6,
    /// Add a notification for a given index
    DeleteNotification = 7,
    /// Change occurred in a given notification,
    /// can be sent by the PLC only
    Notification = 8,
}

impl Command {
    fn action(self) -> &'static str {
        match self {
            Command::DevInfo => "get device info",
            Command::Read => "read data",
            Command::Write => "write data",
            Command::ReadWrite => "write and read data",
            Command::ReadState => "read state",
            Command::WriteControl => "write control",
            Command::AddNotification => "add notification",
            Command::DeleteNotification => "delete notification",
            Command::Notification => "notification",
        }
    }
}

/// Size of the AMS/TCP + AMS headers
// https://infosys.beckhoff.com/content/1033/tc3_ads_intro/115845259.html?id=6032227753916597086
pub(crate) const TCP_HEADER_SIZE: usize = 6;
pub(crate) const AMS_HEADER_SIZE: usize = 38; // including AMS/TCP header
pub(crate) const DEFAULT_BUFFER_SIZE: usize = 100;

/// Specifies the source AMS address to use.
#[derive(Clone, Copy, Debug)]
pub enum Source {
    /// Auto-generate a source address from the local address and a random port.
    Auto,
    /// Use a specified source address.
    Addr(AmsAddr),
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Open a new connection to an ADS server.
    ///
    /// If connecting to a server that has an AMS router, it needs to have a
    /// route set for the source IP and NetID, otherwise the connection will be
    /// closed immediately.  The route can be added from TwinCAT, or this
    /// crate's `udp::add_route` helper can be used to add a route via UDP
    /// message.
    ///
    /// `source` is the AMS address to to use as the source; the NetID needs to
    /// match the route entry in the server.  If `Source::Auto`, the NetID is
    /// constructed from the local IP address with .1.1 appended; if there is no
    /// IPv4 address, `127.0.0.1.1.1` is used.
    ///
    /// The AMS port of `source` is not important, as long as it is not a
    /// well-known service port; an ephemeral port number > 49152 is
    /// recommended.  If Auto, the port is set to 58913.
    ///
    /// Since all communications is supposed to be handled by an ADS router,
    /// only one TCP/ADS connection can exist between two hosts. Non-TwinCAT
    /// clients should make sure to replicate this behavior, as opening a second
    /// connection will close the first.
    ///
    /// # Panics
    ///
    /// Should not panic
    pub fn new<A: ToSocketAddrs + fmt::Debug>(
        addr: A,
        timeouts: Timeouts,
        source: Source,
    ) -> Result<(Self, Reader)> {
        let (inner, reader) = ClientInner::new(addr, timeouts, source)?;
        Ok((
            Self {
                inner: Arc::new(inner),
            },
            reader,
        ))
    }
    /// Return the source address the client is using.
    pub fn source(&self) -> AmsAddr {
        self.inner.source
    }

    /// Get a receiver for notifications.
    pub fn get_notification_channel(&self) -> Receiver<notif::Notification> {
        self.inner.notif_recv.clone()
    }

    /// Return a wrapper that executes operations for a target device (known by
    /// NetID and port).
    ///
    /// The local NetID `127.0.0.1.1.1` is mapped to the client's source NetID,
    /// so that you can connect to a local PLC using:
    ///
    /// ```rust,ignore
    /// let client = Client::new("127.0.0.1", ..., Source::Request);
    /// let device = client.device(AmsAddr::new(AmsNetId::local(), 851));
    /// ```
    ///
    /// without knowing its NetID.
    pub fn device(&self, mut addr: AmsAddr) -> Device {
        if addr.netid() == AmsNetId::local() {
            addr = AmsAddr::new(self.source().netid(), addr.port());
        }
        Device {
            client: self.clone(),
            addr,
        }
    }

    /// Get internal session ID. The handles should be recreated if changed
    pub fn session_id(&self) -> usize {
        self.inner.client.session_id()
    }

    /// Lock TCP session (disable reconnects)
    pub fn lock_session(&self) -> Result<SessionGuard> {
        self.inner.client.lock_session()
    }

    /// Low-level function to execute an ADS command.
    ///
    /// Writes a data from a number of input buffers, and returns data in a
    /// number of output buffers.  The latter might not be filled completely;
    /// the return value specifies the number of total valid bytes.  It is up to
    /// the caller to determine what this means in terms of the passed buffers.
    #[inline]
    pub fn communicate(
        &self,
        cmd: Command,
        target: AmsAddr,
        data_in: &[&[u8]],
        data_out: &mut [&mut [u8]],
    ) -> Result<usize> {
        self.inner.communicate(cmd, target, data_in, data_out)
    }
    /// Purge client, e.g. after restart
    pub fn purge(&self) {
        mem::take(&mut *self.inner.notif_handles.lock());
    }
    // Should be called if notifications are used. Close all open notification handles.
    pub fn shutdown(&self) {
        let handles = mem::take(&mut *self.inner.notif_handles.lock());
        for (addr, handle) in handles {
            let _r = self.device(addr).delete_notification(handle);
        }
    }
}

/// Represents a connection to a ADS server.
///
/// The Client's communication methods use `&self`, so that it can be freely
/// shared within one thread, or sent, between threads.  Wrappers such as
/// `Device` or `symbol::Handle` use a `&Client` as well.
struct ClientInner {
    /// TCP connection (duplicated with the reader)
    client: roboplc::comm::Client,
    /// Current invoke ID (identifies the request/reply pair), incremented
    /// after each request
    invoke_id: AtomicU32,
    /// Read timeout (actually receive timeout for the channel)
    read_timeout: Option<Duration>,
    /// The AMS address of the client
    source: AmsAddr,
    /// Sender for used Vec buffers to the reader thread
    buf_send: Sender<AdsBuffer>,
    /// Communcation replies map
    reply_map: ReplyMap,
    /// Receiver for notifications: cloned and given out to interested parties
    notif_recv: Receiver<notif::Notification>,
    /// Active notification handles: these will be closed on Drop
    notif_handles: Mutex<BTreeSet<(AmsAddr, notif::Handle)>>,
}

impl ClientInner {
    fn new<A: ToSocketAddrs + fmt::Debug>(
        addr: A,
        timeouts: Timeouts,
        source: Source,
    ) -> Result<(Self, Reader)> {
        let read_timeout = timeouts.read;
        let (client, reader_rx) = roboplc::comm::tcp::connect_with_options(
            addr,
            roboplc::comm::ConnectionOptions::new(timeouts.connect)
                .with_reader()
                .timeouts(timeouts),
        )?;
        let reader_rx = reader_rx.expect("reader_rx");
        let source = match source {
            Source::Addr(id) => id,
            Source::Auto => {
                let my_addr = client.local_ip_addr()?.expect("BUG").ip();
                if let IpAddr::V4(ip) = my_addr {
                    let [a, b, c, d] = ip.octets();
                    // use some random ephemeral port
                    AmsAddr::new(AmsNetId::new(a, b, c, d, 1, 1), 58913)
                } else {
                    AmsAddr::new(AmsNetId::new(127, 0, 0, 1, 1, 1), 58913)
                }
            }
        };

        let (buf_send, buf_recv) = policy_channel::bounded(MAX_BUF_QUEUE);
        let (notif_send, notif_recv) = policy_channel::bounded(MAX_NOTIFICATION_QUEUE);
        let mut source_bytes = [0; 8];
        source.write_to(&mut &mut source_bytes[..]).expect("size");

        let reply_map = Arc::new(Mutex::new(BTreeMap::new()));

        let (restart_tx, restart_rx) = policy_channel::bounded(1);

        let reader = Reader {
            client: client.clone(),
            reply_map: reply_map.clone(),
            reader_rx,
            source: source_bytes,
            buf_recv,
            notif_send,
            restart_rx,
            restart_tx,
        };

        Ok((
            ClientInner {
                client,
                source,
                buf_send,
                reply_map,
                notif_recv,
                invoke_id: <_>::default(),
                read_timeout: if read_timeout > Duration::from_secs(0) {
                    Some(read_timeout)
                } else {
                    None
                },
                notif_handles: <_>::default(),
            },
            reader,
        ))
    }

    /// Low-level function to execute an ADS command.
    ///
    /// Writes a data from a number of input buffers, and returns data in a
    /// number of output buffers.  The latter might not be filled completely;
    /// the return value specifies the number of total valid bytes.  It is up to
    /// the caller to determine what this means in terms of the passed buffers.
    fn communicate(
        &self,
        cmd: Command,
        target: AmsAddr,
        data_in: &[&[u8]],
        data_out: &mut [&mut [u8]],
    ) -> Result<usize> {
        // Increase the invoke ID.  We could also generate a random u32, but
        // this way the sequence of packets can be tracked.
        let invoke_id = self.invoke_id.fetch_add(1, Ordering::Relaxed);

        // The data we send is the sum of all data_in buffers.
        let data_in_len = data_in.iter().map(|v| v.len()).sum::<usize>();

        // Create outgoing header.
        let ads_data_len = AMS_HEADER_SIZE - TCP_HEADER_SIZE + data_in_len;
        let header = AdsHeader {
            ams_cmd: 0, // send command
            length: U32::new(ads_data_len.try_into().map_err(Error::invalid_data)?),
            dest_netid: target.netid(),
            dest_port: U16::new(target.port()),
            src_netid: self.source.netid(),
            src_port: U16::new(self.source.port()),
            command: U16::new(cmd as u16),
            state_flags: U16::new(4), // state flags (4 = send command)
            data_length: U32::new(u32::try_from(data_in_len).map_err(Error::invalid_data)?), // overflow checked above
            error_code: U32::new(0),
            invoke_id: U32::new(invoke_id),
        };

        // Collect the outgoing data.  Note, allocating a Vec and calling
        // `socket.write_all` only once is faster than writing in multiple
        // steps, even with TCP_NODELAY.
        let mut request = Vec::with_capacity(ads_data_len);
        request.extend_from_slice(header.as_bytes());
        for buf in data_in {
            request.extend_from_slice(buf);
        }
        // &T impls Write for T: Write, so no &mut self required.
        let cell = DataCell::new();
        self.reply_map.lock().insert(invoke_id, cell.clone());
        self.client.write(&request)?;

        macro_rules! map_ch_err {
            ($res: expr) => {
                match $res {
                    Ok(v) => v,
                    Err(e) => {
                        self.reply_map.lock().remove(&invoke_id);
                        return Err(Error::io(e));
                    }
                }
            };
        }

        // Get a reply from the reader thread, with timeout or not.
        let reply = if let Some(tmo) = self.read_timeout {
            map_ch_err!(cell.get_timeout(tmo))
        } else {
            map_ch_err!(cell.get())
        }
        .0
        .map_err(Error::io)?;

        // Validate the incoming reply.  The reader thread already made sure that
        // it is consistent and addressed to us.

        // The source netid/port must match what we sent.
        if reply[14..22] != request[6..14] {
            return Err(Error::io("unexpected source address"));
        }
        // Read the other fields we need.
        if reply.len() < AMS_HEADER_SIZE {
            return Err(Error::io("reply too short"));
        }
        let mut ptr = &reply[22..];
        let ret_cmd = ptr.read_u16::<LE>()?;
        let state_flags = ptr.read_u16::<LE>()?;
        let data_len = ptr.read_u32::<LE>()?;
        let error_code = ptr.read_u32::<LE>()?;
        let reply_invoke_id = ptr.read_u32::<LE>()?;
        let result = if reply.len() >= AMS_HEADER_SIZE + 4 {
            ptr.read_u32::<LE>()?
        } else {
            0 // this must be because an error code is already set
        };

        // Command must match.
        if ret_cmd != cmd as u16 {
            dbg!(invoke_id);
            return Err(Error::io("unexpected command"));
        }
        // State flags must be "4 | 1".
        if state_flags != 5 {
            return Err(Error::io("unexpected state flags"));
        }
        // Invoke ID must match what we sent.
        if reply_invoke_id != invoke_id {
            return Err(Error::io("unexpected invoke ID"));
        }
        // Check error code in AMS header.
        if error_code != 0 {
            return ads_error(cmd.action(), error_code);
        }
        // Check result field in payload, only relevant if error_code == 0.
        if result != 0 {
            return ads_error(cmd.action(), result);
        }

        // If we don't want return data, we're done.
        if data_out.is_empty() {
            let _r = self.buf_send.send(AdsBuffer(reply));
            return Ok(0);
        }

        // Check returned length, it needs to fill at least the first data_out
        // buffer.  This also ensures that we had a result field.
        if (data_len as usize) < data_out[0].len() + 4 {
            return Err(Error::io("got less data than expected"));
        }

        // The pure user data length, without the result field.
        let data_len = data_len as usize - 4;

        // Distribute the data into the user output buffers, up to the returned
        // data length.
        let mut offset = AMS_HEADER_SIZE + 4;
        let mut rest_len = data_len;
        for buf in data_out {
            let n = buf.len().min(rest_len);
            buf[..n].copy_from_slice(&reply[offset..][..n]);
            offset += n;
            rest_len -= n;
            if rest_len == 0 {
                break;
            }
        }

        // Send back the Vec buffer to the reader thread.
        let _r = self.buf_send.send(AdsBuffer(reply));

        // Return either the error or the length of data.
        Ok(data_len)
    }
}

/// Received every time when the reader has been restarted.
#[derive(Default, Copy, Clone)]
pub struct RestartEvent {}

impl DataDeliveryPolicy for RestartEvent {
    fn delivery_policy(&self) -> roboplc::prelude::DeliveryPolicy {
        roboplc::prelude::DeliveryPolicy::Single
    }
}

/// Implementation detail: reader thread that takes replies and notifications
/// and distributes them accordingly.
pub struct Reader {
    client: roboplc::comm::Client,
    reply_map: ReplyMap,
    reader_rx: roboplc::policy_channel::Receiver<CommReader>,
    source: [u8; 8],
    buf_recv: Receiver<AdsBuffer>,
    notif_send: Sender<notif::Notification>,
    restart_rx: Receiver<RestartEvent>,
    restart_tx: Sender<RestartEvent>,
}

impl Reader {
    /// The method is required to be started in a separate thread.
    ///
    /// # Panics
    ///
    /// Should not panic
    pub fn run(&self) {
        let mut first_start = true;
        while let Ok(reader) = self.reader_rx.recv() {
            let session_id = self.client.session_id();
            self.restart_tx
                .send(RestartEvent {})
                .expect("never disconnects");
            if first_start {
                first_start = false;
            } else {
                warn!(session_id, "ADS reader loop restarted");
            }
            trace!(session_id, "spawning reader");
            self.run_inner(reader);
            // reconnect the client in case it has not been done yet
            if session_id == self.client.session_id() {
                debug!("reader asked the client to reconnect");
                self.client.reconnect();
            }
        }
    }

    /// Gets a channel receiver for restart events. The events can be processed later manually,
    /// e.g. to restore notifications or handles.
    ///
    /// The restart beacon has got a delivery policy `Single` so the event is always delivered in a
    /// single copy, no matter how many restarts happened.
    ///
    /// NOTE: it is physically impossible to be 100% was there a network issue or the server has been
    /// restarted. In production networks, consider the network issue is the less likely case.
    #[allow(dead_code)]
    // the function is being tested for stability
    pub fn get_restart_event_receiver(&self) -> Receiver<RestartEvent> {
        self.restart_rx.clone()
    }

    fn run_inner(&self, mut reader: CommReader) {
        let mut socket = reader.take().expect("can not get reader socket");
        loop {
            // Get a buffer from the free-channel or create a new one.
            let mut buf = self
                .buf_recv
                .try_recv()
                .unwrap_or_else(|_| AdsBuffer(Vec::with_capacity(DEFAULT_BUFFER_SIZE)))
                .0;

            // Read a header from the socket.
            buf.resize(TCP_HEADER_SIZE, 0);
            if socket.read_exact(&mut buf).is_err() {
                // Not sending an error back; we don't know if something was
                // requested or the socket was just closed from either side.
                return;
            }

            // Read the rest of the packet.
            let packet_length = LE::read_u32(&buf[2..6]) as usize;
            buf.resize(TCP_HEADER_SIZE + packet_length, 0);
            if let Err(error) = socket.read_exact(&mut buf[6..]) {
                error!(%error, "error reading ADS packet");
                return;
            }

            // Is it something other than an ADS command packet?
            let ams_cmd = LE::read_u16(&buf);
            if ams_cmd != 0 {
                // if it's a known packet type, continue
                if matches!(ams_cmd, 1 | 4096 | 4097 | 4098) {
                    continue;
                }
                error!("invalid packet or unknown AMS command");
                return;
            }

            // If the header length fields aren't self-consistent, abort the connection.
            let rest_length = LE::read_u32(&buf[26..30]) as usize;
            if rest_length != packet_length + TCP_HEADER_SIZE - AMS_HEADER_SIZE {
                error!("inconsistent packet length");
                return;
            }

            // Check that the packet is meant for us.
            if buf[6..14] != self.source {
                continue;
            }

            // If it looks like a reply, send it back to the requesting thread,
            // it will handle further validation.
            if LE::read_u16(&buf[22..24]) != Command::Notification as u16 {
                let mut ptr = &buf[34..];
                match ptr.read_u32::<LE>() {
                    Ok(invoke_id) => {
                        if let Some(tx) = self.reply_map.lock().remove(&invoke_id) {
                            tx.set(AdsCommResult(Ok(buf)));
                        }
                    }
                    Err(e) => {
                        error!(%e, "error reading invoke ID");
                        return;
                    }
                }
                continue;
            }

            // Validate notification message fields.
            let state_flags = LE::read_u16(&buf[24..26]);
            let error_code = LE::read_u32(&buf[30..34]);
            let length = LE::read_u32(&buf[38..42]) as usize;
            if state_flags != 4 || error_code != 0 || length != rest_length - 4 || length < 4 {
                continue;
            }

            // Send the notification to whoever wants to receive it.
            if let Ok(notif) = notif::Notification::new(buf) {
                self.notif_send.send(notif).expect("never disconnects");
            }
        }
    }
}

/// A `Client` wrapper that talks to a specific ADS device.
#[derive(Clone)]
pub struct Device {
    /// The underlying `Client`.
    pub client: Client,
    addr: AmsAddr,
}

impl Device {
    /// Read the device's name + version.
    pub fn get_info(&self) -> Result<DeviceInfo> {
        let mut data = DeviceInfoRaw::new_zeroed();
        self.client
            .communicate(Command::DevInfo, self.addr, &[], &mut [data.as_bytes_mut()])?;

        // Decode the name string, which is null-terminated.  Technically it's
        // Windows-1252, but in practice no non-ASCII occurs.
        let name = data
            .name
            .iter()
            .take_while(|&&ch| ch > 0)
            .map(|&ch| ch as char)
            .collect::<String>();
        Ok(DeviceInfo {
            major: data.major,
            minor: data.minor,
            version: data.version.get(),
            name,
        })
    }

    /// Wait until the device is in the Run state.
    ///
    /// Returns true if the device is in the Run state,
    /// false if the timeout has been reached.
    pub fn wait_running(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .get_state()
                .map_or(false, |(s, _)| s == crate::AdsState::Run)
            {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Read some data at a given index group/offset.  Returned data can be shorter than
    /// the buffer, the length is the return value.
    pub fn read(&self, index_group: u32, index_offset: u32, data: &mut [u8]) -> Result<usize> {
        let header = IndexLength {
            index_group: U32::new(index_group),
            index_offset: U32::new(index_offset),
            length: U32::new(data.len().try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);

        self.client.communicate(
            Command::Read,
            self.addr,
            &[header.as_bytes()],
            &mut [read_len.as_bytes_mut(), data],
        )?;

        Ok(read_len.get() as usize)
    }

    /// Read some data at a given index group/offset, ensuring that the returned data has
    /// exactly the size of the passed buffer.
    pub fn read_exact(&self, index_group: u32, index_offset: u32, data: &mut [u8]) -> Result<()> {
        let len = self.read(index_group, index_offset, data)?;
        if len != data.len() {
            return Err(Error::io("got less data than expected"));
        }
        Ok(())
    }

    /// Read data of given type.
    ///
    /// Any type that supports `zerocopy::FromBytes` can be read.  You can also
    /// derive that trait on your own structures and read structured data
    /// directly from the symbol.
    ///
    /// Note: to be independent of the host's byte order, use the integer types
    /// defined in `zerocopy::byteorder`.
    pub fn read_value<T: Default + AsBytes + FromBytes>(
        &self,
        index_group: u32,
        index_offset: u32,
    ) -> Result<T> {
        let mut buf = T::default();
        self.read_exact(index_group, index_offset, buf.as_bytes_mut())?;
        Ok(buf)
    }

    /// Read multiple index groups/offsets with one ADS request (a "sum-up" request).
    ///
    /// The returned data can be shorter than the buffer in each case, the `length`
    /// member of the `ReadRequest` is set to the returned length.
    ///
    /// This function only returns Err on errors that cause the whole sum-up
    /// request to fail (e.g. if the device doesn't support such requests).  If
    /// the request as a whole succeeds, each single read can have returned its
    /// own error.  The [`ReadRequest::data`] method will return either the
    /// properly truncated returned data or the error for each read.
    ///
    /// Example:
    /// ```ignore
    /// // create buffers
    /// let mut buf_1 = [0; 128];  // request reading 128 bytes
    /// let mut buf_2 = [0; 128];  // from two indices
    /// // create the request structures
    /// let mut req_1 = ReadRequest::new(ix1, off1, &mut buf_1);
    /// let mut req_2 = ReadRequest::new(ix2, off2, &mut buf_2);
    /// //  actual request
    /// device.read_multi(&mut [req_1, req_2])?;
    /// // extract the resulting data, checking individual reads for
    /// // errors and getting the returned data otherwise
    /// let res_1 = req_1.data()?;
    /// let res_2 = req_2.data()?;
    /// ```
    pub fn read_multi(&self, requests: &mut [ReadRequest]) -> Result<()> {
        let nreq = requests.len();
        let read_len = requests
            .iter()
            .map(|r| size_of::<ResultLength>() + r.rbuf.len())
            .sum::<usize>();
        let write_len = size_of::<IndexLength>() * nreq;
        let header = IndexLengthRW {
            // using SUMUP_READ_EX_2 since would return the actual returned
            // number of bytes, and no empty bytes if the read is short,
            // but then we'd have to reshuffle the buffers
            index_group: U32::new(crate::index::SUMUP_READ_EX),
            index_offset: U32::new(u32::try_from(nreq).map_err(Error::invalid_data)?),
            read_length: U32::new(read_len.try_into().map_err(Error::invalid_data)?),
            write_length: U32::new(write_len.try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);
        let mut w_buffers = vec![header.as_bytes()];
        let mut r_buffers = (0..=2 * nreq).map(|_| &mut [][..]).collect_vec();
        r_buffers[0] = read_len.as_bytes_mut();
        for (i, req) in requests.iter_mut().enumerate() {
            w_buffers.push(req.req.as_bytes());
            r_buffers[1 + i] = req.res.as_bytes_mut();
            r_buffers[1 + nreq + i] = req.rbuf;
        }
        self.client
            .communicate(Command::ReadWrite, self.addr, &w_buffers, &mut r_buffers)?;
        Ok(())
    }

    /// Write some data to a given index group/offset.
    pub fn write(&self, index_group: u32, index_offset: u32, data: &[u8]) -> Result<()> {
        let header = IndexLength {
            index_group: U32::new(index_group),
            index_offset: U32::new(index_offset),
            length: U32::new(data.len().try_into().map_err(Error::invalid_data)?),
        };
        self.client.communicate(
            Command::Write,
            self.addr,
            &[header.as_bytes(), data],
            &mut [],
        )?;
        Ok(())
    }

    /// Write data of given type.
    ///
    /// See `read_value` for details.
    pub fn write_value<T: AsBytes>(
        &self,
        index_group: u32,
        index_offset: u32,
        value: &T,
    ) -> Result<()> {
        self.write(index_group, index_offset, value.as_bytes())
    }

    /// Write multiple index groups/offsets with one ADS request (a "sum-up" request).
    ///
    /// This function only returns Err on errors that cause the whole sum-up
    /// request to fail (e.g. if the device doesn't support such requests).  If
    /// the request as a whole succeeds, each single write can have returned its
    /// own error.  The [`WriteRequest::ensure`] method will return the error for
    /// each write.
    pub fn write_multi(&self, requests: &mut [WriteRequest]) -> Result<()> {
        let nreq = requests.len();
        let read_len = size_of::<u32>() * nreq;
        let write_len = requests
            .iter()
            .map(|r| size_of::<IndexLength>() + r.wbuf.len())
            .sum::<usize>();
        let header = IndexLengthRW {
            index_group: U32::new(crate::index::SUMUP_WRITE),
            index_offset: U32::new(u32::try_from(nreq).map_err(Error::invalid_data)?),
            read_length: U32::new(read_len.try_into().map_err(Error::invalid_data)?),
            write_length: U32::new(write_len.try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);
        let mut w_buffers = vec![&[][..]; 2 * nreq + 1];
        let mut r_buffers = vec![read_len.as_bytes_mut()];
        w_buffers[0] = header.as_bytes();
        for (i, req) in requests.iter_mut().enumerate() {
            w_buffers[1 + i] = req.req.as_bytes();
            w_buffers[1 + nreq + i] = req.wbuf;
            r_buffers.push(req.res.as_bytes_mut());
        }
        self.client
            .communicate(Command::ReadWrite, self.addr, &w_buffers, &mut r_buffers)?;
        Ok(())
    }

    /// Write some data to a given index group/offset and then read back some
    /// reply from there.  This is not the same as a write() followed by read();
    /// it is used as a kind of RPC call.
    pub fn write_read(
        &self,
        index_group: u32,
        index_offset: u32,
        write_data: &[u8],
        read_data: &mut [u8],
    ) -> Result<usize> {
        let header = IndexLengthRW {
            index_group: U32::new(index_group),
            index_offset: U32::new(index_offset),
            read_length: U32::new(read_data.len().try_into().map_err(Error::invalid_data)?),
            write_length: U32::new(write_data.len().try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);
        self.client.communicate(
            Command::ReadWrite,
            self.addr,
            &[header.as_bytes(), write_data],
            &mut [read_len.as_bytes_mut(), read_data],
        )?;
        Ok(read_len.get() as usize)
    }

    /// Like `write_read`, but ensure the returned data length matches the output buffer.
    pub fn write_read_exact(
        &self,
        index_group: u32,
        index_offset: u32,
        write_data: &[u8],
        read_data: &mut [u8],
    ) -> Result<()> {
        let len = self.write_read(index_group, index_offset, write_data, read_data)?;
        if len != read_data.len() {
            return Err(Error::io("got less data than expected"));
        }
        Ok(())
    }

    /// Write multiple index groups/offsets with one ADS request (a "sum-up" request).
    ///
    /// This function only returns Err on errors that cause the whole sum-up
    /// request to fail (e.g. if the device doesn't support such requests).  If
    /// the request as a whole succeeds, each single write/read can have
    /// returned its own error.  The [`WriteReadRequest::data`] method will
    /// return either the properly truncated returned data or the error for each
    /// write/read.
    pub fn write_read_multi(&self, requests: &mut [WriteReadRequest]) -> Result<()> {
        let nreq = requests.len();
        let read_len = requests
            .iter()
            .map(|r| size_of::<ResultLength>() + r.rbuf.len())
            .sum::<usize>();
        let write_len = requests
            .iter()
            .map(|r| size_of::<IndexLengthRW>() + r.wbuf.len())
            .sum::<usize>();
        let header = IndexLengthRW {
            index_group: U32::new(crate::index::SUMUP_READWRITE),
            index_offset: U32::new(u32::try_from(nreq).map_err(Error::invalid_data)?),
            read_length: U32::new(read_len.try_into().map_err(Error::invalid_data)?),
            write_length: U32::new(write_len.try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);
        let mut w_buffers = vec![&[][..]; 2 * nreq + 1];
        let mut r_buffers = (0..=2 * nreq).map(|_| &mut [][..]).collect_vec();
        w_buffers[0] = header.as_bytes();
        r_buffers[0] = read_len.as_bytes_mut();
        for (i, req) in requests.iter_mut().enumerate() {
            w_buffers[1 + i] = req.req.as_bytes();
            w_buffers[1 + nreq + i] = req.wbuf;
            r_buffers[1 + i] = req.res.as_bytes_mut();
            r_buffers[1 + nreq + i] = req.rbuf;
        }
        self.client
            .communicate(Command::ReadWrite, self.addr, &w_buffers, &mut r_buffers)?;
        // unfortunately SUMUP_READWRITE returns only the actual read bytes for each
        // request, so if there are short reads the buffers got filled wrongly
        fixup_write_read_return_buffers(requests);
        Ok(())
    }

    /// Return the ADS and device state of the device.
    pub fn get_state(&self) -> Result<(AdsState, u16)> {
        let mut state = ReadState::new_zeroed();
        self.client.communicate(
            Command::ReadState,
            self.addr,
            &[],
            &mut [state.as_bytes_mut()],
        )?;

        // Convert ADS state to the enum type
        let ads_state = AdsState::try_from(state.ads_state.get()).map_err(Error::io)?;

        Ok((ads_state, state.dev_state.get()))
    }

    /// (Try to) set the ADS and device state of the device.
    pub fn write_control(&self, ads_state: AdsState, dev_state: u16) -> Result<()> {
        let data = WriteControl {
            ads_state: U16::new(ads_state as _),
            dev_state: U16::new(dev_state),
            data_length: U32::new(0),
        };
        self.client.communicate(
            Command::WriteControl,
            self.addr,
            &[data.as_bytes()],
            &mut [],
        )?;
        Ok(())
    }

    /// Add a notification handle for some index group/offset.
    ///
    /// Notifications are delivered via a MPMC channel whose reading end can be
    /// obtained from `get_notification_channel` on the `Client` object.
    /// The returned `Handle` can be used to check which notification has fired.
    ///
    /// If the notification is not deleted explictly using `delete_notification`
    /// and the `Handle`, it is deleted when the `Client` object is dropped or shut down.
    ///
    /// NOTE: Notifications are not restored automatically if the remote is restarted.
    pub fn add_notification(
        &self,
        index_group: u32,
        index_offset: u32,
        attributes: &notif::Attributes,
    ) -> Result<notif::Handle> {
        let data = AddNotif {
            index_group: U32::new(index_group),
            index_offset: U32::new(index_offset),
            length: U32::new(attributes.length.try_into().map_err(Error::invalid_data)?),
            trans_mode: U32::new(attributes.trans_mode as u32),
            max_delay: U32::new(
                attributes
                    .max_delay
                    .as_millis()
                    .try_into()
                    .map_err(Error::invalid_data)?,
            ),
            cycle_time: U32::new(
                attributes
                    .cycle_time
                    .as_millis()
                    .try_into()
                    .map_err(Error::invalid_data)?,
            ),
            reserved: [0; 16],
        };
        let mut handle = U32::<LE>::new(0);
        self.client.communicate(
            Command::AddNotification,
            self.addr,
            &[data.as_bytes()],
            &mut [handle.as_bytes_mut()],
        )?;
        self.client
            .inner
            .notif_handles
            .lock()
            .insert((self.addr, handle.get()));
        Ok(handle.get())
    }

    /// Add a notification handle for a symbol.
    ///
    /// NOTE: Notifications are not restored automatically if the remote is restarted.
    pub fn add_symbol_notification(
        &self,
        symbol: &str,
        attributes: &notif::Attributes,
    ) -> Result<notif::Handle> {
        let (i_group, i_offset) = crate::symbol::get_location(self, symbol)?;
        self.add_notification(i_group, i_offset, attributes)
    }

    /// Add multiple notification handles.
    ///
    /// This function only returns Err on errors that cause the whole sum-up
    /// request to fail (e.g. if the device doesn't support such requests).  If
    /// the request as a whole succeeds, each single read can have returned its
    /// own error.  The [`AddNotifRequest::handle`] method will return either
    /// the returned handle or the error for each read.
    ///
    /// NOTE: Notifications are not restored automatically if the remote has been restarted
    pub fn add_notification_multi(&self, requests: &mut [AddNotifRequest]) -> Result<()> {
        let nreq = requests.len();
        let read_len = size_of::<ResultLength>() * nreq;
        let write_len = size_of::<AddNotif>() * nreq;
        let header = IndexLengthRW {
            index_group: U32::new(crate::index::SUMUP_ADDDEVNOTE),
            index_offset: U32::new(u32::try_from(nreq).map_err(Error::invalid_data)?),
            read_length: U32::new(read_len.try_into().map_err(Error::invalid_data)?),
            write_length: U32::new(write_len.try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);
        let mut w_buffers = vec![header.as_bytes()];
        let mut r_buffers = vec![read_len.as_bytes_mut()];
        for req in requests.iter_mut() {
            w_buffers.push(req.req.as_bytes());
            r_buffers.push(req.res.as_bytes_mut());
        }
        self.client
            .communicate(Command::ReadWrite, self.addr, &w_buffers, &mut r_buffers)?;
        for req in requests {
            if let Ok(handle) = req.handle() {
                self.client
                    .inner
                    .notif_handles
                    .lock()
                    .insert((self.addr, handle));
            }
        }
        Ok(())
    }

    /// Delete a notification with given handle.
    pub fn delete_notification(&self, handle: notif::Handle) -> Result<()> {
        self.client.communicate(
            Command::DeleteNotification,
            self.addr,
            &[U32::<LE>::new(handle).as_bytes()],
            &mut [],
        )?;
        self.client
            .inner
            .notif_handles
            .lock()
            .remove(&(self.addr, handle));
        Ok(())
    }

    /// Delete multiple notification handles.
    ///
    /// This function only returns Err on errors that cause the whole sum-up
    /// request to fail (e.g. if the device doesn't support such requests).  If
    /// the request as a whole succeeds, each single read can have returned its
    /// own error.  The [`DelNotifRequest::ensure`] method will return either the
    /// returned data or the error for each read.
    pub fn delete_notification_multi(&self, requests: &mut [DelNotifRequest]) -> Result<()> {
        let nreq = requests.len();
        let read_len = size_of::<u32>() * nreq;
        let write_len = size_of::<u32>() * nreq;
        let header = IndexLengthRW {
            index_group: U32::new(crate::index::SUMUP_DELDEVNOTE),
            index_offset: U32::new(u32::try_from(nreq).map_err(Error::invalid_data)?),
            read_length: U32::new(read_len.try_into().map_err(Error::invalid_data)?),
            write_length: U32::new(write_len.try_into().map_err(Error::invalid_data)?),
        };
        let mut read_len = U32::<LE>::new(0);
        let mut w_buffers = vec![header.as_bytes()];
        let mut r_buffers = vec![read_len.as_bytes_mut()];
        for req in requests.iter_mut() {
            w_buffers.push(req.req.as_bytes());
            r_buffers.push(req.res.as_bytes_mut());
        }
        self.client
            .communicate(Command::ReadWrite, self.addr, &w_buffers, &mut r_buffers)?;
        for req in requests {
            if req.ensure().is_ok() {
                self.client
                    .inner
                    .notif_handles
                    .lock()
                    .remove(&(self.addr, req.req.get()));
            }
        }
        Ok(())
    }

    /// Creates [`AdsMapping`] for the given symbol. The buffer size MUST be greater or equal to
    /// the target structure size (for reading). For writing the buffer size can be any, however it
    /// is still recommended to use the target structure size for the buffer pre-allocation.
    pub fn mapping(&self, symbol: &str, buf_size: usize) -> AdsMapping {
        AdsMapping::new(self, symbol, buf_size)
    }
}

/// Device info returned from an ADS server.
#[derive(Debug)]
pub struct DeviceInfo {
    /// Name of the ADS device/service.
    pub name: String,
    /// Major version.
    pub major: u8,
    /// Minor version.
    pub minor: u8,
    /// Build version.
    pub version: u16,
}

/// The ADS state of a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
#[repr(u16)]
pub enum AdsState {
    Invalid = 0,
    Idle = 1,
    Reset = 2,
    Init = 3,
    Start = 4,
    Run = 5,
    Stop = 6,
    SaveCfg = 7,
    LoadCfg = 8,
    PowerFail = 9,
    PowerGood = 10,
    Error = 11,
    Shutdown = 12,
    Suspend = 13,
    Resume = 14,
    Config = 15,
    Reconfig = 16,
    Stopping = 17,
    Incompatible = 18,
    Exception = 19,
}

impl TryFrom<u16> for AdsState {
    type Error = &'static str;

    fn try_from(value: u16) -> std::result::Result<Self, &'static str> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::Idle,
            2 => Self::Reset,
            3 => Self::Init,
            4 => Self::Start,
            5 => Self::Run,
            6 => Self::Stop,
            7 => Self::SaveCfg,
            8 => Self::LoadCfg,
            9 => Self::PowerFail,
            10 => Self::PowerGood,
            11 => Self::Error,
            12 => Self::Shutdown,
            13 => Self::Suspend,
            14 => Self::Resume,
            15 => Self::Config,
            16 => Self::Reconfig,
            17 => Self::Stopping,
            18 => Self::Incompatible,
            19 => Self::Exception,
            _ => return Err("invalid state constant"),
        })
    }
}

impl FromStr for AdsState {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match &*s.to_ascii_lowercase() {
            "invalid" => Self::Invalid,
            "idle" => Self::Idle,
            "reset" => Self::Reset,
            "init" => Self::Init,
            "start" => Self::Start,
            "run" => Self::Run,
            "stop" => Self::Stop,
            "savecfg" => Self::SaveCfg,
            "loadcfg" => Self::LoadCfg,
            "powerfail" => Self::PowerFail,
            "powergood" => Self::PowerGood,
            "error" => Self::Error,
            "shutdown" => Self::Shutdown,
            "suspend" => Self::Suspend,
            "resume" => Self::Resume,
            "config" => Self::Config,
            "reconfig" => Self::Reconfig,
            "stopping" => Self::Stopping,
            "incompatible" => Self::Incompatible,
            "exception" => Self::Exception,
            _ => return Err("invalid state name"),
        })
    }
}

// Structures used in communication, not exposed to user,
// but pub(crate) for the test suite.

#[derive(AsBytes, FromBytes, Debug)]
#[repr(C)]
pub(crate) struct AdsHeader {
    /// 0x0 - ADS command
    /// 0x1 - close port
    /// 0x1000 - open port
    /// 0x1001 - note from router (router state changed)
    /// 0x1002 - get local netid
    pub ams_cmd: u16,
    pub length: U32<LE>,
    pub dest_netid: AmsNetId,
    pub dest_port: U16<LE>,
    pub src_netid: AmsNetId,
    pub src_port: U16<LE>,
    pub command: U16<LE>,
    /// 0x01 - response
    /// 0x02 - no return
    /// 0x04 - ADS command
    /// 0x08 - system command
    /// 0x10 - high priority
    /// 0x20 - with time stamp (8 bytes added)
    /// 0x40 - UDP
    /// 0x80 - command during init phase
    /// 0x8000 - broadcast
    pub state_flags: U16<LE>,
    pub data_length: U32<LE>,
    pub error_code: U32<LE>,
    pub invoke_id: U32<LE>,
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct DeviceInfoRaw {
    pub major: u8,
    pub minor: u8,
    pub version: U16<LE>,
    pub name: [u8; 16],
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct IndexLength {
    pub index_group: U32<LE>,
    pub index_offset: U32<LE>,
    pub length: U32<LE>,
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct ResultLength {
    pub result: U32<LE>,
    pub length: U32<LE>,
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct IndexLengthRW {
    pub index_group: U32<LE>,
    pub index_offset: U32<LE>,
    pub read_length: U32<LE>,
    pub write_length: U32<LE>,
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct ReadState {
    pub ads_state: U16<LE>,
    pub dev_state: U16<LE>,
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct WriteControl {
    pub ads_state: U16<LE>,
    pub dev_state: U16<LE>,
    pub data_length: U32<LE>,
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct AddNotif {
    pub index_group: U32<LE>,
    pub index_offset: U32<LE>,
    pub length: U32<LE>,
    pub trans_mode: U32<LE>,
    pub max_delay: U32<LE>,
    pub cycle_time: U32<LE>,
    pub reserved: [u8; 16],
}

/// A single request for a [`Device::read_multi`] request.
pub struct ReadRequest<'buf> {
    req: IndexLength,
    res: ResultLength,
    rbuf: &'buf mut [u8],
}

impl<'buf> ReadRequest<'buf> {
    /// Create the request with given index group, index offset and result buffer.
    pub fn new(index_group: u32, index_offset: u32, buffer: &'buf mut [u8]) -> Result<Self> {
        Ok(Self {
            req: IndexLength {
                index_group: U32::new(index_group),
                index_offset: U32::new(index_offset),
                length: U32::new(u32::try_from(buffer.len()).map_err(Error::invalid_data)?),
            },
            res: ResultLength::new_zeroed(),
            rbuf: buffer,
        })
    }

    /// Get the actual returned data.
    ///
    /// If the request returned an error, returns Err.
    pub fn data(&self) -> Result<&[u8]> {
        if self.res.result.get() == 0 {
            Ok(&self.rbuf[..self.res.length.get() as usize])
        } else {
            ads_error("multi-read data", self.res.result.get())
        }
    }
}

/// A single request for a [`Device::write_multi`] request.
pub struct WriteRequest<'buf> {
    req: IndexLength,
    res: U32<LE>,
    wbuf: &'buf [u8],
}

impl<'buf> WriteRequest<'buf> {
    /// Create the request with given index group, index offset and input buffer.
    pub fn new(index_group: u32, index_offset: u32, buffer: &'buf [u8]) -> Result<Self> {
        Ok(Self {
            req: IndexLength {
                index_group: U32::new(index_group),
                index_offset: U32::new(index_offset),
                length: U32::new(u32::try_from(buffer.len()).map_err(Error::invalid_data)?),
            },
            res: U32::default(),
            wbuf: buffer,
        })
    }

    /// Verify that the data was successfully written.
    ///
    /// If the request returned an error, returns Err.
    pub fn ensure(&self) -> Result<()> {
        if self.res.get() == 0 {
            Ok(())
        } else {
            ads_error("multi-write data", self.res.get())
        }
    }
}

/// A single request for a [`Device::write_read_multi`] request.
pub struct WriteReadRequest<'buf> {
    req: IndexLengthRW,
    res: ResultLength,
    wbuf: &'buf [u8],
    rbuf: &'buf mut [u8],
}

impl<'buf> WriteReadRequest<'buf> {
    /// Create the request with given index group, index offset and input and
    /// result buffers.
    pub fn new(
        index_group: u32,
        index_offset: u32,
        write_buffer: &'buf [u8],
        read_buffer: &'buf mut [u8],
    ) -> Result<Self> {
        Ok(Self {
            req: IndexLengthRW {
                index_group: U32::new(index_group),
                index_offset: U32::new(index_offset),
                read_length: U32::new(
                    u32::try_from(read_buffer.len()).map_err(Error::invalid_data)?,
                ),
                write_length: U32::new(
                    u32::try_from(write_buffer.len()).map_err(Error::invalid_data)?,
                ),
            },
            res: ResultLength::new_zeroed(),
            wbuf: write_buffer,
            rbuf: read_buffer,
        })
    }

    /// Get the actual returned data.
    ///
    /// If the request returned an error, returns Err.
    pub fn data(&self) -> Result<&[u8]> {
        if self.res.result.get() == 0 {
            Ok(&self.rbuf[..self.res.length.get() as usize])
        } else {
            ads_error("multi-read/write data", self.res.result.get())
        }
    }
}

/// A single request for a [`Device::add_notification_multi`] request.
pub struct AddNotifRequest {
    req: AddNotif,
    res: ResultLength, // length is the handle
}

impl AddNotifRequest {
    /// Create the request with given index group, index offset and notification
    /// attributes.
    pub fn new(
        index_group: u32,
        index_offset: u32,
        attributes: &notif::Attributes,
    ) -> Result<Self> {
        Ok(Self {
            req: AddNotif {
                index_group: U32::new(index_group),
                index_offset: U32::new(index_offset),
                length: U32::new(u32::try_from(attributes.length).map_err(Error::invalid_data)?),
                trans_mode: U32::new(attributes.trans_mode as u32),
                max_delay: U32::new(
                    u32::try_from(attributes.max_delay.as_millis()).map_err(Error::invalid_data)?,
                ),
                cycle_time: U32::new(
                    u32::try_from(attributes.cycle_time.as_millis())
                        .map_err(Error::invalid_data)?,
                ),
                reserved: [0; 16],
            },
            res: ResultLength::new_zeroed(),
        })
    }

    /// Get the returned notification handle.
    ///
    /// If the request returned an error, returns Err.
    pub fn handle(&self) -> Result<notif::Handle> {
        if self.res.result.get() == 0 {
            Ok(self.res.length.get())
        } else {
            ads_error("multi-read/write data", self.res.result.get())
        }
    }
}

/// A single request for a [`Device::delete_notification_multi`] request.
pub struct DelNotifRequest {
    req: U32<LE>,
    res: U32<LE>,
}

impl DelNotifRequest {
    /// Create the request with given index group, index offset and notification
    /// attributes.
    pub fn new(handle: notif::Handle) -> Self {
        Self {
            req: U32::new(handle),
            res: U32::default(),
        }
    }

    /// Verify that the handle was successfully deleted.
    ///
    /// If the request returned an error, returns Err.
    pub fn ensure(&self) -> Result<()> {
        if self.res.get() == 0 {
            Ok(())
        } else {
            ads_error("multi-read/write data", self.res.get())
        }
    }
}

fn fixup_write_read_return_buffers(requests: &mut [WriteReadRequest]) {
    // Calculate the initial (using buffer sizes) and actual (using result
    // sizes) offsets of each request.
    let offsets = requests
        .iter()
        .scan((0, 0), |(init_cum, act_cum), req| {
            let (init, act) = (req.rbuf.len(), req.res.length.get() as usize);
            let current = Some((*init_cum, *act_cum, init, act));
            assert!(init >= act);
            *init_cum += init;
            *act_cum += act;
            current
        })
        .collect_vec();

    // Go through the buffers in reverse order.
    for i in (0..requests.len()).rev() {
        let (my_initial, my_actual, _, mut size) = offsets[i];
        if size == 0 {
            continue;
        }
        if my_initial == my_actual {
            // Offsets match, no further action required since all
            // previous buffers must be of full length too.
            break;
        }

        // Check in which buffer our last byte is.
        let mut j = offsets[..=i]
            .iter()
            .rposition(|r| r.0 < my_actual + size)
            .expect("index must be somewhere");
        let mut j_end = my_actual + size - offsets[j].0;

        // Copy the required number of bytes from every buffer from j up to i.
        loop {
            let n = j_end.min(size);
            size -= n;
            if i == j {
                requests[i].rbuf.copy_within(j_end - n..j_end, size);
            } else {
                let (first, second) = requests.split_at_mut(i);
                second[0].rbuf[size..][..n].copy_from_slice(&first[j].rbuf[j_end - n..j_end]);
            }
            if size == 0 {
                break;
            }
            j -= 1;
            j_end = offsets[j].2;
        }
    }
}

#[test]
fn test_fixup_buffers() {
    let mut buf0 = *b"12345678AB";
    let mut buf1 = *b"CDEFabc";
    let mut buf2 = *b"dxyUVW";
    let mut buf3 = *b"XYZY";
    let mut buf4 = *b"XW----";
    let mut buf5 = *b"-------------";
    let reqs = &mut [
        WriteReadRequest::new(0, 0, &[], &mut buf0).unwrap(),
        WriteReadRequest::new(0, 0, &[], &mut buf1).unwrap(),
        WriteReadRequest::new(0, 0, &[], &mut buf2).unwrap(),
        WriteReadRequest::new(0, 0, &[], &mut buf3).unwrap(),
        WriteReadRequest::new(0, 0, &[], &mut buf4).unwrap(),
        WriteReadRequest::new(0, 0, &[], &mut buf5).unwrap(),
    ];
    reqs[0].res.length.set(8);
    reqs[1].res.length.set(6);
    reqs[2].res.length.set(0);
    reqs[3].res.length.set(4);
    reqs[4].res.length.set(2);
    reqs[5].res.length.set(9);

    fixup_write_read_return_buffers(reqs);

    assert!(&reqs[5].rbuf[..9] == b"UVWXYZYXW");
    assert!(&reqs[4].rbuf[..2] == b"xy");
    assert!(&reqs[3].rbuf[..4] == b"abcd");
    assert!(&reqs[1].rbuf[..6] == b"ABCDEF");
    assert!(&reqs[0].rbuf[..8] == b"12345678");
}
