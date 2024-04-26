//! Implements the Beckhoff UDP message protocol for basic operations.

use std::convert::TryInto;
use std::io::Write;
use std::net::{ToSocketAddrs, UdpSocket};
use std::{char, iter, str};

use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt, LE};
use zerocopy::byteorder::{U16, U32};
use zerocopy::{AsBytes, FromBytes};

use crate::{AmsAddr, AmsNetId};
use roboplc::{Error, Result};

/// Magic number for the first four bytes of each UDP packet.
pub const BECKHOFF_UDP_MAGIC: u32 = 0x_71_14_66_03;

/// Represents a message in the UDP protocol.
pub struct Message {
    items: Vec<(u16, usize, usize)>,
    data: Vec<u8>,
}

/// The operation that the PLC should execute.
#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub enum ServiceId {
    /// Identify the PLC, TwinCAT and OS versions.
    Identify = 1,
    /// Add a routing entry to the router.
    AddRoute = 6,
}

/// Identifies a piece of information in the UDP message.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
#[repr(u16)]
pub enum Tag {
    Status = 1,
    Password = 2,
    TCVersion = 3,
    OSVersion = 4,
    ComputerName = 5,
    NetID = 7,
    Options = 9,
    RouteName = 12,
    UserName = 13,
    Fingerprint = 18,
}

impl Message {
    /// Create a new UDP message backed by a byte vector.
    pub fn new(service: ServiceId, source: AmsAddr) -> Self {
        let header = UdpHeader {
            magic: U32::new(BECKHOFF_UDP_MAGIC),
            invoke_id: U32::new(0),
            service: U32::new(service as u32),
            src_netid: source.netid(),
            src_port: U16::new(source.port()),
            num_items: U32::new(0), // will be adapted later
        };
        let data = header.as_bytes().to_vec();
        Self {
            items: Vec::with_capacity(8),
            data,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn set_service(&mut self, service: ServiceId, reply: bool) {
        let service = service as u32 | (if reply { 0x8000_0000 } else { 0 });
        LE::write_u32(&mut self.data[8..12], service);
    }

    /// Parse a UDP message from a byte slice.
    pub fn parse(data: &[u8], exp_service: ServiceId, reply: bool) -> Result<Self> {
        let exp_service = exp_service as u32 | (if reply { 0x8000_0000 } else { 0 });
        Self::parse_internal(data, exp_service)
    }

    fn parse_internal(data: &[u8], exp_service: u32) -> Result<Self> {
        let mut data_ptr = data;
        let magic = data_ptr.read_u32::<LE>()?;
        let invoke_id = data_ptr.read_u32::<LE>()?;
        let rep_service = data_ptr.read_u32::<LE>()?;
        if magic != BECKHOFF_UDP_MAGIC {
            return Err(Error::io("parsing UDP packet: invalid magic"));
        }
        if invoke_id != 0 {
            // we're only generating 0
            return Err(Error::io("parsing UDP packet: invalid invoke ID"));
        }
        if rep_service != exp_service {
            return Err(Error::io("parsing UDP packet: invalid service ID"));
        }
        let _src = AmsAddr::read_from(&mut data_ptr)?;
        let n_items = data_ptr.read_u32::<LE>()?;

        let mut items = Vec::with_capacity(n_items as usize);
        {
            let mut pos = 28;
            while let Ok(tag) = data_ptr.read_u16::<LE>() {
                let len = data_ptr.read_u16::<LE>()? as usize;
                items.push((tag, pos, pos + len));
                pos += len + 4;
                data_ptr = &data_ptr[len..];
            }
        }
        Ok(Self {
            data: data.to_vec(),
            items,
        })
    }

    /// Add a tag containing arbitrary bytes.
    pub fn add_bytes(&mut self, tag: Tag, data: &[u8]) -> Result<()> {
        self.data.write_u16::<LE>(tag as u16)?;
        let start = self.data.len();
        self.data
            .write_u16::<LE>(u16::try_from(data.len()).map_err(Error::invalid_data)?)?;
        self.data.write_all(data)?;
        self.items.push((tag as u16, start, self.data.len()));
        LE::write_u32(
            &mut self.data[20..],
            u32::try_from(self.items.len()).map_err(Error::invalid_data)?,
        );
        Ok(())
    }

    /// Add a tag containing a string with null terminator.
    pub fn add_str(&mut self, tag: Tag, data: &str) -> Result<()> {
        self.data.write_u16::<LE>(tag as u16)?;
        let start = self.data.len();
        // add the null terminator
        self.data
            .write_u16::<LE>(u16::try_from(data.len() + 1).map_err(Error::invalid_data)?)?;
        self.data.write_all(data.as_bytes())?;
        self.data.write_u8(0)?;
        self.items.push((tag as u16, start, self.data.len()));
        LE::write_u32(
            &mut self.data[20..],
            u32::try_from(self.items.len()).map_err(Error::invalid_data)?,
        );
        Ok(())
    }

    /// Add a tag containing an u32.
    pub fn add_u32(&mut self, tag: Tag, data: u32) -> Result<()> {
        self.data.write_u16::<LE>(tag as u16)?;
        let start = self.data.len();
        self.data.write_u16::<LE>(4)?;
        self.data.write_u32::<LE>(data)?;
        self.items.push((tag as u16, start, self.data.len()));
        LE::write_u32(
            &mut self.data[20..],
            u32::try_from(self.items.len()).map_err(Error::invalid_data)?,
        );
        Ok(())
    }

    fn map_tag<'a, O, F>(&'a self, tag: Tag, map: F) -> Option<O>
    where
        F: Fn(&'a [u8]) -> Option<O>,
    {
        self.items
            .iter()
            .find(|item| item.0 == tag as u16)
            .and_then(|&(_, i, j)| map(&self.data[i..j]))
    }

    /// Get the data for given tag as bytes.
    pub fn get_bytes(&self, tag: Tag) -> Option<&[u8]> {
        self.map_tag(tag, Some)
    }

    /// Get the data for given tag as null-terminated string.
    pub fn get_str(&self, tag: Tag) -> Option<&str> {
        // exclude the null terminator
        self.map_tag(tag, |b| str::from_utf8(&b[..b.len() - 1]).ok())
    }

    /// Get the data for given tag as a u32.
    pub fn get_u32(&self, tag: Tag) -> Option<u32> {
        self.map_tag(tag, |mut b| b.read_u32::<LE>().ok())
    }

    /// Get the AMS address originating the message.
    ///
    /// # Panics
    ///
    /// Should not panic
    pub fn get_source(&self) -> AmsAddr {
        AmsAddr::read_from(&mut &self.data[12..20]).expect("size")
    }

    /// Create a complete UDP packet from the message and its header.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Send the packet and receive a reply from the server.
    pub fn send_receive(&self, to: impl ToSocketAddrs) -> Result<Self> {
        // Send self as a request.
        let sock = UdpSocket::bind("0.0.0.0:0")?;
        sock.send_to(self.as_bytes(), to)?;

        // Receive the reply.
        let mut reply = [0; 576];
        sock.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
        let (n, _) = sock.recv_from(&mut reply)?;

        // Parse the reply.
        Self::parse_internal(&reply[..n], LE::read_u32(&self.data[8..]) | 0x8000_0000)
    }
}

/// Send a UDP message for setting a route.
///
/// - `target`: (host, port) of the AMS router to add the route to
///   (the port should normally be `ads::UDP_PORT`)
/// - `netid`: the NetID of the route's target
/// - `host`: the IP address or hostname of the route's target (when using
///   hostnames instead of IP addresses, beware of Windows hostname resolution)
/// - `routename`: name of the route, default is `host`
/// - `username`: system username for the router, default is `Administrator`
/// - `password`: system password for the given user, default is `1`
/// - `temporary`: marks the route as "temporary"
pub fn add_route(
    target: (&str, u16),
    netid: AmsNetId,
    host: &str,
    routename: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
    temporary: bool,
) -> Result<()> {
    let mut packet = Message::new(ServiceId::AddRoute, AmsAddr::new(netid, 0));
    packet.add_bytes(Tag::NetID, &netid.0)?;
    packet.add_str(Tag::ComputerName, host)?;
    packet.add_str(Tag::UserName, username.unwrap_or("Administrator"))?;
    packet.add_str(Tag::Password, password.unwrap_or("1"))?;
    packet.add_str(Tag::RouteName, routename.unwrap_or(host))?;
    if temporary {
        packet.add_u32(Tag::Options, 1)?;
    }

    let reply = packet.send_receive(target)?;

    match reply.get_u32(Tag::Status) {
        None => Err(Error::io("setting route: no status in reply")),
        Some(0) => Ok(()),
        Some(n) => crate::errors::ads_error("setting route", n),
    }
}

/// Send a UDP message for querying remote system NetID.
pub fn get_netid(target: (&str, u16)) -> Result<AmsNetId> {
    let packet = Message::new(ServiceId::Identify, AmsAddr::default());
    let reply = packet.send_receive(target)?;
    Ok(reply.get_source().netid())
}

/// Information about the system running TwinCAT.
pub struct SysInfo {
    /// AMS NetID of the system.
    pub netid: AmsNetId,
    /// Hostname of the system.
    pub hostname: String,
    /// The TwinCAT (major, minor, build) version.
    pub twincat_version: (u8, u8, u16),
    /// The OS (name, major, minor, build, service_pack) version.
    pub os_version: (&'static str, u32, u32, u32, String),
    /// The system's fingerprint.
    pub fingerprint: String,
}

/// Send a UDP message for querying remote system information.
pub fn get_info(target: (&str, u16)) -> Result<SysInfo> {
    let request = Message::new(ServiceId::Identify, AmsAddr::default());
    let reply = request.send_receive(target)?;

    // Parse TwinCAT version.
    let tcver = reply.get_bytes(Tag::TCVersion).unwrap_or(&[]);
    let twincat_version = if tcver.len() >= 4 {
        let tcbuild = u16::from_le_bytes(tcver[2..4].try_into().map_err(Error::invalid_data)?);
        (tcver[0], tcver[1], tcbuild)
    } else {
        (0, 0, 0)
    };

    // Parse OS version.  This is a Windows OSVERSIONINFO structure, which
    // consists of major/minor/build versions, the platform, and a "service
    // pack" string, coded as UTF-16.  It is not known how the data looks on
    // non-Windows devices, but hopefully the format is kept the same.
    let os_version = if let Some(mut bytes) = reply.get_bytes(Tag::OSVersion) {
        if bytes.len() >= 22 {
            // Size of the structure (redundant).
            let _ = bytes.read_u32::<LE>().map_err(Error::invalid_data)?;
            let major = bytes.read_u32::<LE>().map_err(Error::invalid_data)?;
            let minor = bytes.read_u32::<LE>().map_err(Error::invalid_data)?;
            let build = bytes.read_u32::<LE>().map_err(Error::invalid_data)?;
            let platform = match bytes.read_u32::<LE>().map_err(Error::invalid_data)? {
                1 => "TC/RTOS",
                2 => "Windows NT",
                3 => "Windows CE",
                _ => "Unknown platform",
            };
            let string = if platform == "TC/RTOS" {
                bytes
                    .iter()
                    .take_while(|&&b| b != 0)
                    .map(|&b| b as char)
                    .collect()
            } else {
                iter::from_fn(|| bytes.read_u16::<LE>().ok())
                    .take_while(|&ch| ch != 0)
                    .filter_map(|ch| char::from_u32(u32::from(ch)))
                    .collect()
            };
            (platform, major, minor, build, string)
        } else {
            ("Unknown OS info format", 0, 0, 0, String::new())
        }
    } else {
        ("No OS info", 0, 0, 0, String::new())
    };
    Ok(SysInfo {
        netid: reply.get_source().netid(),
        hostname: reply.get_str(Tag::ComputerName).unwrap_or("unknown").into(),
        twincat_version,
        os_version,
        fingerprint: reply.get_str(Tag::Fingerprint).unwrap_or_default().into(),
    })
}

#[derive(FromBytes, AsBytes, Default)]
#[repr(C)]
pub(crate) struct UdpHeader {
    magic: U32<LE>,
    invoke_id: U32<LE>,
    service: U32<LE>,
    src_netid: AmsNetId,
    src_port: U16<LE>,
    num_items: U32<LE>,
}
