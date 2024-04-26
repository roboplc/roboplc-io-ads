//! Everything to do with ADS notifications.

use std::io::{self, Cursor};
use std::time::Duration;

use bma_ts::Timestamp;
use byteorder::{ReadBytesExt, LE};
use roboplc::{io::IoMapping, DataDeliveryPolicy, Error, Result};

use crate::client::AMS_HEADER_SIZE;

/// A handle to the notification; this can be used to delete the notification later.
pub type Handle = u32;

/// Attributes for creating a notification.
pub struct Attributes {
    /// Length of data the notification is interested in.
    pub length: usize,
    /// When notification messages should be transmitted.
    pub trans_mode: TransmissionMode,
    /// The maximum delay between change and transmission.
    pub max_delay: Duration,
    /// The cycle time for checking for changes.
    pub cycle_time: Duration,
}

impl Attributes {
    /// Return new notification attributes.
    pub fn new(
        length: usize,
        trans_mode: TransmissionMode,
        max_delay: Duration,
        cycle_time: Duration,
    ) -> Self {
        Self {
            length,
            trans_mode,
            max_delay,
            cycle_time,
        }
    }
}

/// When notifications should be generated.
#[repr(u32)]
#[derive(Clone, Copy, Debug)]
pub enum TransmissionMode {
    /// No transmission.
    NoTrans = 0,
    /// Notify each server cycle.
    ServerCycle = 3,
    /// Notify when the content changes.
    ServerOnChange = 4,
    // Other constants from the C++ library:
    // ClientCycle = 1,
    // ClientOnChange = 2,
    // ServerCycle2 = 5,
    // ServerOnChange2 = 6,
    // Client1Req = 10,
}

/// A notification message from the ADS server.
pub struct Notification {
    data: Vec<u8>,
    nstamps: u32,
}

impl DataDeliveryPolicy for Notification {}

impl std::fmt::Debug for Notification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Notification [")?;
        for sample in self.samples() {
            writeln!(f, "    {:?}", sample)?;
        }
        write!(f, "]")
    }
}

impl Notification {
    /// Parse a notification message from an ADS message.
    pub fn new(data: impl Into<Vec<u8>>) -> Result<Self> {
        // Relevant data starts at byte 42 with the number of stamps.
        let data = data.into();
        if data.len() < AMS_HEADER_SIZE + 8 {
            // header + length + #stamps
            return Err(Error::io(io::ErrorKind::UnexpectedEof));
        }
        let mut ptr = &data[AMS_HEADER_SIZE + 4..];
        let nstamps = ptr.read_u32::<LE>()?;
        for _ in 0..nstamps {
            let _timestamp = ptr.read_u64::<LE>()?;
            let nsamples = ptr.read_u32::<LE>()?;

            for _ in 0..nsamples {
                let _handle = ptr.read_u32::<LE>()?;
                let length = ptr.read_u32::<LE>()? as usize;
                if ptr.len() >= length {
                    ptr = &ptr[length..];
                } else {
                    return Err(Error::io(io::ErrorKind::UnexpectedEof));
                }
            }
        }
        if ptr.is_empty() {
            Ok(Self { data, nstamps })
        } else {
            Err(Error::io(io::ErrorKind::UnexpectedEof))
        }
    }

    /// Return an iterator over all data samples in this notification.
    pub fn samples(&self) -> SampleIter<'_> {
        SampleIter {
            data: &self.data[46..],
            cur_timestamp: 0,
            stamps_left: self.nstamps,
            samples_left: 0,
        }
    }
}

/// A single sample in a notification message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sample<'a> {
    /// The notification handle associated with the data.
    pub handle: Handle,
    /// Timestamp of generation (already converted to UNIX)
    pub timestamp: Timestamp,
    /// Data of the handle at the specified time.
    pub data: &'a [u8],
}

impl<'d> IoMapping for Sample<'d> {
    type Options = ();

    fn read<T>(&mut self) -> Result<T>
    where
        T: for<'a> roboplc::prelude::BinRead<Args<'a> = ()>,
    {
        let mut c = Cursor::new(self.data);
        let res: T = T::read_le(&mut c)?;
        Ok(res)
    }

    fn write<T>(&mut self, _value: T) -> Result<()>
    where
        T: for<'a> roboplc::prelude::BinWrite<Args<'a> = ()>,
    {
        Err(Error::Unimplemented)
    }
}

/// An iterator over all samples within a notification message.
pub struct SampleIter<'a> {
    data: &'a [u8],
    cur_timestamp: u64,
    stamps_left: u32,
    samples_left: u32,
}

impl<'a> Iterator for SampleIter<'a> {
    type Item = Sample<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.samples_left > 0 {
            // Read more samples from the current stamp.
            let handle = self.data.read_u32::<LE>().expect("size");
            let length = self.data.read_u32::<LE>().expect("size") as usize;
            let (data, rest) = self.data.split_at(length);
            self.data = rest;
            self.samples_left -= 1;
            Some(Sample {
                handle,
                data,
                timestamp: Timestamp::from_nanos(
                    self.cur_timestamp.checked_mul(100).unwrap_or_default(),
                )
                .try_from_ansi_to_unix()
                .unwrap_or_default(),
            })
        } else if self.stamps_left > 0 {
            // Go to next stamp.
            self.cur_timestamp = self.data.read_u64::<LE>().expect("size");
            self.samples_left = self.data.read_u32::<LE>().expect("size");
            self.stamps_left -= 1;
            self.next()
        } else {
            // Nothing else here.
            None
        }
    }
}
