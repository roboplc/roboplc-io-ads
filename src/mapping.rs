use std::io::Cursor;

use roboplc::io::binrw::BinWrite;
use roboplc::io::{binrw::BinRead, IoMapping};
use roboplc::{Error, Result};

use crate::{Device, Handle};

#[allow(clippy::module_name_repetitions)]
pub struct AdsMapping {
    device: Device,
    buf: Vec<u8>,
    symbol: String,
    session_id: usize,
    handle: Option<Handle>,
}

impl AdsMapping {
    /// The buffer size MUST be greater or equal to the target structure size (for reading). For
    /// writing the buffer size can be any, however it is still recommended to use the target
    /// structure size for the buffer pre-allocation.
    pub fn new(device: &Device, symbol: &str, buf_size: usize) -> Self {
        Self {
            device: device.clone(),
            buf: vec![0; buf_size],
            symbol: symbol.to_owned(),
            session_id: 0,
            handle: None,
        }
    }
    fn get_handle(&mut self) -> Result<&Handle> {
        let session_id = self.device.client.session_id();
        if self.handle.is_none() || self.session_id != session_id {
            self.handle = Some(Handle::new(&self.device, &self.symbol)?);
            self.session_id = session_id;
        }
        Ok(self.handle.as_ref().unwrap())
    }
}

impl IoMapping for AdsMapping {
    type Options = ();

    fn read<T>(&mut self) -> Result<T>
    where
        T: for<'a> BinRead<Args<'a> = ()>,
    {
        let handle_id = self.get_handle()?.raw();
        let len = self
            .device
            .read(crate::index::RW_SYMVAL_BYHANDLE, handle_id, &mut self.buf)?;
        if len > self.buf.len() {
            return Err(Error::io("buffer overflow"));
        }
        let mut c = Cursor::new(&self.buf[..len]);
        let res: T = T::read_le(&mut c)?;
        Ok(res)
    }

    fn write<T>(&mut self, value: T) -> Result<()>
    where
        T: for<'a> BinWrite<Args<'a> = ()>,
    {
        let handle_id = self.get_handle()?.raw();
        let mut c = Cursor::new(&mut self.buf);
        value.write_le(&mut c)?;
        let pos = usize::try_from(c.position()).map_err(Error::invalid_data)?;
        self.device.write(
            crate::index::RW_SYMVAL_BYHANDLE,
            handle_id,
            &self.buf[..pos],
        )?;
        Ok(())
    }
}
