#![ doc = include_str!( concat!( env!( "CARGO_MANIFEST_DIR" ), "/", "README.md" ) ) ]
pub mod client;
pub mod errors;
pub mod file;
pub mod index;
pub mod mapping;
pub mod netid;
pub mod notif;
pub mod ports;
pub mod strings;
pub mod symbol;
#[cfg(test)]
mod test;
pub mod udp;

pub use client::{AdsState, Client, Device, Reader, Source};
pub use file::File;
pub use mapping::AdsMapping;
pub use netid::{AmsAddr, AmsNetId, AmsPort};
pub use symbol::Handle;

/// The default port for TCP communication.
pub const PORT: u16 = 0xBF02;
/// The default port for UDP communication.
pub const UDP_PORT: u16 = 0xBF03;
