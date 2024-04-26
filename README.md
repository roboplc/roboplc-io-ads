<h2>
  RoboPLC Pro I/O connector for TwinCAT/ADS
  <a href="https://crates.io/crates/roboplc-io-ads"><img alt="crates.io page" src="https://img.shields.io/crates/v/roboplc-io-ads.svg"></img></a>
  <a href="https://docs.rs/roboplc-io-ads"><img alt="docs.rs page" src="https://docs.rs/roboplc-io-ads/badge.svg"></img></a>
</h2>

# Introduction

ADS is the native protocol used by programmable logic controllers (PLCs) and
the TwinCAT automation system produced by [Beckhoff GmbH](https://www.beckhoff.com/).

This crate provides I/O connector for [RoboPLC](https://www.roboplc.com/).

The crate IS NOT FREE for any commercial or production use. Please refer to
<https://github.com/roboplc/roboplc-io-ads/blob/main/LICENSE.md> for more
information.

The
[specification](https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_ads_over_mqtt/index.html&id=)
can be found on their Information System pages.

# Example

RoboPLC I/O mapping:

```rust,no_run
use ads::client::Client;
use roboplc::{comm::Timeouts, io::IoMapping, prelude::binrw};
use roboplc_io_ads as ads;
use std::time::Duration;

#[binrw]
struct MyStruct {
    field1: u32,
    field2: f32,
    field3: [u8; 8],
    field4: f64
}

// Open a connection to an ADS device identified by hostname/IP and port.
// For TwinCAT devices, a route must be set to allow the client to connect.
// The source AMS address is automatically generated from the local IP,
// but can be explicitly specified as the third argument.

// The socket is automatically reconnected if the connection is lost.
let (client, reader) = Client::new(("plchost", ads::PORT),
    Timeouts::new(Duration::from_secs(1)),
    ads::Source::Auto).unwrap();

// The reader thread MUST be started manually. Apply real-time settings if needed.
std::thread::spawn(move || { reader.run(); });

// Specify the target ADS device to talk to, by NetID and AMS port.
// Port 851 usually refers to the first PLC instance.
let device = client.device(ads::AmsAddr::new([5, 32, 116, 5, 1, 1].into(), 851));

// Create a mapping for a symbol. The mapping contains a handle (automatically recreated on
// each reconnect) as well as a pre-allocated buffer.
let mut mapping = device.mapping("MY_SYMBOL", 24);

// Read a structure from the PLC.
let mut data: MyStruct = mapping.read().unwrap();
data.field1 += 1;
// Write the modified structure back to the PLC.
mapping.write(&data).unwrap();
```

# Example

Direct usage:

```rust,no_run
use ads::client::Client;
use roboplc::comm::Timeouts;
use roboplc_io_ads as ads;
use std::time::Duration;

let (client, reader) = Client::new(("plchost", ads::PORT),
    Timeouts::new(Duration::from_secs(1)),
    ads::Source::Auto).unwrap();

std::thread::spawn(move || { reader.run(); });

let device = client.device(ads::AmsAddr::new([5, 32, 116, 5, 1, 1].into(), 851));

// Ensure that the PLC instance is running.
assert!(device.get_state().unwrap().0 == ads::AdsState::Run);

// Request a handle to a named symbol in the PLC instance.
let handle = ads::Handle::new(&device, "MY_SYMBOL").unwrap();

// Read data in form of an u32 from the handle.
let value: u32 = handle.read_value().unwrap();
println!("MY_SYMBOL value is {}", value);
```

The API slightly differs from the free version as many methods have been rewritten to less-panic
and thread-safe code. Certain internal types have been replaced with RoboPLC defaults.

The crate code is based on <https://github.com/birkenfeld/ads-rs> project, (c)
Georg Brandl, Serhij Symonenko and other contributors.

The commercial client additionally supports:

- Auto-reconnects
- Multi-threading
- Real-time safety
- Enterprise support from the vendor

Note: as the client has got an asynchronous-manner reader loop, it is HIGHLY RECOMMENDED to use
timeouts. In case if a remote does not respond, a request with no timeout gets stuck forever.

