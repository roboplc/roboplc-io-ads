use roboplc::{comm::Timeouts, prelude::*, time::interval, Result};
use roboplc_io_ads as ads;
use tracing::{error, info};

type Message = ();
type Variables = ();

#[binrw]
struct BusConfig {
    info: [BusInfo; 6],
}

#[binrw]
struct BusInfo {
    voltage: i32,
    current: [i32; 12],
}

#[derive(WorkerOpts)]
struct Test {
    client: ads::Client,
}

impl Worker<Message, Variables> for Test {
    fn run(&mut self, context: &Context<Message, Variables>) -> WResult {
        let device = self.client.device("10.90.1.6.1.1:851".parse().unwrap());
        let mut mapping = device.mapping("main.bus", 1024);
        for _ in interval(Duration::from_secs(1)) {
            if !context.is_online() {
                break;
            }
            let m = Monotonic::now();
            match mapping.read::<BusConfig>() {
                Ok(b) => {
                    info!(
                        w = self.worker_name(),
                        elapsed_us = m.elapsed().as_micros(),
                        v0 = b.info[0].voltage,
                        v2 = b.info[2].voltage
                    );
                }
                Err(error) => {
                    error!(worker=self.worker_name(), %error);
                }
            }
        }
        Ok(())
    }
}

#[derive(WorkerOpts)]
struct TestWriter {
    client: ads::Client,
}

impl Worker<Message, Variables> for TestWriter {
    fn run(&mut self, context: &Context<Message, Variables>) -> WResult {
        let device = self.client.device("10.90.1.6.1.1:851".parse().unwrap());
        let mut mapping = device.mapping("main.bus", 1024);
        for _ in interval(Duration::from_millis(200)) {
            match mapping.read::<BusConfig>() {
                Ok(mut b) => {
                    b.info[0].voltage += 1;
                    if let Err(error) = mapping.write(&b) {
                        error!(worker=self.worker_name(), %error);
                    }
                }
                Err(error) => {
                    error!(worker=self.worker_name(), %error);
                }
            }
            if !context.is_online() {
                break;
            }
        }
        Ok(())
    }
}

#[derive(WorkerOpts)]
#[worker_opts(blocking = true)]
struct AdsReader {
    reader: ads::Reader,
}

impl Worker<Message, Variables> for AdsReader {
    fn run(&mut self, _context: &Context<Message, Variables>) -> WResult {
        self.reader.run();
        Ok(())
    }
}

fn main() -> Result<()> {
    roboplc::setup_panic();
    roboplc::configure_logger(roboplc::LevelFilter::Info);
    let mut controller: Controller<Message, Variables> = Controller::new();
    let (client, reader) = ads::Client::new(
        ("10.90.1.6", ads::PORT),
        Timeouts::new(Duration::from_secs(5)),
        ads::Source::Auto,
    )?;
    controller.spawn_worker(AdsReader { reader })?;
    let device = client.device("10.90.1.6.1.1:851".parse().unwrap());
    info!(device=?device.get_state());
    controller.spawn_worker(Test {
        client: client.clone(),
    })?;
    controller.spawn_worker(TestWriter {
        client: client.clone(),
    })?;
    controller.register_signals(Duration::from_secs(5))?;
    controller.block();
    client.shutdown();
    Ok(())
}
