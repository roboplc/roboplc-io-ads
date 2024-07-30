use ads::notif::{Attributes, Sample, TransmissionMode};
use roboplc::{comm::Timeouts, locking::Mutex, prelude::*, time::interval, Result};
use roboplc_io_ads as ads;
use tracing::{error, info, warn};

const DEVICE: &str = "10.90.1.6.1.1:851";
const SYMBOL_COUNTER: &str = "MAIN.COUNTER";

type Message = ();

#[derive(Default)]
struct Variables {
    nh: Mutex<NHandles>,
}

#[binrw]
struct BusConfig {
    info: [BusInfo; 6],
}

#[binrw]
struct BusInfo {
    voltage: i32,
    current: [i32; 12],
}

#[derive(Default)]
struct NHandles {
    counter: u32,
}

/// Notification subscriber. Certain low-level methods require manually checking session id as
/// handles and notification subscriptions may be lost if the remote has been restarted.
///
/// Note that session id change does not guarantee that the remote has been restarted, it may be
/// related to other factors, such as network issues. Use with caution.
#[derive(WorkerOpts)]
#[worker_opts(blocking = true)]
struct NotifSub {
    client: ads::Client,
}

impl NotifSub {
    fn create_handles(&mut self, nh: &mut NHandles) -> Result<()> {
        let device = self.client.device(DEVICE.parse().unwrap());
        let attrs = Attributes::new(
            4,
            TransmissionMode::ServerOnChange,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        nh.counter = device.add_symbol_notification(SYMBOL_COUNTER, &attrs)?;
        Ok(())
    }
}

impl Worker<Message, Variables> for NotifSub {
    fn run(&mut self, context: &Context<Message, Variables>) -> WResult {
        let mut sess_id_created: Option<usize> = None;
        for _ in interval(Duration::from_secs(5)) {
            let Ok(s_lock) = self.client.lock_session() else {
                continue;
            };
            if sess_id_created.map_or(false, |id| id == s_lock.session_id()) {
                // notification handles already created for the session
                continue;
            }
            info!(session_id = s_lock.session_id(), "recreating handles");
            if let Err(e) = self.create_handles(&mut context.variables().nh.lock()) {
                error!(worker=self.worker_name(), %e);
                continue;
            }
            sess_id_created.replace(s_lock.session_id());
        }
        Ok(())
    }
}

#[derive(WorkerOpts)]
#[worker_opts(name = "nhandler", blocking = true)]
struct NotificationHandler {
    client: ads::Client,
}

impl NotificationHandler {
    fn process_sample(&self, mut sample: Sample, nh: &NHandles) -> Result<()> {
        match sample.handle {
            v if v == nh.counter => {
                let c: Counter = sample.read()?;
                info!(counter = c.counter);
            }
            _ => {
                warn!(
                    worker = self.worker_name(),
                    handle = sample.handle,
                    "unknown handle"
                );
            }
        }
        Ok(())
    }
}

impl Worker<Message, Variables> for NotificationHandler {
    fn run(&mut self, context: &Context<Message, Variables>) -> WResult {
        let rx = self.client.get_notification_channel();
        loop {
            match rx.recv() {
                Ok(frame) => {
                    for sample in frame.samples() {
                        if let Err(e) = self.process_sample(sample, &context.variables().nh.lock())
                        {
                            error!(worker=self.worker_name(), %e);
                        }
                    }
                }
                Err(e) => {
                    error!(worker=self.worker_name(), %e);
                    break;
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

#[binrw]
#[derive(Default)]
struct Counter {
    counter: i32,
}

impl Worker<Message, Variables> for TestWriter {
    fn run(&mut self, context: &Context<Message, Variables>) -> WResult {
        let device = self.client.device(DEVICE.parse().unwrap());
        let mut mapping = device.mapping(SYMBOL_COUNTER, 8);
        let mut counter = Counter::default();
        for _ in interval(Duration::from_millis(500)) {
            if let Err(e) = mapping.write(&counter) {
                error!(worker=self.worker_name(), %e);
            } else {
                info!(counter = counter.counter, "written");
            }
            counter.counter += 1;
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
    roboplc::configure_logger(roboplc::LevelFilter::Info);
    let mut controller: Controller<Message, Variables> = Controller::new();
    let (client, reader) = ads::Client::new(
        ("10.90.1.6", ads::PORT),
        Timeouts::new(Duration::from_secs(2)),
        ads::Source::Auto,
    )?;
    controller.spawn_worker(AdsReader { reader })?;
    controller.spawn_worker(NotifSub {
        client: client.clone(),
    })?;
    controller.spawn_worker(NotificationHandler {
        client: client.clone(),
    })?;
    controller.spawn_worker(TestWriter {
        client: client.clone(),
    })?;
    controller.register_signals(Duration::from_secs(30))?;
    controller.block();
    client.shutdown();
    info!("shut down");
    Ok(())
}
