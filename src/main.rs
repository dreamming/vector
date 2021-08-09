extern crate myvector;

#[macro_use]
extern crate log;
extern crate fern;
extern crate memchr;

use memchr::memchr;
use myvector::transport::Coordinator;
use myvector::{ConsoleSink, ConsoleSource, Sampler};
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{atomic::AtomicBool, Arc};
use std::thread;
use myvector::splunk::RawTcpSource;

fn main() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stderr())
        .apply()
        .unwrap();

    let last_input_offset = Arc::new(AtomicUsize::new(0));
    let last_output_offset = Arc::new(AtomicUsize::new(0));
    let mut coordinator = Coordinator::new("logs");
    let input_log = coordinator
        .create_log("input")
        .expect("failed to create log");
    let input_consumer = coordinator
        .build_consumer("input")
        .expect("failed to build consumer");

    let output_log = coordinator
        .create_log("output")
        .expect("failed to create log");
    let output_consumer = coordinator
        .build_consumer("output")
        .expect("failed to build consumer");

    // let source = ConsoleSource::new(input_log);
    let source = RawTcpSource::new(input_log);
    let sampler = Sampler::new(99, input_consumer, output_log, last_input_offset.clone());
    let sink = ConsoleSink::new(output_consumer, last_output_offset.clone());

    info!("starting source");
    let source_handle = source.run();
    let sampler_handle = sampler.run();
    let sink_handle = sink.run();

    let input_end_offset = source_handle.join().unwrap();
    info!("source finished at offset {}", input_end_offset);
    last_input_offset.store(input_end_offset as usize, Ordering::Relaxed);

    info!("starting sampler");
    let output_end_offset = sampler_handle.join().unwrap();
    info!("sampler finished at offset {}", output_end_offset);
    last_output_offset.store(output_end_offset as usize, Ordering::Relaxed);

    info!("starting sink");
    sink_handle.join().unwrap();
}
