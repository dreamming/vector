extern crate myvector;

#[macro_use]
extern crate log;
extern crate regex;

use regex::bytes::RegexSet;
use myvector::{splunk, transforms::Sampler, transport::Coordinator};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn main() {
    myvector::setup_logger();

    // keep track of last offset of upstream producers so consumers know when to quit
    let last_input_offset = Arc::new(AtomicUsize::new(0));
    let last_output_offset = Arc::new(AtomicUsize::new(0));

    // build up producer/consumer graph first so everything starts at the beginning
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

    // let source = console::Source::new(input_log);
    let source = splunk::RawTcpSource::new(input_log);
    let pass_list = RegexSet::new(&["important"]).unwrap();
    let sampler = Sampler::new(
        10,
        pass_list,
        input_consumer,
        output_log,
        last_input_offset.clone(),
    );
    // let sink = console::Sink::new(output_consumer, last_output_offset.clone());
    let sink = splunk::RawTcpSink::new(
        output_consumer,
        "localhost:9999",
        last_output_offset.clone(),
    );

    info!("starting source");
    let source_handle = source.run();
    let sampler_handle = sampler.run();
    let sink_handle = sink.run();

    // wait for source to finish (i.e. consume all of stdin)
    let input_end_offset = source_handle.join().unwrap();
    info!("source finished at offset {}", input_end_offset);

    // tell sampler we're done
    last_input_offset.store(input_end_offset as usize, Ordering::Relaxed);

    // wait for sampler to finish
    let output_end_offset = sampler_handle.join().unwrap();
    info!("sampler finished at offset {}", output_end_offset);

    // tell sink we're done
    last_output_offset.store(output_end_offset as usize, Ordering::Relaxed);

    // wait for sink to finish
    sink_handle.join().unwrap();
}