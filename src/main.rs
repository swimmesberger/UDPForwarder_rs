use clap::{App, Arg};
use std::net::SocketAddr;
use crate::fudp::util::ForwardingConfiguration;

mod fudp;

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;
// mimalloc would be the better choice but there seems to be build issues on windows atm.
//static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:0";

fn main() -> std::io::Result<()> {
    let matches = App::new("udp_forwarder")
        .version("0.3.1")
        .about("Forwards UDP data from one port to multiple others")
        .author("Simon Wimmesberger")
        .arg(Arg::with_name("input-address").short("i").help("The address the server should listen on").required(false).default_value("").takes_value(true))
        .arg(Arg::with_name("output-address").short("o").help("The address the server should send data to").required(true).multiple(true).takes_value(true))
        .arg(Arg::with_name("async").short("a").required(false).help("Should the server handle udp async or not"))
        .arg(Arg::with_name("queue").short("q").required(false).help("Enables queueing in blocking mode"))
        .arg(Arg::with_name("block").short("b").required(false).help("Enables blocking mode without queue"))
        .arg(Arg::with_name("sending").short("s").required(false).help("Switches to sending mode"))
        .arg(Arg::with_name("packet-size").short("p").required(false).takes_value(true).default_value("300").help("Set's the send packet size"))
        .arg(Arg::with_name("send-threads").short("t").required(false).takes_value(true).default_value("").help("The number of threads used for sending"))
        .arg(Arg::with_name("receive-threads").short("r").required(false).takes_value(true).default_value("").help("The number of threads used for receiving"))
        .get_matches();

    let mut listen_address = matches.value_of("input-address").unwrap();
    let peers: Vec<SocketAddr> = matches.values_of("output-address").unwrap().map(|address| address.parse().unwrap()).collect();
    let is_async = matches.values_of("async").is_some();
    let mut is_queue = matches.values_of("queue").is_some();
    let mut is_block = matches.values_of("block").is_some();
    let mut is_sending = matches.values_of("sending").is_some();
    let send_packet_size: usize = matches.value_of("packet-size").unwrap().parse().unwrap();
    let no_listen_address = listen_address.is_empty();
    let mut send_threads_s = matches.value_of("send-threads").unwrap();
    let mut receive_threads_s = matches.value_of("receive-threads").unwrap();

    if no_listen_address {
        listen_address = DEFAULT_LISTEN_ADDRESS;
        is_sending = true;
    }

    if !is_sending && !is_async && !is_queue && !is_block {
        if peers.len() <= 1 {
            is_block = true;
        } else {
            is_queue = true;
        }
    }

    if send_threads_s.is_empty() {
        if is_async {
            send_threads_s = "1";
        } else if is_queue {
            send_threads_s = "2";
        } else if is_sending {
            send_threads_s = "2";
        } else if is_block {
            send_threads_s = "2";
        }
    }

    if receive_threads_s.is_empty() {
        if is_async {
            receive_threads_s = "1";
        } else if is_queue {
            receive_threads_s = "1";
        } else if is_sending {
            receive_threads_s = "0";
        } else if is_block {
            receive_threads_s = "0";
        }
    }

    let send_threads: usize = send_threads_s.parse().unwrap();
    let receive_threads: usize = receive_threads_s.parse().unwrap();

    // print packets per second every second
    let pps = &mut fudp::util::PacketsPerSecond::new(1000);
    pps.start().unwrap();

    let config = ForwardingConfiguration::new(listen_address, &peers, pps, send_packet_size, send_threads, receive_threads);

    let result;
    if is_sending {
        result = fudp::sender::run(&config);
    } else if is_async {
        result = fudp::async_udp::run(&config);
    } else if is_queue {
        result = fudp::blocking_udp_queue::run(&config);
    } else if is_block {
        result = fudp::blocking_udp::run(&config);
    } else {
        println!("No mode selected");
        result = Ok(());
    }
    return result;
}