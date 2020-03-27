use clap::{App, Arg};
use std::net::SocketAddr;

mod fudp;

#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;
// mimalloc would be the better choice but there seems to be build issues on windows atm.
//static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:0";

fn main() -> std::io::Result<()> {
    let matches = App::new("udp_forwarder")
        .version("0.3.0")
        .about("Forwards UDP data from one port to multiple others")
        .author("Simon Wimmesberger")
        .arg(Arg::with_name("input-address").short("i").help("The address the server should listen on").required(false).default_value("").takes_value(true))
        .arg(Arg::with_name("output-address").short("o").help("The address the server should send data to").required(true).multiple(true).takes_value(true))
        .arg(Arg::with_name("async").short("a").required(false).help("Should the server handle udp async or not"))
        .arg(Arg::with_name("no-queue").short("q").required(false).help("Disables queueing in blocking mode"))
        .arg(Arg::with_name("sending").short("s").required(false).help("Switches to sending mode"))
        .arg(Arg::with_name("packet-size").short("p").required(false).takes_value(true).default_value("300").help("Set's the send packet size"))
        .arg(Arg::with_name("packet-per-second-check").short("c").required(false).takes_value(true).default_value("0").help("After how many packets should the system time be checked for elapsed time."))
        .get_matches();

    let mut listen_address = matches.value_of("input-address").unwrap();
    let peers: Vec<SocketAddr> = matches.values_of("output-address").unwrap().map(|address| address.parse().unwrap()).collect();
    let is_async = matches.values_of("async").is_some();
    let is_no_queue = matches.values_of("no-queue").is_some();
    let mut is_sending = matches.values_of("sending").is_some();
    let send_packet_size: usize = matches.value_of("packet-size").unwrap().parse().unwrap();
    let no_listen_address = listen_address.is_empty();
    let ppsc : i64 = matches.value_of("packet-per-second-check").unwrap().parse().unwrap();

    if no_listen_address {
        listen_address = DEFAULT_LISTEN_ADDRESS;
        is_sending = true;
    }

    let pps = &mut fudp::util::PacketsPerSecond::with_time_check_count(ppsc);

    let result;
    if is_sending {
        result = fudp::sender::run(listen_address, &peers, send_packet_size, pps);
    } else if is_async {
        result = fudp::async_udp::run(listen_address, &peers, pps);
    } else if is_no_queue {
        result = fudp::blocking_udp::run(listen_address, &peers, pps);
    } else {
        result = fudp::blocking_udp_queue::run(listen_address, &peers, pps);
    }
    return result;
}