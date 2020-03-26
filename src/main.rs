use clap::{App, Arg};
use std::net::SocketAddr;

mod fudp;

fn main() -> std::io::Result<()> {
    let matches = App::new("udp_forwarder")
        .version("1.0")
        .about("Forwards UDP data from one port to multiple others")
        .author("Simon Wimmesberger")
        .arg(Arg::with_name("input-address").short("i").help("The address the server should listen on").required(true).takes_value(true))
        .arg(Arg::with_name("output-address").short("o").help("The address the server should send data to").required(true).multiple(true).takes_value(true))
        .arg(Arg::with_name("async").short("a").help("Should the server handle fudp async or not"))
        .arg(Arg::with_name("theading").short("t").help("Should the server use threads when in blocking mode"))
        .get_matches();

    let listen_address = matches.value_of("input-address").unwrap();
    let peers: Vec<SocketAddr> = matches.values_of("output-address").unwrap().map(|address| address.parse().unwrap()).collect();
    let is_async = matches.values_of("async").is_some();
    let is_threading = matches.values_of("theading").is_some();

    let result;
    if is_async {
        result = fudp::async_udp::run(listen_address, &peers);
    } else if is_threading {
        result = fudp::blocking_udp_queue::run(listen_address, &peers);
    } else {
        result = fudp::blocking_udp::run(listen_address, &peers);
    }
    return result;
}