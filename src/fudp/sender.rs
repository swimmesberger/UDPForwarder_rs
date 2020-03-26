use std::net::SocketAddr;
use crate::fudp::util;
use bytes::BytesMut;
use std::time::Instant;
use num_format::{Locale, ToFormattedString};

const TIME_PACKET_CHECK_COUNT:usize = 50_000;

pub fn run(listen_address: &str, peer: SocketAddr) -> std::io::Result<()> {
    let socket  = util::create_udp_socket(listen_address);
    socket.connect(peer).unwrap();

    #[cfg(debug_assertions)]
    println!("Binding sending socket on {}", socket.local_addr().unwrap());

    let mut buf = BytesMut::with_capacity(300);
    // init buffer with empty bytes
    unsafe {
        buf.set_len(300);
    }

    println!();

    let locale = Locale::de;
    let mut now = Instant::now();
    let mut packet_count = 0;
    // we try to check the time only after some packets
    let mut time_packet_count = 0;
    loop {
        let write_result = socket.send(&buf);
        if write_result.is_err() {
            #[cfg(debug_assertions)]
            println!("Error on send {}", peer);
            continue;
        }
        write_result.unwrap();

        packet_count = packet_count+1;
        time_packet_count = time_packet_count+1;

        if time_packet_count > TIME_PACKET_CHECK_COUNT {
            let new_time = Instant::now();
            if (new_time-now).as_secs() > 1 {
                println!("{} packets per second", packet_count.to_formatted_string(&locale));
                packet_count = 0;
                now = new_time;
            }
            time_packet_count = 0;
        }
    }
}