use std::net::{SocketAddr};
use bytes::BytesMut;
use crate::fudp::util;

pub fn run(listen_address: &str, peers: &Vec<SocketAddr>) -> std::io::Result<()> {
    #[cfg(debug_assertions)]
    println!("Binding blocking socket on {}", listen_address);
    let socket  = util::create_udp_socket(listen_address);

    let mut buf = BytesMut::with_capacity(65550);
    // init full buffer - otherwise we can't receive anything
    unsafe {
        buf.set_len(65550);
    }

    loop {
        #[cfg(debug_assertions)]
        println!();
        #[cfg(debug_assertions)]
        println!("### Reading data");

        let read_result = socket.recv(&mut buf);
        if read_result.is_err() {
            #[cfg(debug_assertions)]
            println!("Error on read {}", read_result.unwrap_err());
            continue;
        }

        let read_bytes = read_result.unwrap();
        if read_bytes <= 0 {
            #[cfg(debug_assertions)]
            println!("No data read");
            continue;
        }

        #[cfg(debug_assertions)]
        println!("Read data {}", read_bytes);

        // Redeclare `buf` as slice of the received data and send reverse data back to origin.
        let read_buf = &buf[..read_bytes];

        for peer in peers.iter() {
            #[cfg(debug_assertions)]
            println!("Sending data {} to {}", read_buf.len(), peer);

            let write_result = socket.send_to(read_buf, &peer);
            if write_result.is_err() {
                #[cfg(debug_assertions)]
                println!("Error on send {}", peer);
                continue;
            }

            let written_bytes = write_result.unwrap();
            #[cfg(debug_assertions)]
            println!("Sent data {} to {}", written_bytes, peer);
        }
    }
}