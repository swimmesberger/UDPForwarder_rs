use std::net::{SocketAddr};
use bytes::BytesMut;
use crossbeam_channel::Sender;
use crossbeam_channel::Receiver;
use std::{thread};
use crate::fudp::util;

// number of packets in queue until the reader is blocked
const QUEUE_SIZE:usize = 65550;

pub fn run(listen_address: &str, peers: &Vec<SocketAddr>) -> std::io::Result<()> {
    let socket  = util::create_udp_socket(listen_address);
    #[cfg(debug_assertions)]
    println!("Binding queued blocking socket on {}", socket.local_addr().unwrap());

    // channel senders
    let mut senders : Vec<Sender<Vec<u8>>> = Vec::with_capacity(peers.len());
    let mut children = Vec::with_capacity(peers.len());
    for peer in peers.iter() {
        let peer_socket = socket.try_clone()?;
        let peer_copy = peer.clone();

        // copy receiving channel end
        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = crossbeam_channel::bounded(QUEUE_SIZE);
        senders.push(tx);

        // Each thread will send its id via the channel
        let child = thread::spawn(move || {
            loop {
                let read_buf_vec = rx.recv().unwrap();
                let read_buf : &[u8] = read_buf_vec.as_ref();
                #[cfg(debug_assertions)]
                println!("Sending data {} to {}", read_buf.len(), peer_copy);

                let write_result =peer_socket.send_to(read_buf, peer_copy);
                if write_result.is_err() {
                    #[cfg(debug_assertions)]
                    println!("Error on send {}", peer_copy);
                    continue;
                }

                let written_bytes = write_result.unwrap();
                #[cfg(debug_assertions)]
                println!("Sent data {} to {}", written_bytes, peer_copy);
            }
        });
        children.push(child);
    }

    let mut buf = BytesMut::with_capacity(util::BUFFER_SIZE);
    // init full buffer - otherwise we can't receive anything
    unsafe {
        buf.set_len(65550);
    }

    #[cfg(debug_assertions)]
    println!("Sending to {:?}", peers);

    loop {
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

        // Redeclare `buf` as slice of the received data
        let read_buf = &buf[..read_bytes].to_vec();
        for sender in senders.iter() {
            // each channel get's is own copied buffer instance
            sender.send(read_buf.clone()).unwrap();
        }
    }
}