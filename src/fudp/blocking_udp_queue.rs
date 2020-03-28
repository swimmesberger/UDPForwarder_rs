use std::{thread};
use bus::Bus;
use crate::fudp::util;
use crate::fudp::util::{ForwardingConfiguration};
use bytes::{Bytes, BytesMut};

// number of packets in queue until the reader is blocked
const QUEUE_SIZE: usize = 65550;

pub fn run(config: &mut ForwardingConfiguration) -> std::io::Result<()> {
    let peers = config.peers;
    let listen_address = config.listen_address;
    let pks = &mut config.pks;

    let socket = util::create_udp_socket(listen_address);
    socket.set_nonblocking(false).unwrap();

    println!("Binding queued blocking read socket on {}", socket.local_addr().unwrap());

    // channel senders
    let mut bus: Bus<Bytes> = Bus::new(QUEUE_SIZE);
    let mut children = Vec::with_capacity(peers.len());
    for peer in peers.iter() {
        let peer_copy = peer.clone();
        let mut peer_rx = bus.add_rx();
        // Each thread will send its id via the channel
        let child = thread::spawn(move || {
            let peer_address = "127.0.0.1:0";
            let peer_socket = util::create_udp_socket(peer_address);
            peer_socket.connect(peer_copy).unwrap();
            println!("Binding queued blocking send socket on {}", peer_socket.local_addr().unwrap());
            loop {
                let read_buf_vec = peer_rx.recv().unwrap();
                let read_buf: &[u8] = read_buf_vec.as_ref();
                #[cfg(debug_assertions)]
                println!("Sending data {} to {}", read_buf.len(), peer_copy);

                let write_result = peer_socket.send(read_buf);
                if write_result.is_err() {
                    #[cfg(debug_assertions)]
                    println!("Error on send {}", peer_copy);
                    continue;
                }

                if cfg!(debug_assertions) {
                    let written_bytes = write_result.unwrap();
                    println!("Sent data {} to {}", written_bytes, peer_copy);
                } else {
                    write_result.unwrap();
                }
            }
        });
        children.push(child);
    }

    // init full buffer - otherwise we can't receive anything,
    let mut buf;
    {
        let buf_backed: Vec<u8> = vec![0; util::BUFFER_SIZE];
        buf = BytesMut::from(buf_backed.as_slice());
    }

    println!("Sending to {:?}", peers);

    loop {
        let read_result = socket.recv_from(buf.as_mut());
        if read_result.is_err() {
            #[cfg(debug_assertions)]
            println!("Error on read {}", read_result.unwrap_err());
            continue;
        }

        #[allow(unused_variables)]
        let (read_bytes, src) = read_result.unwrap();
        if read_bytes <= 0 {
            #[cfg(debug_assertions)]
            println!("No data read");
            continue;
        }

        #[cfg(debug_assertions)]
        println!("Read data {} from {}", read_bytes, src);

        // Redeclare `buf` as slice of the received data
        let read_buf: Bytes = BytesMut::from(&buf[..read_bytes]).freeze();
        bus.broadcast(read_buf);
        pks.on_packet();
    }
}