use crate::fudp::util;
use bytes::{BytesMut, BufMut};
use rand::Rng;
use crate::fudp::util::{ForwardingConfiguration};

pub fn run(config: &mut ForwardingConfiguration) -> std::io::Result<()> {
    let peers = config.peers;
    let listen_address = config.listen_address;
    let packet_size = config.send_packet_size;
    let pks = &mut config.pks;

    if peers.is_empty() {
        return Ok(())
    }
    let socket  = util::create_udp_socket(listen_address);
    println!("Binding sending socket on {}", socket.local_addr().unwrap());

    let is_single_target = peers.len() == 1;
    if is_single_target {
        let peer = peers.get(0).unwrap();
        socket.connect(peer).unwrap();
    }

    let mut buf = BytesMut::with_capacity(packet_size);
    let mut rng = rand::thread_rng();

    println!();

    loop {
        let random_bytes: Vec<u8> = (0..packet_size).map(|_| { rng.gen::<u8>() }).collect();
        unsafe {
            buf.set_len(0);
        }
        buf.put_slice(&random_bytes);

        if is_single_target {
            let write_result = socket.send(&buf);
            if write_result.is_err() {
                #[cfg(debug_assertions)]
                println!("Error on send");
                continue;
            }
            write_result.unwrap();
        } else {
            for peer in peers.iter() {
                let write_result = socket.send_to(&buf, peer);
                if write_result.is_err() {
                    #[cfg(debug_assertions)]
                    println!("Error on send {}", peer);
                    continue;
                }
                write_result.unwrap();
            }
        }

        pks.on_packet();
    }
}