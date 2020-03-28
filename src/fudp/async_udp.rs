use tokio;
use tokio::net::UdpSocket;
use bytes::{BytesMut};
use crate::fudp::util;
use crate::fudp::util::{ForwardingConfiguration};

#[tokio::main]
pub async fn run(config: &mut ForwardingConfiguration) -> std::io::Result<()>  {
    let peers = config.peers;
    let listen_address = config.listen_address;
    let pks = &mut config.pks;
    if peers.is_empty() {
        return Ok(())
    }

    let socket = UdpSocket::from_std(util::create_udp_socket(listen_address)).unwrap();
    println!("Binding async socket on {}", socket.local_addr().unwrap());

    let (mut receive, mut send) = socket.split();

    let mut buf = BytesMut::with_capacity(util::BUFFER_SIZE);
    // init full buffer - otherwise we can't receive anything
    unsafe {
        buf.set_len(util::BUFFER_SIZE);
    }

    println!("Sending to {:?}", peers);

    loop {
        #[cfg(debug_assertions)]
        println!();
        #[cfg(debug_assertions)]
        println!("### Reading data");

        let read_bytes_result = receive.recv(&mut buf).await;
        if read_bytes_result.is_err() {
            #[cfg(debug_assertions)]
            println!("Error on read {}", read_bytes_result.unwrap_err());
            continue;
        }
        let read_bytes = read_bytes_result.unwrap();

        if read_bytes <= 0 {
            #[cfg(debug_assertions)]
            println!("No data read");
            continue;
        }

        #[cfg(debug_assertions)]
        println!("Read data {}", read_bytes);

        // get a reference to the buffer from zero to the read bytes
        // this ensures that every peer only gets as much data as we read
        let mut read_buf = &buf[0..read_bytes];
        for peer in peers.iter() {
            #[cfg(debug_assertions)]
            println!("Sending data {} to {}", read_buf.len(), peer);

            // we have to wait here because in tokio we have no possibility to somehow clone the sender
            // so we can only send one packet at a time
            let written_bytes_result = send.send_to(&mut read_buf, peer).await;
            if written_bytes_result.is_err() {
                #[cfg(debug_assertions)]
                println!("Error on send {}", peer);
                continue;
            }
            if cfg!(debug_assertions) {
                let written_bytes = written_bytes_result.unwrap();
                println!("Sent data {} to {}", written_bytes, peer);
            } else{
                written_bytes_result.unwrap();
            }
        }

        pks.on_packet();
    }
}
