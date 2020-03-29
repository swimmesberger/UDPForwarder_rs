use tokio;
use tokio::net::UdpSocket;
use tokio::net::udp::RecvHalf;
use bytes::{BytesMut, Bytes};
use crate::fudp::util;
use crate::fudp::util::{ForwardingConfiguration, ForwardingPacket, PacketsPerSecond};
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use futures::future::join_all;

#[tokio::main]
pub async fn run(config: &ForwardingConfiguration) -> std::io::Result<()> {
    let peers = config.peers;
    let pks = &config.pks;
    let peers_count = peers.len();
    let send_thread_count = config.send_thread_count;
    let socket_params = config.socket.parameters;

    if peers.is_empty() {
        return Ok(())
    }

    let socket = UdpSocket::from_std(util::create_udp_socket_with_config(config.socket)).unwrap();
    let socket_addr = socket.local_addr().unwrap();
    println!("Binding async receive socket on {}", socket_addr);
    let (mut sock_rx, _sock_tx)  = socket.split();

    let (mut channel_tx, _channel_rx) = broadcast::channel(util::QUEUE_SIZE);
    let mut children = Vec::with_capacity(peers_count);
    for (idx, peer) in peers.iter().enumerate() {
        for _x in 0..send_thread_count {
            let mut peer_rx = channel_tx.subscribe();
            let peer_socket = UdpSocket::from_std(util::create_udp_socket_with_address("127.0.0.1:0", socket_params)).unwrap();
            peer_socket.connect(peer).await?;
            let peer_socket_addr = peer_socket.local_addr().unwrap();
            println!("Binding async send socket on {}", peer_socket_addr);

            let (_peer_sock_rx, mut peer_sock_tx)  = peer_socket.split();
            let child = tokio::spawn(async move {
                loop {
                    let read_buf_packet_result = peer_rx.recv().await;
                    if read_buf_packet_result.is_err() {
                        #[cfg(debug_assertions)]
                        println!("Error on channel receive {}", peer_socket_addr);
                        continue;
                    }

                    let read_buf_packet: ForwardingPacket = read_buf_packet_result.unwrap();
                    if !read_buf_packet.check_sent(idx) {
                        continue;
                    }
                    let read_buf: &[u8] = read_buf_packet.bytes();
                    #[cfg(debug_assertions)]
                    println!("Sending data {} to {}", read_buf.len(), peer_socket_addr);

                    let write_result = peer_sock_tx.send(read_buf).await;
                    if write_result.is_err() {
                        #[cfg(debug_assertions)]
                        println!("Error on send {}", peer_socket_addr);
                        continue;
                    }

                    if cfg!(debug_assertions) {
                        let written_bytes = write_result.unwrap();
                        println!("Sent data {} to {}", written_bytes, peer_socket_addr);
                    } else {
                        write_result.unwrap();
                    }
                }
            });
            children.push(child);
        }
    }

    println!("Starting read worker for {}", socket_addr);

    read_worker(&mut sock_rx, &peers, &mut channel_tx, &pks).await;

    join_all(children).await;

    return Ok(());
}

#[inline]
async fn read_worker(socket: &mut RecvHalf, peers: &Vec<SocketAddr>, sender: &mut Sender<ForwardingPacket>, pks: &&&mut PacketsPerSecond) {
    let peers_count = peers.len();

    // init full buffer - otherwise we can't receive anything,
    let mut buf;
    {
        let buf_backed: Vec<u8> = vec![0; util::BUFFER_SIZE];
        buf = BytesMut::from(buf_backed.as_slice());
    }

    println!("Sending to {:?}", peers);

    loop {
        #[cfg(debug_assertions)]
        println!();
        #[cfg(debug_assertions)]
        println!("### Reading data");

        let read_bytes_result = socket.recv(&mut buf).await;
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

        let read_buf: Bytes = BytesMut::from(&buf[..read_bytes]).freeze();
        let packet = ForwardingPacket::new(read_buf, peers_count);
        let _sub_count= match sender.send(packet)   {
            Ok(sub_count) => sub_count,
            Err(_e) => 0,
        };

        pks.on_packet();
    }
}