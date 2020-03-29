use std::{thread};
use bus::{Bus, BusReader};
use crate::fudp::util;
use crate::fudp::util::{ForwardingConfiguration, ForwardingPacket, PacketsPerSecond, SocketConfigurationParameter};
use bytes::{Bytes, BytesMut};
use std::thread::JoinHandle;
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Mutex, Arc};

pub fn run(config: &ForwardingConfiguration) -> std::io::Result<()> {
    let peers = config.peers;
    let pks = &config.pks;
    let peers_count = peers.len();
    let send_thread_count = config.send_thread_count;
    let recv_thread_count = config.receive_thread_count;
    let socket_params = config.socket.parameters;

    // channel senders
    let mut bus: Bus<ForwardingPacket> = Bus::new(util::QUEUE_SIZE);
    let mut children = Vec::with_capacity(peers_count);
    for (idx, peer) in peers.iter().enumerate() {
        let peer_copy = peer.clone();
        for _x in 0..send_thread_count {
            let peer_rx = bus.add_rx();
            let child = spawn_send_worker(idx, peer_copy, peer_rx, socket_params);
            children.push(child);
        }
    }

    let socket = util::create_udp_socket_with_config(config.socket);
    println!("Binding queued blocking read socket on {}", socket.local_addr().unwrap());

    if recv_thread_count <= 1 {
        println!("Starting read worker for {}", socket.local_addr().unwrap());

        let mut bus_option = Option::from(bus);
        read_worker(&socket, &peers, None, &mut bus_option, &pks);
    } else {
        let bus_locked = Arc::new(Mutex::new(bus));
        crossbeam::scope(|scope| {
            for idx in 0..recv_thread_count {
                println!("Starting read worker for {}", socket.local_addr().unwrap());
                let _child = scope.builder().name(format!("Receive-Worker-{}", idx)).spawn(|_| {
                    read_worker(&socket, &peers, Option::from(&bus_locked), &mut None, &pks);
                }).unwrap();
            }
        }).unwrap();
    }

    return Ok(());
}

#[inline]
fn read_worker(socket: &UdpSocket, peers: &Vec<SocketAddr>, bus_locked: Option<&Arc<Mutex<Bus<ForwardingPacket>>>>, bus_unlocked: &mut Option<Bus<ForwardingPacket>>, pks: &&&mut PacketsPerSecond) {
    let peers_count = peers.len();

    // init full buffer - otherwise we can't receive anything,
    let mut buf;
    {
        let buf_backed: Vec<u8> = vec![0; util::BUFFER_SIZE];
        buf = BytesMut::from(buf_backed.as_slice());
    }

    let has_bus_locked = bus_locked.is_some();
    let has_bus_unlocked = bus_unlocked.is_some();
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
        if has_bus_locked {
            let mut send_bus = bus_locked.unwrap().lock().unwrap();
            send_bus.broadcast(ForwardingPacket::new(read_buf, peers_count));
        } else if has_bus_unlocked {
            bus_unlocked.as_mut().unwrap().broadcast(ForwardingPacket::new(read_buf, peers_count));
        }
        pks.on_packet();
    }
}

fn spawn_send_worker(idx: usize, dst:  SocketAddr, mut rx: BusReader<ForwardingPacket>, config: SocketConfigurationParameter) -> JoinHandle<()> {
    // Each thread will send its id via the channel
    let child = thread::Builder::new().name(format!("Send-Worker-{}", idx)).spawn(move || {
        send_worker(idx, dst, &mut rx, config);
    }).unwrap();
    return child;
}

#[inline]
fn send_worker(idx: usize, dst: SocketAddr, rx: &mut BusReader<ForwardingPacket>, config: SocketConfigurationParameter) {
    let peer_socket = util::create_udp_socket_with_address("127.0.0.1:0", config);
    peer_socket.connect(dst).unwrap();
    println!("Binding queued blocking send socket on {}", peer_socket.local_addr().unwrap());
    loop {
        let read_buf_packet = rx.recv().unwrap();
        if !read_buf_packet.check_sent(idx) {
            continue;
        }

        let read_buf: &[u8] = read_buf_packet.bytes();
        #[cfg(debug_assertions)]
        println!("Sending data {} to {}", read_buf.len(), dst);

        let write_result = peer_socket.send(read_buf);
        if write_result.is_err() {
            #[cfg(debug_assertions)]
            println!("Error on send {}", dst);
            continue;
        }

        if cfg!(debug_assertions) {
            let written_bytes = write_result.unwrap();
            println!("Sent data {} to {}", written_bytes, dst);
        } else {
            write_result.unwrap();
        }
    }
}