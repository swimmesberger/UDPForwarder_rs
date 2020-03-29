use bytes::BytesMut;
use crate::fudp::util;
use crate::fudp::util::{ForwardingConfiguration, PacketsPerSecond};
use std::net::{UdpSocket};
use std::sync::Arc;
use parking_lot::Mutex;

pub fn run(config: &ForwardingConfiguration) -> std::io::Result<()> {
    let peers = config.peers;
    let pks = &config.pks;
    let thread_count = config.send_thread_count;
    let socket_params = config.socket.parameters;

    let socket  = util::create_udp_socket_with_config(config.socket);
    println!("Binding blocking receive socket on {}", socket.local_addr().unwrap());

    let mut send_sockets : Vec<Vec<Arc<UdpSocket>>> = Vec::with_capacity(peers.len());
    for _idx in 0..thread_count {
        let mut thread_send_sockets : Vec<Arc<UdpSocket>> = Vec::with_capacity(peers.len());
        for peer in peers.iter() {
            let peer_socket = util::create_udp_socket_with_address("127.0.0.1:0", socket_params);
            peer_socket.connect(peer).unwrap();
            println!("Binding blocking send socket on {}", peer_socket.local_addr().unwrap());
            thread_send_sockets.push(Arc::new(peer_socket));
        }
        send_sockets.push(thread_send_sockets);
    }

    println!("Sending to {:?}", peers);

    if thread_count == 1 {
        let copy_send_sockets = send_sockets.get(0).unwrap().clone();
        println!("Starting read worker for {}", socket.local_addr().unwrap());
        read_worker(&socket, copy_send_sockets, &pks, &mut None);
    } else {
        let recv_mutex = Arc::new(Mutex::new(0));
        crossbeam::scope(|scope| {
            for idx in 0..thread_count {
                let mutex_clone = recv_mutex.clone();
                let copy_send_sockets = send_sockets.get(idx).unwrap().clone();
                println!("Starting read worker for {}", socket.local_addr().unwrap());
                let _child = scope.builder().name(format!("Receive-Worker-{}", idx)).spawn(|_| {
                    let mut mutex_option = Option::from(mutex_clone);
                    read_worker(&socket, copy_send_sockets, &pks, &mut mutex_option);
                }).unwrap();
            }
        }).unwrap();
    }

    return Ok(());
}

#[inline]
fn read_worker(read_socket: &UdpSocket, mut write_sockets: Vec<Arc<UdpSocket>>, pks: &&&mut PacketsPerSecond, recv_mutex: &mut Option<Arc<Mutex<i32>>>) {
    // init full buffer - otherwise we can't receive anything,
    let mut buf;
    {
        let buf_backed: Vec<u8> = vec![0; util::BUFFER_SIZE];
        buf = BytesMut::from(buf_backed.as_slice());
    }

    let has_mutex = recv_mutex.is_some();
    loop {
        #[cfg(debug_assertions)]
        println!();
        #[cfg(debug_assertions)]
        println!("### Reading data");

        let read_result;
        if has_mutex {
            // lock this scope because when multiple threads block in recv simultaneously WSAEINPROGRESS error is thrown to prevent that we use a mutex
            let _lock_scope = recv_mutex.as_mut().unwrap().lock();
            read_result = read_socket.recv(&mut buf);
        } else {
            read_result = read_socket.recv(&mut buf);
        }

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

        for (_idx, write_socket) in write_sockets.iter_mut().enumerate() {
            #[cfg(debug_assertions)]
            println!("Sending data {} to {}", read_buf.len(), write_socket.local_addr().unwrap());

            let write_result = write_socket.send(read_buf);
            if write_result.is_err() {
                #[cfg(debug_assertions)]
                println!("Error on send {}", write_socket.local_addr().unwrap());
                continue;
            }

            if cfg!(debug_assertions) {
                let written_bytes = write_result.unwrap();
                println!("Sent data {} to {}", written_bytes, write_socket.local_addr().unwrap());
            } else{
                write_result.unwrap();
            }
        }

        pks.on_packet();
    }
}