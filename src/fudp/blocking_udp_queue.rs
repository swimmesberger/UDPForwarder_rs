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
    let mut bus_config = BusConfig::new(send_thread_count);

    let mut children = Vec::with_capacity(peers_count);
    for (idx, peer) in peers.iter().enumerate() {
        let peer_copy = peer.clone();
        for _x in 0..send_thread_count {
            let (peer_rx, peer_basic_rx) = bus_config.add_rx();

            let child = spawn_send_worker(idx, peer_copy, peer_rx, peer_basic_rx,socket_params);
            children.push(child);
        }
    }

    let socket = util::create_udp_socket_with_config(config.socket);
    println!("Binding queued blocking read socket on {}", socket.local_addr().unwrap());

    if recv_thread_count <= 1 {
        println!("Starting read worker for {}", socket.local_addr().unwrap());

        let mut bus_locked_config = BusLockedConfig::new(None, &mut bus_config.bus_option, None,
                                                         &mut bus_config.bus_basic_option);
        read_worker(&socket, &peers,  &mut bus_locked_config, &pks);
    } else {
        let bus_locked;
        let bus_basic_locked;
        if bus_config.bus_option.is_some() {
            bus_locked = Option::from(Arc::new(Mutex::new(bus_config.bus_option.unwrap())));
            bus_basic_locked = None;
        } else {
            bus_basic_locked = Option::from(Arc::new(Mutex::new(bus_config.bus_basic_option.unwrap())));
            bus_locked = None;
        }
        crossbeam::scope(|scope| {
            for idx in 0..recv_thread_count {
                println!("Starting read worker for {}", socket.local_addr().unwrap());
                let _child = scope.builder().name(format!("Receive-Worker-{}", idx)).spawn(|_| {
                    let bus_locked_child = bus_locked.as_ref();
                    let bus_basic_locked= bus_basic_locked.as_ref();
                    let mut bus_unlocked = None;
                    let mut bus_basic_unlocked = None;
                    let mut bus_locked_config = BusLockedConfig::new(bus_locked_child, &mut bus_unlocked,
                                                                     bus_basic_locked, &mut bus_basic_unlocked);
                    read_worker(&socket, &peers, &mut bus_locked_config, &pks);
                }).unwrap();
            }
        }).unwrap();
    }

    return Ok(());
}

#[inline]
fn read_worker(socket: &UdpSocket, peers: &Vec<SocketAddr>, bus_config: &mut BusLockedConfig, pks: &&&mut PacketsPerSecond) {
    let peers_count = peers.len();

    // init full buffer - otherwise we can't receive anything,
    let mut buf;
    {
        let buf_backed: Vec<u8> = vec![0; util::BUFFER_SIZE];
        buf = BytesMut::from(buf_backed.as_slice());
    }

    let has_bus_locked = bus_config.bus_locked.is_some();
    let has_bus_unlocked = bus_config.bus_unlocked.is_some();
    let has_bus_basic_locked = bus_config.bus_basic_locked.is_some();
    let has_bus_basic_unlocked = bus_config.bus_basic_unlocked.is_some();
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
        if has_bus_basic_locked {
            let mut send_bus = bus_config.bus_basic_locked.unwrap().lock().unwrap();
            send_bus.broadcast(read_buf);
        } else if has_bus_basic_unlocked {
            bus_config.bus_basic_unlocked.as_mut().unwrap().broadcast(read_buf);
        } else if has_bus_locked {
            let mut send_bus = bus_config.bus_locked.unwrap().lock().unwrap();
            send_bus.broadcast(ForwardingPacket::new(read_buf, peers_count));
        } else if has_bus_unlocked {
            bus_config.bus_unlocked.as_mut().unwrap().broadcast(ForwardingPacket::new(read_buf, peers_count));
        }
        pks.on_packet();
    }
}

fn spawn_send_worker(idx: usize, dst:  SocketAddr, mut rx: Option<BusReader<ForwardingPacket>>, mut rx_basic: Option<BusReader<Bytes>>, config: SocketConfigurationParameter) -> JoinHandle<()> {
    // Each thread will send its id via the channel
    let child = thread::Builder::new().name(format!("Send-Worker-{}", idx)).spawn(move || {
        send_worker(idx, dst, &mut rx,  &mut rx_basic, config);
    }).unwrap();
    return child;
}

#[inline]
fn send_worker(idx: usize, dst: SocketAddr, rx: &mut Option<BusReader<ForwardingPacket>>, rx_basic: &mut Option<BusReader<Bytes>>, config: SocketConfigurationParameter) {
    let peer_socket = util::create_udp_socket_with_address("127.0.0.1:0", config);
    peer_socket.connect(dst).unwrap();
    println!("Binding queued blocking send socket on {}", peer_socket.local_addr().unwrap());

    let has_packet_bus = rx.is_some();
    loop {

        let write_result;
        if has_packet_bus {
            let read_buf_packet = rx.as_mut().unwrap().recv().unwrap();
            if !read_buf_packet.check_sent(idx) {
                continue;
            }
            let read_buf: &[u8] = read_buf_packet.bytes();
            #[cfg(debug_assertions)]
            println!("Sending data {} to {}", read_buf.len(), dst);

            write_result = peer_socket.send(read_buf);
        } else {
            let read_buf_data = rx_basic.as_mut().unwrap().recv().unwrap();
            let read_buf: &[u8] = &read_buf_data;
            #[cfg(debug_assertions)]
            println!("Sending data {} to {}", read_buf.len(), dst);

            write_result = peer_socket.send(read_buf);
        }

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

pub struct BusConfig {
    bus_basic_option: Option<Bus<Bytes>>,
    bus_option: Option<Bus<ForwardingPacket>>,
}

impl BusConfig {
    pub fn new(send_thread_count: usize) -> BusConfig {
        let bus_basic_option: Option<Bus<Bytes>>;
        let bus_option: Option<Bus<ForwardingPacket>>;
        if send_thread_count <= 1 {
            bus_option = None;
            bus_basic_option = Option::from(Bus::new(util::QUEUE_SIZE));
        } else {
            bus_option = Option::from(Bus::new(util::QUEUE_SIZE));
            bus_basic_option = None;
        }
        return BusConfig {
            bus_basic_option,
            bus_option
        }
    }

    pub fn add_rx(&mut self) -> (Option<BusReader<ForwardingPacket>>, Option<BusReader<Bytes>>) {
        let peer_rx;
        let peer_basic_rx;
        if self.bus_basic_option.is_some() {
            peer_basic_rx = Option::from(self.bus_basic_option.as_mut().unwrap().add_rx());
            peer_rx = None;
        } else {
            peer_rx = Option::from(self.bus_option.as_mut().unwrap().add_rx());
            peer_basic_rx = None;
        }
        return (peer_rx, peer_basic_rx);
    }
}

pub struct BusLockedConfig<'a> {
    bus_locked: Option<&'a Arc<Mutex< Bus<ForwardingPacket>>>>,
    bus_unlocked: &'a mut Option<Bus<ForwardingPacket>>,
    pub(crate) bus_basic_locked: Option<&'a Arc<Mutex<Bus<Bytes>>>>,
    pub(crate) bus_basic_unlocked: &'a mut Option<Bus<Bytes>>
}

impl<'a> BusLockedConfig<'a> {
    pub fn new(bus_locked: Option<&'a Arc<Mutex< Bus<ForwardingPacket>>>>,
               bus_unlocked: &'a mut Option<Bus<ForwardingPacket>>,
               bus_basic_locked: Option<&'a Arc<Mutex<Bus<Bytes>>>>,
               bus_basic_unlocked: &'a mut Option<Bus<Bytes>>) -> BusLockedConfig<'a> {
        return BusLockedConfig {
            bus_locked,
            bus_unlocked,
            bus_basic_locked,
            bus_basic_unlocked
        }
    }
}