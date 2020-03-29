use std::mem;
use std::net::{UdpSocket, SocketAddr};
use net2::{UdpBuilder, UdpSocketExt};
use num_format::{ToFormattedString, Locale};
use std::io::{Write};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time;
use atomic_counter::{AtomicCounter, RelaxedCounter};
use std::sync::{Arc};
use bytes::Bytes;

pub const BUFFER_SIZE: usize = 65550;
pub const QUEUE_SIZE: usize = 65550;

#[derive(Clone)]
pub struct ForwardingPacket {
    data: Bytes,
    is_sent: Vec<Arc<AtomicBool>>
}

impl ForwardingPacket {
    pub fn new(data: Bytes, capacity: usize) -> ForwardingPacket {
        let mut is_sent: Vec<Arc<AtomicBool>> = Vec::with_capacity(capacity);
        for _x in 0..capacity {
            is_sent.push(Arc::new(AtomicBool::new(false)));
        }
        ForwardingPacket {
            data,
            is_sent
        }
    }

    pub fn check_sent(&self, idx: usize) -> bool {
        let is_send_arc = &self.is_sent[idx];
        return is_send_arc.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok();
    }

    pub fn bytes(&self) -> &Bytes {
        return &self.data;
    }
}

#[derive(Copy, Clone)]
pub struct SocketConfigurationParameter {
    pub(crate) so_receive_buffer: usize,
    pub(crate) so_send_buffer: usize,
}

impl SocketConfigurationParameter {
    pub fn new(so_receive_buffer: usize, so_send_buffer: usize) -> SocketConfigurationParameter {
        SocketConfigurationParameter {
            so_receive_buffer,
            so_send_buffer
        }
    }
}

#[derive(Copy, Clone)]
pub struct SocketConfiguration<'a> {
    pub(crate) listen_address: &'a str,
    pub(crate) parameters: SocketConfigurationParameter
}

impl<'a> SocketConfiguration<'a> {
    pub fn new(listen_address: &'a str, so_receive_buffer: usize, so_send_buffer: usize) -> SocketConfiguration<'a> {
        SocketConfiguration {
            listen_address,
            parameters: SocketConfigurationParameter::new(so_receive_buffer, so_send_buffer)
        }
    }
}


pub struct ForwardingConfiguration<'a> {
    pub(crate) socket: &'a SocketConfiguration<'a>,
    pub(crate) peers: &'a Vec<SocketAddr>,
    pub(crate) pks: &'a mut PacketsPerSecond,
    pub(crate) send_packet_size: usize,
    pub(crate) send_thread_count: usize,
    pub(crate) receive_thread_count: usize
}

impl<'a> ForwardingConfiguration<'a> {
    pub fn new(socket: &'a SocketConfiguration, peers: &'a Vec<SocketAddr>, pks: &'a mut PacketsPerSecond, send_packet_size: usize,
               send_thread_count: usize, receive_thread_count: usize) -> ForwardingConfiguration<'a> {
        ForwardingConfiguration {
            socket,
            peers,
            pks,
            send_packet_size,
            send_thread_count,
            receive_thread_count
        }
    }
}

pub struct PacketsPerSecond {
    period: time::Duration,
    running: Arc<AtomicBool>,
    th: Option<thread::JoinHandle<()>>,
    packet_count: Arc<RelaxedCounter>,
    locale: Locale
}

impl PacketsPerSecond {
    /// Creates a new `Updater` with the specified update period, in milliseconds.
    pub fn new(period_millis: u64) -> PacketsPerSecond {
        PacketsPerSecond {
            period: time::Duration::from_millis(period_millis),
            running: Arc::new(AtomicBool::new(false)),
            th: None,
            packet_count: Arc::new(RelaxedCounter::new(0)),
            locale: Locale::de
        }
    }

    #[inline]
    pub fn on_packet(&self) {
        self.packet_count.inc();
    }

    /// Spawns a background task to update packets periodically
    pub fn start(&mut self) -> Result<(), io::Error> {
        let period = self.period;

        let packet_count = self.packet_count.clone();
        let running = self.running.clone();
        running.store(true, Ordering::Relaxed);

        let locale = self.locale.clone();

        let th: thread::JoinHandle<()> = thread::Builder::new()
            .name("coarsetime".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    thread::sleep(period);
                    let packet_count_val = packet_count.reset();
                    let formatted_packet_count = packet_count_val.to_formatted_string(&locale);
                    print!("\r{} packets per second", formatted_packet_count);
                    std::io::stdout().flush().unwrap();
                }
            })?;
        self.th = Some(th);
        Ok(())
    }

    /// Stops the periodic updates
    #[allow(dead_code)]
    pub fn stop(mut self) -> Result<(), io::Error> {
        self.running.store(false, Ordering::Relaxed);
        self.th
            .take()
            .expect("updater is not running")
            .join()
            .map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "failed to properly stop the updater")
            })
    }
}

pub fn create_udp_socket_with_config(config: &SocketConfiguration) -> UdpSocket {
    return create_udp_socket_with_address(config.listen_address, config.parameters);
}

pub fn create_udp_socket_with_address(listen_address: &str, config: SocketConfigurationParameter) -> UdpSocket {
    return create_udp_socket(listen_address, config.so_receive_buffer, config.so_send_buffer);
}

pub fn create_udp_socket(listen_address: &str, so_receive_buffer: usize, so_send_buffer: usize) -> UdpSocket {
    let socket = UdpBuilder::new_v4().unwrap().bind(listen_address).unwrap();
    if so_receive_buffer > 0 {
        socket.set_recv_buffer_size(so_receive_buffer).unwrap();
    }
    if so_send_buffer > 0 {
        socket.set_send_buffer_size(so_send_buffer).unwrap();
    }

    if cfg!(windows) {
        // on windows a connection reset error is thrown on receive when we send to a unbound port
        // with this call we disable that behaviour
        disable_connreset(&socket).unwrap();
    }

    return socket;
}


#[cfg(windows)]
pub fn disable_connreset(socket: &UdpSocket) -> std::io::Result<()> {
    use winapi::um::winsock2::WSAIoctl;
    use winapi::um::winsock2::SOCKET;
    use winapi::um::mswsock::SIO_UDP_CONNRESET;
    use winapi::shared::minwindef::DWORD;
    use winapi::shared::minwindef::BOOL;
    use winapi::shared::minwindef::LPVOID;
    use winapi::shared::minwindef::LPDWORD;
    use winapi::shared::minwindef::FALSE;
    use std::os::windows::io::AsRawSocket;
    use std::ptr::null_mut;

    unsafe {
        let mut bytes_returned: DWORD = 0;
        let mut enable: BOOL = FALSE;
        let wsa_result = WSAIoctl(
            socket.as_raw_socket() as SOCKET,
            SIO_UDP_CONNRESET,
            &mut enable as *mut _ as LPVOID,
            mem::size_of_val(&enable) as DWORD,
            null_mut(),
            0,
            &mut bytes_returned as *mut _ as LPDWORD,
            null_mut(),
            None);
        if wsa_result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}