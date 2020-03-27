use std::mem;
use std::net::UdpSocket;
use net2::{UdpBuilder, UdpSocketExt};
use atomic_counter::{RelaxedCounter, AtomicCounter};
use std::time::Instant;
use std::sync::{Mutex, Arc};
use num_format::{ToFormattedString, Locale};
use std::ops::Sub;
use std::io::Write;

pub const BUFFER_SIZE: usize = 65550;
const TIME_PACKET_CHECK_COUNT: usize = 50_000;

pub(crate) struct PacketsPerSecond {
    packet_count: RelaxedCounter,
    timed_packet_count: RelaxedCounter,
    last_time: Arc<Mutex<Instant>>,
    locale: Locale,
}

impl PacketsPerSecond {
    pub fn new() -> PacketsPerSecond {
        PacketsPerSecond {
            packet_count: RelaxedCounter::new(0),
            timed_packet_count: RelaxedCounter::new(0),
            last_time: Arc::new(Mutex::new(Instant::now())),
            locale: Locale::de,
        }
    }

    pub fn on_packet(&mut self) {
        let prev_packet_count = self.packet_count.inc();
        let prev_time_packet_count = self.timed_packet_count.inc();

        if prev_time_packet_count > TIME_PACKET_CHECK_COUNT {
            let mut last_time_mut = self.last_time.lock().unwrap();
            let last_time = last_time_mut.to_owned();

            let new_time = Instant::now();
            if new_time.sub(last_time).as_secs() > 1 {
                print!("\r{} packets per second", prev_packet_count.to_formatted_string(&self.locale));
                std::io::stdout().flush().unwrap();
                self.packet_count.reset();
                *last_time_mut = new_time;
            }
            self.timed_packet_count.reset();
        }
    }
}

pub fn create_udp_socket(listen_address: &str) -> UdpSocket {
    let socket = UdpBuilder::new_v4().unwrap().bind(listen_address).unwrap();
    socket.set_recv_buffer_size(BUFFER_SIZE).unwrap();

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