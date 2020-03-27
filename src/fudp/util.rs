use std::mem;
use std::net::UdpSocket;
use net2::{UdpBuilder, UdpSocketExt};
use coarsetime::{Instant, Updater};
use num_format::{ToFormattedString, Locale};
use std::io::Write;

pub const BUFFER_SIZE: usize = 65550;

pub struct PacketsPerSecond {
    packet_count: u64,
    timed_packet_count: u32,
    last_time: Instant,
    locale: Locale,
    time_check_count: i64,
    _updater: Updater
}

impl PacketsPerSecond {
    pub fn with_time_check_count(time_check_count: i64) -> PacketsPerSecond {
        return PacketsPerSecond {
            packet_count: 0,
            timed_packet_count: 0,
            last_time: Instant::now(),
            locale: Locale::de,
            time_check_count,
            _updater: Updater::new(500).start().unwrap()
        }
    }

    #[inline]
    pub fn on_packet(&mut self) {
        if self.time_check_count < 0 {
            return;
        }

        self.packet_count = self.packet_count + 1;
        if self.time_check_count == 0 {
            self.check_time();
            return;
        }

        self.timed_packet_count = self.timed_packet_count + 1;
        if self.timed_packet_count > self.time_check_count as u32 {
            self.check_time();
            self.timed_packet_count = 0;
        }
    }

    #[inline]
    fn check_time(&mut self) {
        let new_time = Instant::recent();
        if new_time.duration_since(self.last_time).as_secs() > 1 {
            let formatted_packet_count = self.packet_count.to_formatted_string(&self.locale);
            print!("\r{} packets per second", formatted_packet_count);
            std::io::stdout().flush().unwrap();
            self.packet_count = 0;
            self.last_time = new_time;
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