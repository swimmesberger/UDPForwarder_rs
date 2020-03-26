use std::mem;
use std::net::UdpSocket;
use net2::{UdpBuilder, UdpSocketExt};

pub const BUFFER_SIZE: usize = 65550;

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