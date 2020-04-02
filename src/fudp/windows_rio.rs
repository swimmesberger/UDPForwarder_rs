use crate::fudp::util::{ForwardingConfiguration, PacketsPerSecond};
use winapi::um::winsock2::{WSAStartup, WSASocketW, WSADATA, SOCKET, SOCK_DGRAM, WSA_FLAG_REGISTERED_IO, bind, INVALID_SOCKET, WSAIoctl, closesocket};
use core::{mem, cmp};
use winapi::shared::ws2def::{AF_INET, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER};
use winapi::shared::ws2def::IPPROTO_UDP;
use winapi::shared::ws2def::SOCKADDR;
use std::ptr::null_mut;
use std::net::SocketAddr;
use winapi::ctypes::c_int;
use winapi::um::handleapi::{SetHandleInformation, INVALID_HANDLE_VALUE};
use winapi::um::winbase::{HANDLE_FLAG_INHERIT, INFINITE};
use winapi::shared::ntdef::{HANDLE, PVOID};
use winapi::shared::minwindef::{DWORD, LPVOID, LPDWORD, ULONG};
use winapi::um::mswsock::{WSAID_MULTIPLE_RIO, RIO_EXTENSION_FUNCTION_TABLE, RIO_NOTIFICATION_COMPLETION, RIO_NOTIFICATION_COMPLETION_u, RIO_NOTIFICATION_COMPLETION_u_s2, RIO_IOCP_COMPLETION};
use winapi::shared::guiddef::GUID;
use winapi::um::ioapiset::{CreateIoCompletionPort, GetQueuedCompletionStatus, PostQueuedCompletionStatus};
use winapi::um::minwinbase::{OVERLAPPED, LPOVERLAPPED, CRITICAL_SECTION, LPCRITICAL_SECTION};
use winapi::shared::mswsockdef::{RIO_CQ, RIO_INVALID_CQ, RIO_INVALID_BUFFERID, RIO_BUF, PRIO_BUF, RIO_BUFFERID, RIORESULT, PRIORESULT, RIO_CORRUPT_CQ, RIO_MSG_DEFER, RIO_MSG_COMMIT_ONLY};
use winapi::um::sysinfoapi::GetSystemInfo;
use winapi::um::sysinfoapi::SYSTEM_INFO;
use winapi::vc::limits::UINT_MAX;
use winapi::um::processthreadsapi::GetCurrentProcess;
use winapi::um::winnt::{MEM_COMMIT, MEM_RESERVE, PAGE_READWRITE, PCHAR, ULONGLONG};
use winapi::um::memoryapi::VirtualAllocEx;
use winapi::shared::basetsd::{SIZE_T, PULONG_PTR, ULONG_PTR};
use winapi::um::synchapi::{EnterCriticalSection, OpenEventA};
use winapi::um::synchapi::LeaveCriticalSection;
use winapi::um::synchapi::InitializeCriticalSectionAndSpinCount;
use winapi::um::synchapi::DeleteCriticalSection;
use winapi::shared::winerror::ERROR_SUCCESS;
use arrayvec::ArrayVec;
use std::thread::{yield_now, sleep};
use std::time::Duration;
use winapi::_core::ffi::c_void;
use std::sync::Arc;
use parking_lot::Mutex;
use std::ops::DerefMut;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use winapi::_core::ptr::null;

const RIO_PENDING_RECVS: u32 = 100_000;
const RIO_PENDING_SENDS: u32 = 100_000;
const RECV_BUFFER_SIZE: u32 = 1024;
const SEND_BUFFER_SIZE: u32 = 1024;
// maximum size of a native SOCKET_ADDR
const ADDR_BUFFER_SIZE: u32 = 64;
const NUM_IOCP_THREADS: u32 = 4;
const RIO_MAX_RESULTS: usize = 1024;
const RIO_SEND_PACKET_BUFFER: usize = 50_000;
const MUTEX_SPIN_TIME_COUNT: u32 = 4000;

const CK_STOP: usize = 0;
const CK_START: usize = 1;
const CK_SENT: usize = 2;
const CK_COPIED: usize = 3;

const OP_NONE: u32 = 0;
const OP_RECV: u32 = 1;
const OP_SEND: u32 = 2;

pub fn run(config: &ForwardingConfiguration) -> std::io::Result<()> {
    //std::thread::sleep(Duration::from_secs(10));

    let peer_count = config.peers.len();
    // number of send targets - we create registered buffer for the socket address of each send target
    let addr_buffer_count : u32 = peer_count as u32;
    let receive_thread_count : u32 = config.receive_thread_count as u32;
    let send_thread_count : u32 = config.send_thread_count as u32;
    let total_rio_pending_sends = RIO_PENDING_SENDS * peer_count as u32;
    let total_pending = RIO_PENDING_RECVS + total_rio_pending_sends;
    let pks = &config.pks;

    let bind_address : SocketAddr = config.socket.listen_address.parse().unwrap();

    let _wsa_data = startup().unwrap();

    let g_socket : SOCKET = socket().unwrap();
    bind_socket(g_socket, &bind_address).unwrap();

    let g_rio = create_rio_function_table(g_socket).unwrap();
    let g_receive_iocp : HANDLE = create_io_completion_port(receive_thread_count).unwrap();
    let g_send_iocp : HANDLE = create_io_completion_port(send_thread_count).unwrap();

    let g_request_completion_queue = create_io_completion_queue(g_rio, g_receive_iocp, RIO_PENDING_RECVS).unwrap();
    let g_send_completion_queue = create_io_completion_queue(g_rio, g_send_iocp, total_rio_pending_sends).unwrap();

    let g_request_queue = create_io_request_queue(g_rio, g_socket, g_request_completion_queue, g_send_completion_queue, RIO_PENDING_RECVS, total_rio_pending_sends).unwrap();

    let mut send_buffers = Vec::with_capacity(peer_count);
    // create buffers for each destination
    for i in 0..peer_count {
        let mut buffers = create_buffers(g_rio, SEND_BUFFER_SIZE, RIO_PENDING_SENDS, OP_SEND).unwrap();
        buffers.peer_idx = i;
        for buf in buffers.buffers.iter_mut() {
            buf.send_peer = i;
        }
        send_buffers.push(buffers);
    }

    let mut address_buffers = create_buffers(g_rio, ADDR_BUFFER_SIZE, addr_buffer_count, OP_NONE).unwrap();
    fill_with_peers(&mut address_buffers, config.peers);

    let mut receive_buffers = create_buffers(g_rio, RECV_BUFFER_SIZE, RIO_PENDING_RECVS, OP_RECV).unwrap();
    // add receive requests for each receive buffer to the queue so we can start reading immediately
    // further receives a queued when all sends are through
    post_receives(g_rio, g_request_queue, &mut receive_buffers.buffers).unwrap();

    let shared_rio = SharedRioExtensionFunctionTable(g_rio);
    let shared_request_completion_queue = SharedRioCQ(g_request_completion_queue);
    let shared_send_completion_queue = SharedRioCQ(g_send_completion_queue);
    let shared_request_queue = SharedRioCQ(g_request_queue);
    let shared_request_iocp = SharedHandle(g_receive_iocp);
    let shared_send_iocp = SharedHandle(g_send_iocp);
    let buffers = RioBuffers::new(&mut address_buffers, &mut send_buffers, &mut receive_buffers, peer_count, pks);
    let mut shared_buffers = Arc::new(Mutex::new(buffers));

    crossbeam::scope(|scope| {
        for idx in 0..receive_thread_count {
            println!("Starting read worker for {}", bind_address);
            let t_name = format!("RIO-Receive-Worker-{}", idx);
            scope.builder().name(t_name).spawn(|_| {
                iocp_receive_worker(&shared_rio, &shared_request_completion_queue, &shared_request_queue, &shared_request_iocp, &shared_send_iocp,  &shared_buffers);
                println!("Thread {} exited", std::thread::current().name().expect("No thread name"));
            }).unwrap();
        }

        for idx in 0..send_thread_count {
            println!("Starting send worker for {}", bind_address);
            let t_name = format!("RIO-Send-Worker-{}", idx);
            scope.builder().name(t_name).spawn(|_| {
                iocp_send_worker(&shared_rio, &shared_send_completion_queue, &shared_request_queue, &shared_request_iocp, &shared_send_iocp,  &shared_buffers);
                println!("Thread {} exited", std::thread::current().name().expect("No thread name"));
            }).unwrap();
        }

        // notify completion-ready
        rio_notify(g_rio, g_request_completion_queue).unwrap();
        rio_notify(g_rio, g_send_completion_queue).unwrap();
    }).unwrap();

    let mut buffers_lock = shared_buffers.lock();
    let buffers = &mut *buffers_lock;
    let send_buffers = &buffers.send_buffers;

    close_socket(g_socket).unwrap();
    close_completion_queue(g_rio, g_request_completion_queue).unwrap();
    close_completion_queue(g_rio, g_send_completion_queue).unwrap();
    for send_buffer in send_buffers.iter() {
        deregister_buffer(g_rio, send_buffer.rio_id()).unwrap();
    }
    deregister_buffer(g_rio, buffers.receive_buffers.rio_id()).unwrap();
    deregister_buffer(g_rio, buffers.address_buffers.rio_id()).unwrap();

    Ok(())
}

fn fill_with_peers(chunk: &mut RioChunkBuffers, peers: &Vec<SocketAddr>) {
    let chunk_ptr = chunk.buffer_pointer();
    for (idx, addr_buf) in chunk.buffers.iter_mut().enumerate() {
        let peer = peers.get(idx).unwrap();
        let mut rio_buf = addr_buf.buf;
        unsafe {
            let rio_buf_ptr = chunk_ptr.offset(rio_buf.Offset as isize);
            let rio_buf_socket_ptr = rio_buf_ptr as *mut SOCKADDR;
            let (raw_addr, _length) = addr2raw(peer);
            std::ptr::write(rio_buf_socket_ptr, *raw_addr);
        }
    }
}

fn post_receives(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ, buffers: &mut Vec<ExtendedRioBuf>) -> std::io::Result<()> {
    for addr_buf in buffers.iter_mut() {
        // posting pre RECVs
        let receive_result = rio_receive_ex(g_rio, g_request_queue, addr_buf);
        if receive_result.is_err() {
            return receive_result;
        }
    }

    Ok(())
}

fn create_critical_section() -> std::io::Result<CRITICAL_SECTION> {
    unsafe {
        let spin_count : DWORD = MUTEX_SPIN_TIME_COUNT as DWORD;
        let mut g_critical_section: CRITICAL_SECTION = mem::zeroed();
        let critical_section_ptr = (&mut g_critical_section) as LPCRITICAL_SECTION;
        let result = InitializeCriticalSectionAndSpinCount(critical_section_ptr, spin_count);
        if result == 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(g_critical_section);
    }
}

fn delete_critical_section(mut g_critical_section: CRITICAL_SECTION) -> std::io::Result<()> {
    unsafe {
        let critical_section_ptr = (&mut g_critical_section) as LPCRITICAL_SECTION;
        DeleteCriticalSection(critical_section_ptr);
        Ok(())
    }
}

fn enter_critical_section(mut g_critical_section: LPCRITICAL_SECTION) {
    unsafe {
        EnterCriticalSection(g_critical_section);
    }
}

fn leave_critical_section(mut g_critical_section: LPCRITICAL_SECTION) {
    unsafe {
        LeaveCriticalSection(g_critical_section);
    }
}

fn startup() -> std::io::Result<WSADATA> {
    unsafe {
        let mut data : WSADATA = mem::zeroed();
        let err = WSAStartup(0x202, &mut data);
        return os_error_result(err).map(|_| data);
    }
}

fn socket() -> std::io::Result<SOCKET> {
    unsafe {
        let socket : SOCKET = WSASocketW(AF_INET, SOCK_DGRAM, IPPROTO_UDP as i32, null_mut(), 0, WSA_FLAG_REGISTERED_IO);
        if socket == INVALID_SOCKET {
            return Err(std::io::Error::last_os_error());
        }
        SetHandleInformation(socket as HANDLE, HANDLE_FLAG_INHERIT, 0);
        return Ok(socket);
    }
}

fn close_socket(socket: SOCKET) -> std::io::Result<()> {
    unsafe {
        let result = closesocket(socket);
        return os_error_result(result);
    }
}

fn bind_socket(socket: SOCKET, socket_addr: &SocketAddr) -> std::io::Result<()> {
    unsafe {
        let (addr, len) =  addr2raw(socket_addr);
        let err = bind(socket, addr, len);
        return os_error_result(err);
    }
}

fn os_error_result(err: i32) -> std::io::Result<()> {
    return if err == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn addr2raw(addr: &SocketAddr) -> (*const SOCKADDR, c_int) {
    match *addr {
        SocketAddr::V4(ref a) => {
            (a as *const _ as *const _, mem::size_of_val(a) as c_int)
        }
        SocketAddr::V6(ref a) => {
            (a as *const _ as *const _, mem::size_of_val(a) as c_int)
        }
    }
}

fn create_rio_function_table(socket: SOCKET) -> std::io::Result<RIO_EXTENSION_FUNCTION_TABLE> {
    unsafe {
        let mut g_rio: RIO_EXTENSION_FUNCTION_TABLE = mem::zeroed();
        let mut bytes_returned: DWORD = 0;
        let mut function_table_id: GUID = WSAID_MULTIPLE_RIO;
        let wsa_result = WSAIoctl(
            socket,
            SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
            &mut function_table_id as *mut _ as LPVOID,
            mem::size_of_val(&function_table_id) as DWORD,
            &mut g_rio as *mut _ as LPVOID,
            mem::size_of_val(&g_rio) as DWORD,
            &mut bytes_returned as *mut _ as LPDWORD,
            null_mut(),
            None);
        if wsa_result == 0 {
            Ok(g_rio)
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

fn create_io_completion_port(concurrent_threads: u32) -> std::io::Result<HANDLE> {
    unsafe {
        let iocp_handle : HANDLE = CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(),0,concurrent_threads as DWORD);
        if iocp_handle.is_null() {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(iocp_handle)
        }
    }
}

fn create_io_completion_queue(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_hiocp: HANDLE, queue_size: u32) -> std::io::Result<RIO_CQ>{
    unsafe {
        let mut overlapped: OVERLAPPED = mem::zeroed();
        let mut completion_key = CK_START;
        let mut completion_type_u :RIO_NOTIFICATION_COMPLETION_u = std::mem::zeroed();
        *completion_type_u.Iocp_mut() = RIO_NOTIFICATION_COMPLETION_u_s2 {
            IocpHandle: g_hiocp,
            CompletionKey: &mut completion_key as *mut _ as PVOID,
            Overlapped: &mut overlapped as *mut _ as LPVOID
        };
        let mut completion_type = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            u: completion_type_u
        };

        // creating RIO CQ, which is bigger than (or equal to) RQ size
        let g_completion_queue : RIO_CQ = (g_rio.RIOCreateCompletionQueue.unwrap())(queue_size as DWORD, &mut completion_type);
        if g_completion_queue == RIO_INVALID_CQ {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(g_completion_queue)
        }
    }
}

fn create_io_request_queue(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_socket: SOCKET, g_receive_completion_queue: RIO_CQ, g_send_completion_queue: RIO_CQ, max_outstanding_receive: u32, max_outstanding_send: u32) -> std::io::Result<RIO_CQ> {
    unsafe {
        // must be 1!!
        let max_receive_data_buffers: ULONG = 1;
        // must be 1!!
        let max_send_data_buffers: ULONG = 1;
        // creating RIO RQ
        // SEND and RECV within one CQ (you can do with two CQs, seperately)
        let g_request_queue : RIO_CQ = (g_rio.RIOCreateRequestQueue.unwrap())(g_socket, max_outstanding_receive as ULONG, max_receive_data_buffers, max_outstanding_send as ULONG, max_send_data_buffers, g_receive_completion_queue, g_send_completion_queue, null_mut());
        if g_request_queue == RIO_INVALID_CQ {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(g_request_queue)
        }
    }
}

fn close_completion_queue(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_completion_queue: RIO_CQ) -> std::io::Result<()> {
    unsafe {
        (g_rio.RIOCloseCompletionQueue.unwrap())(g_completion_queue);
        Ok(())
    }
}

fn deregister_buffer(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_buffer_id: RIO_BUFFERID) -> std::io::Result<()> {
    unsafe {
        (g_rio.RIODeregisterBuffer.unwrap())(g_buffer_id);
        Ok(())
    }
}

fn create_buffers(g_rio: RIO_EXTENSION_FUNCTION_TABLE, buffer_size: u32, buffer_count: u32, operation: u32) -> std::io::Result<RioChunkBuffers> {
    unsafe {
        // registering RIO buffers for SEND
        let mut total_buffer_size : u32 = 0;
        let mut total_buffer_count : u32 = 0;

        let g_send_buffer_pointer : LPVOID = allocate_buffer_space(buffer_size, buffer_count, &mut total_buffer_size, &mut total_buffer_count).unwrap();
        let g_send_buffer_id : RIO_BUFFERID = (g_rio.RIORegisterBuffer.unwrap())(g_send_buffer_pointer as PCHAR, total_buffer_size as DWORD);
        if g_send_buffer_id == RIO_INVALID_BUFFERID {
            return Err(std::io::Error::last_os_error());
        }

        let mut g_send_rio_bufs: Vec<ExtendedRioBuf> = Vec::with_capacity(total_buffer_count as usize);
        let g_send_rio_buf_total_count = total_buffer_count;

        let mut offset: u32 = 0;

        for idx  in 0..g_send_rio_buf_total_count {
            // split g_send_rio_bufs to buffer_size for each RIO operation
            let rio_buf = ExtendedRioBuf::new(g_send_buffer_id, offset, buffer_size, operation, idx as usize);
            g_send_rio_bufs.push(rio_buf);
            offset += buffer_size;
        }
        Ok(RioChunkBuffers::new(g_send_rio_bufs, g_send_buffer_id, g_send_buffer_pointer))
    }
}

fn allocate_buffer_space(buf_size: u32, buf_count: u32, total_buffer_size: &mut u32, total_buffer_count: &mut u32) -> std::io::Result<LPVOID> {
    unsafe {
        let mut system_info: SYSTEM_INFO = mem::zeroed();
        GetSystemInfo(&mut system_info);

        let granularity = system_info.dwAllocationGranularity;
        let desired_size = buf_size * buf_count;
        let mut actual_size = round_up(desired_size, granularity);
        if actual_size > UINT_MAX {
            actual_size = (UINT_MAX / granularity) * granularity;
        }

        *total_buffer_count =  cmp::min(buf_count, actual_size / buf_size);
        *total_buffer_size = actual_size;

        let p_buffer : LPVOID = VirtualAllocEx(GetCurrentProcess(), null_mut(), actual_size as SIZE_T, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
        if p_buffer.is_null() {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(p_buffer)
        }
    }
}

// round up n to the multiple of m
fn round_up(n: u32, m:u32) -> u32 {
    return ((n + m - 1) / m) * m;
}

fn iocp_receive_worker(shared_rio: &SharedRioExtensionFunctionTable, shared_completion_queue: &SharedRioCQ, shared_request_queue: &SharedRioCQ, shared_request_iocp: &SharedHandle, shared_send_iocp: &SharedHandle, mutex: &Mutex<RioBuffers>) {
    let g_rio = shared_rio.0;
    let g_completion_queue = shared_completion_queue.0;
    let g_request_queue = shared_request_queue.0;
    let g_request_iocp = shared_request_iocp.0;
    let g_send_iocp = shared_send_iocp.0;

    let mut results_vec: ArrayVec<[RIORESULT;RIO_MAX_RESULTS]> = ArrayVec::<[RIORESULT; RIO_MAX_RESULTS]>::new();
    let mut number_of_bytes : u32 = 0;
    let mut completion_key : usize = CK_STOP;
    let mut overlapped : LPOVERLAPPED = null_mut();
    let mut offset = 0;
    let mut last_num_results = 0;
    loop {
        // Wait for the IOCP to be queued from RIO that we have results in our CQ
        get_queued_completion_status(g_request_iocp, &mut number_of_bytes, &mut completion_key, &mut overlapped).unwrap();

        if completion_key == CK_STOP {
            break;
        }

        // we have some outstanding data that needs to be copied to the send buffers
        if offset != 0 && last_num_results != 0 {
            let expected_count = last_num_results-offset;
            let copied_count = handle_receive_result(g_rio, g_request_queue, &results_vec, offset, last_num_results, mutex);
            if copied_count != expected_count {
                offset += copied_count;
                // we again failed to copy all data - retry next signal
                continue;
            }
        }

        // this signal is just for us to copy outstanding results - it does not mean that there are already real IO events to read
        if completion_key == CK_SENT {
            continue;
        }

        // do not call get_queued_completion_status until we have zero results - this ensures that we do not waste a syscall on high load
        loop {
            let num_results= do_dequeue(g_rio, g_completion_queue, &mut results_vec, mutex);
            let copied_count = handle_receive_result(g_rio, g_request_queue, &results_vec, 0, num_results, mutex);
            if copied_count != 0 {
                // notify the send iocp that we have copied more data
                post_queued_completion_status(g_send_iocp, CK_COPIED).unwrap();
            }
            if copied_count != num_results {
                offset += copied_count;
                last_num_results = num_results;
                break;
            }
        }
    }
}

fn iocp_send_worker(shared_rio: &SharedRioExtensionFunctionTable, shared_completion_queue: &SharedRioCQ, shared_request_queue: &SharedRioCQ, shared_request_iocp: &SharedHandle, shared_send_iocp: &SharedHandle, mutex: &Mutex<RioBuffers>) {
    let g_rio = shared_rio.0;
    let g_completion_queue = shared_completion_queue.0;
    let g_request_queue = shared_request_queue.0;
    let g_request_iocp = shared_request_iocp.0;
    let g_send_iocp = shared_send_iocp.0;

    let mut results_vec: ArrayVec<[RIORESULT;RIO_MAX_RESULTS]> = ArrayVec::<[RIORESULT; RIO_MAX_RESULTS]>::new();
    let mut number_of_bytes : u32 = 0;
    let mut completion_key : usize = CK_STOP;
    let mut overlapped : LPOVERLAPPED = null_mut();
    loop {
        // Wait for the IOCP to be queued from RIO that we have results in our CQ
        get_queued_completion_status(g_send_iocp, &mut number_of_bytes, &mut completion_key, &mut overlapped).unwrap();

        if completion_key == CK_STOP {
            break;
        }

        // this signal is just for us to copy outstanding results - it does not mean that there are already real IO events to read
        if completion_key == CK_COPIED {
            queue_sends(g_rio, g_request_queue, mutex).unwrap();
            // we have sent all send request wait for completion now
            continue;
        }

        // do not call get_queued_completion_status until we have zero results - this ensures that we do not waste a syscall on high load
        loop {
            let num_results= do_dequeue(g_rio, g_completion_queue, &mut results_vec, mutex);
            if num_results == 0 {
                // break from inner loop
                break;
            }

            let (finished_sends, completed_sends) = handle_send_result(&results_vec, num_results, mutex);
            if finished_sends > 0 {
                post_queued_completion_status(g_request_iocp, CK_SENT).unwrap();
            }
        }
    }
}

#[inline]
fn do_dequeue(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_completion_queue: RIO_CQ, results_vec: &mut ArrayVec<[RIORESULT;RIO_MAX_RESULTS]>, mutex: &Mutex<RioBuffers>) -> u32 {
    let num_results;
    {
        // Dequeue from the RIO socket under our locks - dequeue is not thread safe
        let _buffers_lock = mutex.lock();
        num_results = rio_dequeue_completion_vec(g_rio, g_completion_queue, results_vec).unwrap();
        // Immediately after invoking Dequeue, post another Notify
        rio_notify(g_rio, g_completion_queue).unwrap();
    }
    return num_results;
}

#[inline]
fn queue_sends(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ, mutex: &Mutex<RioBuffers>) -> std::io::Result<u32> {
    let mut send_count : u32 = 0;
    {
        let mut buffers_lock = mutex.lock();
        let buffers = &mut *buffers_lock;
        let send_buffers = &mut buffers.send_buffers;
        for peer_send_buffers in send_buffers.iter_mut() {
            let buffer_count = peer_send_buffers.buffer_idx;
            // inclusive for loop
            for idx in 0..=buffer_count {
                let send_buffer = peer_send_buffers.buffers.get_mut(idx).unwrap();
                // commit the last send operation
                let send_result = rio_send_ex(g_rio, g_request_queue, send_buffer, false);
                if send_result.is_err() {
                    return send_result.map(|_|0);
                }
                send_count += 1;
            }
        }
    }
    if send_count > 0 {
        // commit sends outside of the mutex because this operation is most probably the bottleneck
        rio_commit_send(g_rio, g_request_queue).unwrap();
    }
    return Ok(send_count);
}

#[inline]
fn handle_send_result(results_vec: &ArrayVec<[RIORESULT;RIO_MAX_RESULTS]>, num_results: u32, mutex: &Mutex<RioBuffers>) -> (u32, u32) {
    let mut finished_sends : u32 = 0;
    let mut completed_sends : u32 = 0;
    for idx in 0..num_results {
        let result: &RIORESULT = results_vec.get(idx as usize).unwrap();
        let ext_buffer: &mut ExtendedRioBuf = get_context_buffer(result);
        let status: i32 = result.Status;
        let operation = ext_buffer.operation;
        if status != 0 {
            println!("Failed to receive/send request status {} on operation {}", status, operation_to_string(operation));
            break;
        }
        if OP_SEND == operation {
            let mut buffers_lock = mutex.lock();
            let buffers = &mut *buffers_lock;
            let sent_to_all = on_send_op(buffers, ext_buffer);
            if sent_to_all {
                let pks = buffers.pks;
                pks.on_packet();
                completed_sends += 1;
            }
            finished_sends += 1;
        }
    }
    return (finished_sends, completed_sends);
}

#[inline]
fn handle_receive_result(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ, results_vec: &ArrayVec<[RIORESULT;RIO_MAX_RESULTS]>, offset: u32, num_results: u32, mutex: &Mutex<RioBuffers>) -> u32 {
    let mut copied_results : u32 = 0;
    for idx in offset..num_results {
        let result: &RIORESULT = results_vec.get(idx as usize).unwrap();
        let ext_buffer: &mut ExtendedRioBuf = get_context_buffer(result);
        let status: i32 = result.Status;
        let operation = ext_buffer.operation;
        if status != 0 {
            println!("Failed to receive/send request status {} on operation {}", status, operation_to_string(operation));
            break;
        }
        if OP_RECV == operation {
            let read_data: u32 = result.BytesTransferred;
            let mut buffers_lock = mutex.lock();
            let buffers = &mut *buffers_lock;
            let (_copied_entries, is_send_full) = on_receive_op(g_rio, g_request_queue, buffers, ext_buffer, read_data).unwrap();
            if is_send_full {
                break;
            } else {
                copied_results += 1;
            }
        } else {
            // the operation is not for us - this should not happen but we still increment the result to be sure
            copied_results += 1;
        }
    }
    return copied_results;
}

// returns a mutable reference to the raw pointer of the RequestContext variable
#[inline]
fn get_context_buffer(result: &RIORESULT) -> &mut ExtendedRioBuf {
    let context_ptr : *const ExtendedRioBuf = result.RequestContext as *const ExtendedRioBuf;
    let ext_buffer_ptr = context_ptr as *mut ExtendedRioBuf;
    let ext_buffer : &mut ExtendedRioBuf;
    unsafe {
        ext_buffer = &mut *ext_buffer_ptr;
    }
    return ext_buffer;
}

#[inline]
fn operation_to_string<'a>(operation: u32) -> &'a str {
    return match operation {
        1 => "OP_RECV",
        2 => "OP_SEND",
        _ => "UNKNOWN",
    }
}

#[inline]
fn copy_to_send_buffers(buffers: &mut RioBuffers, buffer: &mut ExtendedRioBuf, read_data: u32) -> u32 {
    let address_buffers = &mut buffers.address_buffers;
    let send_buffers = &mut buffers.send_buffers;
    let mut copied_count : u32 = 0;
    for (idx, send_addr_buffer) in address_buffers.buffers.iter_mut().enumerate() {
        // copy the read data into the send buffer of the destination
        let send_buffer = send_buffers.get_mut(idx).unwrap();
        let send_ptr = send_buffer.buffer_pointer();
        let receive_ptr = buffers.receive_buffers.buffer_pointer();
        let send_rio_buffer_op = send_buffer.get_send_buffer();
        if send_rio_buffer_op.is_some() {
            let send_rio_buffer = send_rio_buffer_op.unwrap();
            // save the target address buffer the address buffers are never changed
            send_rio_buffer.remote_address = Option::Some(&mut send_addr_buffer.buf);
            // save the index of the receive buffer
            send_rio_buffer.receive_index = buffer.index;

            // copy the received data into the send buffer
            rio_copy(receive_ptr, send_ptr, buffer, send_rio_buffer, read_data);
            copied_count += 1;
        } else {
            // we are out of send buffer
            break;
        }
    }
    return copied_count;
}

#[inline]
fn on_receive_op(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ, buffers: &mut RioBuffers, receive_buffer: &mut ExtendedRioBuf, read_data: u32) -> std::io::Result<(u32, bool)> {
    // copy data into send buffers
    let copied_entries = copy_to_send_buffers(buffers, receive_buffer, read_data);
    let is_full = copied_entries != buffers.send_peer_count as u32;
    if !is_full {
        let receive_result = rio_receive_ex(g_rio, g_request_queue, receive_buffer);
        return receive_result.map(|_| (copied_entries,false));
    }
    Ok((copied_entries,true))
}

#[inline]
fn on_send_op(buffers: &mut RioBuffers, send_buffer: &mut ExtendedRioBuf) -> bool {
    let receive_index = send_buffer.receive_index;
    buffers.finish_send(send_buffer);

    let receive_buffer = buffers.receive_buffers.buffers.get_mut(receive_index).unwrap();
    receive_buffer.send_count += 1;
    let send_peer_count = buffers.send_peer_count;

    // if the read data is successfully sent to all clients we queue a additional receive request
    if receive_buffer.send_count == send_peer_count {
        receive_buffer.send_count = 0;
        return true;
    }

    return false;
}

#[inline]
fn get_queued_completion_status(g_hiocp: HANDLE, number_of_bytes: &mut u32, completion_key: &mut usize, overlapped: &mut LPOVERLAPPED) -> std::io::Result<()> {
    unsafe {
        let mut completion_key_int : ULONG_PTR = 0;
        let result = GetQueuedCompletionStatus(g_hiocp, number_of_bytes as LPDWORD, &mut completion_key_int as PULONG_PTR, overlapped, INFINITE);
        if result == 0 {
            return Err(std::io::Error::last_os_error())
        }
        *completion_key = completion_key_int;
        return Ok(());
    }
}

#[inline]
fn rio_dequeue_completion_vec(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_completion_queue: RIO_CQ, result: &mut ArrayVec<[RIORESULT;RIO_MAX_RESULTS]>) -> std::io::Result<u32> {
    let result_ptr : PRIORESULT = result.as_mut_ptr();
    let num_results = rio_dequeue_completion(g_rio, g_completion_queue, result_ptr);
    return if num_results.is_ok() {
        let f_num_results = num_results.unwrap();
        unsafe {
            result.set_len(f_num_results as usize);
        }
        Ok(f_num_results)
    } else {
        num_results
    }
}

#[inline]
fn rio_dequeue_completion(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_completion_queue: RIO_CQ, result: PRIORESULT) -> std::io::Result<u32> {
    unsafe {
        let num_results = (g_rio.RIODequeueCompletion.unwrap())(g_completion_queue, result, RIO_MAX_RESULTS as ULONG);
        if num_results == RIO_CORRUPT_CQ {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(num_results);
    }
}

#[inline]
fn rio_notify(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_completion_queue: RIO_CQ) -> std::io::Result<()> {
    unsafe {
        let result = (g_rio.RIONotify.unwrap())(g_completion_queue);
        if result != ERROR_SUCCESS as i32 {
            return Err(std::io::Error::from_raw_os_error(result));
        }
        return Ok(());
    }
}

#[inline]
fn rio_receive_ex(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ, buffer: &mut ExtendedRioBuf) -> std::io::Result<()> {
    unsafe {
        let ext_rio_raw_ptr : *const ExtendedRioBuf = buffer as *mut ExtendedRioBuf;
        let ext_rio_buf_ptr = ext_rio_raw_ptr as LPVOID;
        let rio_buf : PRIO_BUF = &mut buffer.buf;
        let result = (g_rio.RIOReceiveEx.unwrap())(g_request_queue, rio_buf, 1, null_mut(), null_mut(), null_mut(), null_mut(), 0, ext_rio_buf_ptr);
        if result == 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }
}

#[inline]
fn rio_commit_send(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ) -> std::io::Result<()> {
    unsafe {
        let result = (g_rio.RIOSendEx.unwrap())(g_request_queue, null_mut(), 0, null_mut(), null_mut(), null_mut(), null_mut(), RIO_MSG_COMMIT_ONLY, null_mut());
        if result == 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }
}

#[inline]
fn rio_send_ex(g_rio: RIO_EXTENSION_FUNCTION_TABLE, g_request_queue: RIO_CQ, buffer: &mut ExtendedRioBuf, commit: bool) -> std::io::Result<()> {
    unsafe {
        let buffer_ptr : PVOID = buffer as *mut _ as LPVOID;
        let rio_buffer_ptr : PRIO_BUF = &mut buffer.buf;
        let remote_addr_ptr = buffer.remote_address.unwrap();
        let flags;
        if commit {
            flags = 0;
        } else {
            flags = RIO_MSG_DEFER;
        }
        let result = (g_rio.RIOSendEx.unwrap())(g_request_queue, rio_buffer_ptr, 1, null_mut(), remote_addr_ptr, null_mut(), null_mut(), flags, buffer_ptr);
        if result == 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }
}

#[inline]
fn rio_copy(src_chunk: LPVOID, dst_chunk: LPVOID, src: &mut ExtendedRioBuf, dst: &mut ExtendedRioBuf, length: u32) {
    // copy the read data into the send buffer of the destination
    unsafe {
        let src_offset_ptr = src_chunk.offset(src.buf.Offset as isize);
        let dst_offset_ptr = dst_chunk.offset(dst.buf.Offset as isize);
        dst.buf.Length = length;
        std::ptr::copy_nonoverlapping(src_offset_ptr, dst_offset_ptr, length as usize);
    }
}

fn post_queued_completion_status(completion_port: HANDLE, completion_key: ULONG_PTR) -> std::io::Result<()> {
    unsafe {
        let result = PostQueuedCompletionStatus(completion_port, 0, completion_key, null_mut());
        if result == 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }
}

struct ExtendedRioBuf {
    buf: RIO_BUF,
    operation: u32,
    // the index of the buffer in the chunk
    index: usize,
    // the times this buffer was successfully sent (only used for receive buffers)
    send_count: usize,
    // the index of the buffer in the receive chunk (only used for send buffers)
    receive_index: usize,
    // track the client index this buffer is associated with (only used for send buffers)
    send_peer: usize,
    // the buffer containing the remote address (only used for send buffers)
    remote_address: Option<PRIO_BUF>,
}

unsafe impl Send for ExtendedRioBuf {}
unsafe impl Sync for ExtendedRioBuf {}


impl ExtendedRioBuf {
    fn new(buf_id: RIO_BUFFERID, offset: u32, length: u32, operation: u32, index: usize) -> ExtendedRioBuf {
        ExtendedRioBuf::new_send(buf_id, offset, length, operation, 0, index)
    }

    fn new_send(buf_id: RIO_BUFFERID, offset: u32, length: u32, operation: u32, send_peer: usize, index: usize) -> ExtendedRioBuf {
        ExtendedRioBuf {
            buf: RIO_BUF {
                BufferId: buf_id,
                Offset: offset as ULONG,
                Length: length as ULONG
            },
            operation,
            send_peer,
            index,
            send_count: 0,
            receive_index: 0,
            remote_address: Option::None
        }
    }
}

struct RioChunkBuffers {
    buffers: Vec<ExtendedRioBuf>,
    // the native id for the buffers
    rio_id: SharedRioBufferId,
    // pointer to the buffer location
    buffer_pointer: SharedLPVOID,
    // track the client index this buffer is associated with
    peer_idx: usize,
    // the current used index
    // there is always only a single send "in-flight" for a single client
    // therefore we can simply increment/decrement this index
    buffer_idx: usize,
}


impl RioChunkBuffers {
    fn new(buffers: Vec<ExtendedRioBuf>, rio_id: RIO_BUFFERID, buffer_pointer: LPVOID) -> RioChunkBuffers {
        RioChunkBuffers::new_send(buffers, rio_id, buffer_pointer, 0)
    }

    fn new_send(buffers: Vec<ExtendedRioBuf>, rio_id: RIO_BUFFERID, buffer_pointer: LPVOID, peer_idx: usize) -> RioChunkBuffers {
        RioChunkBuffers {
            buffers,
            rio_id: SharedRioBufferId(rio_id),
            buffer_pointer: SharedLPVOID(buffer_pointer),
            peer_idx,
            buffer_idx: 0,
        }
    }

    #[inline]
    fn rio_id(&self) -> RIO_BUFFERID {
        return self.rio_id.0;
    }

    #[inline]
    fn buffer_pointer(&self) -> LPVOID {
        return self.buffer_pointer.0;
    }

    #[inline]
    fn get_send_buffer(&mut self) -> Option<&mut ExtendedRioBuf> {
        let send_buffer = self.buffers.get_mut(self.buffer_idx);
        if send_buffer.is_some() {
            self.buffer_idx += 1;
        }
        return send_buffer;
    }

    #[inline]
    fn finish_send(&mut self) {
        self.buffer_idx -= 1;
    }
}

struct RioBuffers<'a> {
    address_buffers: &'a mut RioChunkBuffers,
    send_buffers: &'a mut Vec<RioChunkBuffers>,
    receive_buffers: &'a mut RioChunkBuffers,
    // how may targets do we have
    send_peer_count: usize,
    // how many packets have we sent so far since the last commit
    send_commit_count: usize,
    pks: &'a&'a mut PacketsPerSecond
}

impl<'a> RioBuffers<'a> {
    fn new(address_buffers: &'a mut RioChunkBuffers, send_buffers: &'a mut Vec<RioChunkBuffers>, receive_buffers: &'a mut RioChunkBuffers, send_peer_count: usize, pks: &'a&'a mut PacketsPerSecond) -> RioBuffers<'a> {
        RioBuffers {
            address_buffers,
            send_buffers,
            receive_buffers,
            send_peer_count,
            send_commit_count: 0,
            pks
        }
    }

    #[inline]
    fn finish_send(&mut self, buffer: &ExtendedRioBuf) {
        let send_buffers = self.send_buffers.get_mut(buffer.send_peer).unwrap();
        // make the buffer usable again
        send_buffers.finish_send();
    }
}

struct SharedRioExtensionFunctionTable(RIO_EXTENSION_FUNCTION_TABLE);
unsafe impl Send for SharedRioExtensionFunctionTable {}
unsafe impl Sync for SharedRioExtensionFunctionTable {}

struct SharedRioCQ(RIO_CQ);
unsafe impl Send for SharedRioCQ {}
unsafe impl Sync for SharedRioCQ {}

struct SharedHandle(HANDLE);
unsafe impl Send for SharedHandle {}
unsafe impl Sync for SharedHandle {}

struct SharedRioBufferId(RIO_BUFFERID);
unsafe impl Send for SharedRioBufferId {}
unsafe impl Sync for SharedRioBufferId {}

struct SharedLPVOID(LPVOID);
unsafe impl Send for SharedLPVOID {}
unsafe impl Sync for SharedLPVOID {}