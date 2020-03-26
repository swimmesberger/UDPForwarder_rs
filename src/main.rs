use tokio;
use tokio::net::UdpSocket;
use std::error::Error;
//use futures::future::join_all;
//use core::future::Future;
//use std::io::Write;
use clap::{App, Arg};
use bytes::{BytesMut};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>  {
    let matches = App::new("udp_forwarder")
        .version("1.0")
        .about("Forwards UDP data from one port to multiple others")
        .author("Simon Wimmesberger")
        .arg(Arg::with_name("input-address").short("i").help("The address the server should listen on").required(true).takes_value(true))
        .arg(Arg::with_name("output-address").short("o").help("The address the server should send data to").required(true).multiple(true).takes_value(true))
        .get_matches();

    let listen_address = matches.value_of("input-address").unwrap();
    let peers: Vec<_> = matches.values_of("output-address").unwrap().map(|address| address.parse().unwrap()).collect();

    #[cfg(debug_assertions)]
    println!("Binding socket on {}", listen_address);
    let socket = UdpSocket::bind(&listen_address).await?;

    let (mut receive, mut send) = socket.split();

    let mut buf = BytesMut::with_capacity(65550);
    // init full buffer - otherwise we can't receive anything
    unsafe {
        buf.set_len(65550);
    }

    #[cfg(debug_assertions)]
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
        //let mut writes= Vec::new();
        //let mut peer_index = 0;
        for peer in peers.iter() {
            #[cfg(debug_assertions)]
            println!("Sending data {}", read_buf.len());
            let written_bytes_result = send.send_to(&mut read_buf, peer).await;
            if written_bytes_result.is_err() {
                #[cfg(debug_assertions)]
                println!("Error on send {}", peer);
                continue;
            }
            let written_bytes = written_bytes_result.unwrap();
            #[cfg(debug_assertions)]
            println!("Sent data {}", written_bytes);

            //peer_index = peer_index+1;
            //let write_future = send.send_to(&mut buf_copy, peer);
            //writes.push(write_future);
        }
        //join_all(writes).await;
    }
}
