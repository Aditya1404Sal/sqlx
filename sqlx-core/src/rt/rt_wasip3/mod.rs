use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use bytes::BytesMut;
use wasi::async_support;
use wasi::async_support::futures::channel::oneshot;

use crate::net::WithSocket;

mod socket;

pub struct JoinHandle<T> {
    rx: oneshot::Receiver<T>,
}

impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(v)) => Poll::Ready(Some(v)),
            Poll::Ready(Err(oneshot::Canceled)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn spawn<T: 'static>(fut: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let (tx, rx) = oneshot::channel();
    async_support::spawn(async move {
        let v = fut.await;
        _ = tx.send(v);
    });
    JoinHandle { rx }
}

pub struct TcpSocket {
    pub tx: tokio_util::sync::PollSender<Vec<u8>>,
    pub rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    pub buf: BytesMut,
    pub task: tokio::task::JoinHandle<()>,
}

impl Drop for TcpSocket {
    fn drop(&mut self) {
        self.task.abort()
    }
}

pub async fn connect_tcp<Ws: WithSocket>(
    host: &str,
    port: u16,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    //let ips = wasi::sockets::ip_name_lookup::resolve_addresses(host)
    //    .await
    //    .expect("failed to lookup IP");
    //for ip in ips {
    //    let (family, addr) = match ip {
    //        wasi::sockets::types::IpAddress::Ipv4(address) => (
    //            wasi::sockets::types::IpAddressFamily::Ipv4,
    //            wasi::sockets::types::IpSocketAddress::Ipv4(
    //                wasi::sockets::types::Ipv4SocketAddress { address, port },
    //            ),
    //        ),
    //        wasi::sockets::types::IpAddress::Ipv6(address) => (
    //            wasi::sockets::types::IpAddressFamily::Ipv6,
    //            wasi::sockets::types::IpSocketAddress::Ipv6(
    //                wasi::sockets::types::Ipv6SocketAddress {
    //                    address,
    //                    port,
    //                    flow_info: 0,
    //                    scope_id: 0,
    //                },
    //            ),
    //        ),
    //    };
    //   let sock = wasi::sockets::types::TcpSocket::new(family);
    let sock = wasi::sockets::types::TcpSocket::new(wasi::sockets::types::IpAddressFamily::Ipv4);
    sock.connect(wasi::sockets::types::IpSocketAddress::Ipv4(
        wasi::sockets::types::Ipv4SocketAddress {
            address: (127, 0, 0, 1),
            port,
        },
    ))
    .await
    .expect(&format!("failed to connect to 127.0.0.1:{port}"));


    let (rx_tx, rx_rx) = tokio::sync::mpsc::channel(1);
    let (tx_tx, mut tx_rx) = tokio::sync::mpsc::channel(1);
    let (mut send_tx, send_rx) = wasi::wit_stream::new();
    let (mut recv_rx, recv_fut) = sock.receive();

    let task = tokio::task::spawn_local(async move {
        use futures_util::{SinkExt, StreamExt};

        let sock = Arc::new(sock);

        let (ready_tx, ready_rx) = oneshot::channel();
        async_support::spawn({
            let sock = Arc::clone(&sock);
            async move {
                let fut = sock.send(send_rx);
                _ = ready_tx.send(());
                _ = fut.await.unwrap();
                drop(sock);
            }
        });
        async_support::spawn({
            let sock = Arc::clone(&sock);
            async move {
                _ = recv_fut.await.unwrap();
                drop(sock);
            }
        });
        futures_util::join!(
            async {
                while let Some(Ok(buf)) = recv_rx.next().await {
                   _  = rx_tx.send(buf).await;
                }
                drop(recv_rx);
                drop(rx_tx);
            },
            async {
                _ = ready_rx.await;
                while let Some(buf) = tx_rx.recv().await {
                    _ = send_tx.send(buf).await;
                }
                drop(tx_rx);
                drop(send_tx);
            },
        );
    });
    Ok(with_socket
        .with_socket(TcpSocket {
            tx: tokio_util::sync::PollSender::new(tx_tx),
            rx: rx_rx,
            buf: bytes::BytesMut::new(),
            task,
        })
        .await)
    //}
}
