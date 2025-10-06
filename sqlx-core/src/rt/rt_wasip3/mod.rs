use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use bytes::BytesMut;
use wasip3::{wit_bindgen, wit_future};
use crate::net::WithSocket;

mod socket;

pub struct JoinHandle<T : 'static> {
    rx: wit_bindgen::FutureReader<Result<(), wasip3::http::types::ErrorCode>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // For now, just return None since we can't properly handle the generic T
        // This is a limitation of the current wasip3 FuturePayload constraints
        Poll::Ready(None)
    }
}

pub fn spawn<T: 'static>(fut: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let (tx, rx) = wit_future::new::<Result<(), wasip3::http::types::ErrorCode>>(|| Ok(()));
    
    wasip3::wit_bindgen::spawn(async move {
        let _v = fut.await;
        if let Err(_) = tx.write(Ok(())).await {
            eprintln!("Failed to signal completion");
        }
    });
    
    JoinHandle { 
        rx,
        _phantom: std::marker::PhantomData,
    }
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
    let sock = wasip3::sockets::types::TcpSocket::create(wasip3::sockets::types::IpAddressFamily::Ipv4)
        .expect("failed to create TCP socket");
    sock.connect(wasip3::sockets::types::IpSocketAddress::Ipv4(
        wasip3::sockets::types::Ipv4SocketAddress {
            address: (127, 0, 0, 1),
            port,
        },
    ))
    .await
    .expect(&format!("failed to connect to 127.0.0.1:{port}"));

    let (rx_tx, rx_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let (tx_tx, mut tx_rx) = tokio::sync::mpsc::channel(1);
    let (mut send_tx, send_rx) = wasip3::wit_stream::new();
    let (mut recv_rx, recv_fut) = sock.receive();

    let task = tokio::task::spawn_local(async move {
        use futures_util::{SinkExt, StreamExt};

        let sock = Arc::new(sock);

        // We replace oneshot::channel with wit_future
        let (ready_tx, ready_rx) = wit_future::new::<Result<(), wasip3::http::types::ErrorCode>>(|| Ok(()));
        
        wasip3::wit_bindgen::spawn({
            let sock = Arc::clone(&sock);
            async move {
                let fut = sock.send(send_rx);
                _ = ready_tx.write(Ok(()));
                _ = fut.await.unwrap();
                drop(sock);
            }
        });
        
        wasip3::wit_bindgen::spawn({
            let sock = Arc::clone(&sock);
            async move {
                let _ = recv_fut.await.unwrap();
                drop(sock);
            }
        });
        
        futures_util::join!(
            async {
                use futures_util::StreamExt;
                while let Some(result) = recv_rx.next().await {
                    _ = rx_tx.send(vec![result]).await;
                }
                drop(recv_rx);
                drop(rx_tx);
            },
            async {
                _ = ready_rx.await;
                while let Some(buf) = tx_rx.recv().await {
                    let (_result, _buffer) = send_tx.write(buf).await;
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
}
