use tokio::sync::broadcast;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Streaming;
use tonic::{transport::Server, Request, Response, Status};

use chat::chat_service_server::{ChatService, ChatServiceServer};
use chat::{
    ReceiveMessagesRequest, ReceiveMessagesResponse, SendMessageRequest, SendMessageResponse,
    TypingStatusRequest, TypingStatusResponse,
};

pub mod chat {
    tonic::include_proto!("nyxin.api.chat.v1");
}

pub struct ChatServer {
    sender: broadcast::Sender<ReceiveMessagesResponse>,
}

impl ChatServer {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(10);
        Self { sender: tx }
    }
}

#[tonic::async_trait]
impl ChatService for ChatServer {
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let req = request.into_inner();
        let string = String::from_utf8(req.encrypted_message.clone()).expect("Invalid UTF-8");
        println!("msg: {} | from: {}", string, req.sender_id.clone());
        let msg = ReceiveMessagesResponse {
            sender_id: req.sender_id,
            receiver_id: req.receiver_id,
            encrypted_message: req.encrypted_message,
            timestamp: req.timestamp,
        };

        let _ = self.sender.send(msg);

        Ok(Response::new(SendMessageResponse { success: true }))
    }

    type ReceiveMessagesStream = ReceiverStream<Result<ReceiveMessagesResponse, Status>>;

    async fn receive_messages(
        &self,
        request: Request<ReceiveMessagesRequest>,
    ) -> Result<Response<Self::ReceiveMessagesStream>, Status> {
        let mut rx = self.sender.subscribe();
        let (tx, rx_stream) = mpsc::channel(10);

        let receiver_id = request.into_inner().receiver_id.clone();
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                println!("send: {} | aaaa: {}", msg.sender_id, receiver_id);
                if msg.sender_id == receiver_id {
                    continue;
                } else {
                    if tx.send(Ok(msg)).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx_stream)))
    }

    type TypingStatusStream = ReceiverStream<Result<TypingStatusResponse, Status>>;

    async fn typing_status(
        &self,
        request: Request<Streaming<TypingStatusRequest>>,
    ) -> Result<Response<Self::TypingStatusStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            while let Some(Ok(req)) = stream.next().await {
                let response = TypingStatusResponse {
                    sender_id: req.sender_id,
                    is_typing: req.is_typing,
                };

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9999".parse().unwrap();
    let chat_server = ChatServer::new();

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(ChatServiceServer::new(chat_server))
        .serve(addr)
        .await?;

    Ok(())
}
