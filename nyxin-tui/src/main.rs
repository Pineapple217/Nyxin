use crossterm::event::{self, KeyCode, KeyEvent};
use crossterm::{self, execute, terminal};
use ratatui::{prelude::*, widgets::*};
use std::io::{self, stdout};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use chat::chat_service_client::ChatServiceClient;
use chat::{ReceiveMessagesRequest, SendMessageRequest, SendMessageResponse};

pub mod chat {
    tonic::include_proto!("nyxin.api.chat.v1");
}

struct App {
    input: String,
    messages: Vec<String>,
    client: ChatServiceClient<Channel>,
}

impl App {
    pub async fn new() -> Self {
        let client = ChatServiceClient::connect("http://127.0.0.1:9999")
            .await
            .unwrap();
        Self {
            input: String::default(),
            messages: Vec::default(),
            client: client,
        }
    }
}

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
    terminal::enable_raw_mode()?;
    let backend = CrosstermBackend::new(stdout());
    let terminal = Terminal::new(backend)?;

    execute!(stdout(), terminal::Clear(terminal::ClearType::All))?;

    Ok(terminal)
}

fn restore_terminal() -> io::Result<()> {
    terminal::disable_raw_mode()?;
    execute!(stdout(), terminal::Clear(terminal::ClearType::All))?;
    Ok(())
}

async fn send_message(
    client: &mut ChatServiceClient<Channel>,
    sender_id: &str,
    receiver_id: &str,
    message: &str,
) -> Result<SendMessageResponse, Box<dyn std::error::Error>> {
    let request = SendMessageRequest {
        sender_id: sender_id.to_string(),
        receiver_id: receiver_id.to_string(),
        encrypted_message: message.as_bytes().to_vec(),
        timestamp: chrono::Utc::now().timestamp(),
    };

    let response = client.send_message(request).await?;
    Ok(response.into_inner())
}

async fn receive_messages(
    receiver_id: &str,
    app: Arc<Mutex<App>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = ReceiveMessagesRequest {
        receiver_id: receiver_id.to_string(),
    };

    let mut stream = app
        .lock()
        .await
        .client
        .receive_messages(request)
        .await?
        .into_inner();

    while let Some(response) = stream.message().await? {
        let message = String::from_utf8(response.encrypted_message)?;
        app.lock()
            .await
            .messages
            .push(format!("{}: {}", response.sender_id, message));
    }

    Ok(())
}

fn draw_ui(f: &mut Frame, app: &App) {
    let size = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(size);

    let text: Vec<ListItem> = app
        .messages
        .iter()
        .map(|m| ListItem::new(m.as_str()))
        .collect();
    let list = List::new(text).block(Block::default().borders(Borders::ALL).title("Chat"));
    f.render_widget(list, chunks[0]);

    let input_box = Paragraph::new(app.input.to_string())
        .block(Block::default().borders(Borders::ALL).title("Input"));
    f.render_widget(input_box, chunks[1]);
}

fn draw_username_prompt(f: &mut Frame, app: &App) {
    let size = f.area();
    let prompt = "Please enter your username: ";

    let paragraph = Paragraph::new(prompt.to_string() + &app.input)
        .block(Block::default().borders(Borders::ALL).title("Username"));
    f.render_widget(paragraph, size);
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut terminal = setup_terminal()?;
    let app1 = Arc::new(Mutex::new(App::new().await));

    let mut username: Option<String> = None;
    let app_clone = app1.clone();
    'main: loop {
        if username.is_none() {
            let app = app1.lock().await;
            terminal.draw(|f| draw_username_prompt(f, &app))?;
        } else {
            let receiver_id = username.clone().unwrap();
            let _receive_task = tokio::spawn(async move {
                receive_messages(&receiver_id, app_clone).await.unwrap();
            });

            loop {
                let mut app = app1.lock().await;
                terminal.draw(|f| draw_ui(f, &app))?;
                if event::poll(Duration::from_millis(100))? {
                    if let event::Event::Key(KeyEvent { code, .. }) = event::read()? {
                        match code {
                            KeyCode::Char(c) => app.input.push(c),
                            KeyCode::Backspace => {
                                app.input.pop();
                            }
                            KeyCode::Enter => {
                                if !app.input.is_empty() {
                                    let sender_id = username.clone().unwrap();
                                    let receiver_id = "user999";
                                    let message = app.input.clone();

                                    let response = send_message(
                                        &mut app.client,
                                        &sender_id,
                                        receiver_id,
                                        &message,
                                    )
                                    .await?;

                                    if response.success {
                                        app.messages.push(format!("You: {}", message));
                                    } else {
                                        app.messages.push("Failed to send message.".to_string());
                                    }
                                    app.input.clear();
                                }
                            }
                            KeyCode::Esc => {
                                break 'main;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        {
            let mut app = app1.lock().await;
            if event::poll(Duration::from_millis(100))? {
                if let event::Event::Key(KeyEvent { code, .. }) = event::read()? {
                    match code {
                        KeyCode::Char(c) => app.input.push(c),
                        KeyCode::Backspace => {
                            app.input.pop();
                        }
                        KeyCode::Enter => {
                            if username.is_none() {
                                username = Some(app.input.clone());
                                app.input.clear();
                            }
                        }
                        KeyCode::Esc => {
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    restore_terminal()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}
