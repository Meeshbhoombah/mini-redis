use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

enum Command {
    Get {
        key: String, 
    },
    Set {
        key: String,
        value: Bytes,
    },
}

async fn process(socket: TcpStream) {
    use mini_redis::Command::{self, Get, Set};
    use std::collections::HashMap;

    let mut db = HashMap::new();

    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("Ok", to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd)
        };

        connection.write_frame(&response).await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    let tx2 = tx.clone();

    // Spawn two tasks, one gets a key, the other sets a key
    let t1 = tokio::spawn(async move {
        let cmd = Command::Get {
            key: "hello".to_string(),
        };

        tx.send(cmd).await.unwrap();
    });

    let t2 = tokio::spawn(async move {
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
        };

        tx2.send(cmd).await.unwrap();
    });

    use mini_redis::client;

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv.await {
            use Command::*;

            match cmd {
                Get { key } => {
                    client.get(&key).await;
                },
                Set { key, val} {
                    client.set(&key).await;
                }
            }
        };

    });

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println("Listening...");
    
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let db = db.clone();

        tokio::spawn(async move {
            process(socket, db).await;
        });
    }

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}
