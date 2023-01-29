#[macro_use]
extern crate log;

use bytes::BytesMut;
use speedy::Writable;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    time::Instant,
};

type Key = u64;
type Db = Arc<Mutex<HashMap<u64, Vec<u8>>>>;
type Expiration = Instant;

const KEY_EXPIRE_SEC: u64 = 30;

async fn serve() {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let db2 = db.clone();
    let db3 = db.clone();

    let expirations: Arc<Mutex<BTreeMap<Expiration, Key>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let expirations2 = expirations.clone();

    let last_snapshot = Arc::new(Mutex::new(None));

    tokio::spawn(async move {
        let expirations_local = expirations2.clone();
        let db_local = db2.clone();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = Instant::now();
            loop {
                let (expiration, key) = match expirations_local.lock().unwrap().iter().next() {
                    Some((&expiration, &key)) => (expiration, key),
                    None => break,
                };

                if expiration > now {
                    tokio::time::sleep_until(expiration).await;
                }

                let mut db_guard = db_local.lock().unwrap();
                db_guard.remove(&key);
                drop(db_guard);
                let mut expirations_guard = expirations_local.lock().unwrap();
                expirations_guard.remove(&expiration);
                drop(expirations_guard);
            }
        }
    });

    tokio::spawn(async move {
        let db_local = db3.clone();
        let last_snapshot_local = last_snapshot.clone();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let new_snap_filename = format!("snapshot-{}.db", fastrand::u64(..));
            let mut file = File::create(&new_snap_filename).await.unwrap();

            let data = {
                let mut db_guard = db_local.lock().unwrap();
                db_guard.shrink_to_fit();
                db_guard.write_to_vec().unwrap()
            };

            file.write_all(&data).await.unwrap();
            file.sync_all().await.unwrap();

            let last_snap_fname = last_snapshot_local.lock().unwrap().clone();
            if let Some(fname) = last_snap_fname {
                tokio::fs::remove_file(fname).await.unwrap();
            }
            *last_snapshot_local.lock().unwrap() = Some(new_snap_filename);
        }
    });

    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();
    info!("Listening on {:?}", listener.local_addr().unwrap());

    let semaphore = Arc::new(Semaphore::new(2048));

    loop {
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let (mut stream, _) = listener.accept().await.unwrap();
        let local_db = db.clone();
        let expirations2 = expirations.clone();

        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(4 * 1024);
            loop {
                let len = stream.read_buf(&mut buf).await.unwrap();

                if len == 0 {
                    break;
                } else {
                    let key = fastrand::u64(..);

                    local_db.lock().unwrap().insert(key, buf.to_vec());
                    let expire = Instant::now() + Duration::from_secs(KEY_EXPIRE_SEC);
                    expirations2.lock().unwrap().insert(expire, key);

                    stream.write(b"$-1\r\n").await.unwrap();
                }

                buf.clear();
            }
            drop(permit);
        });
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    serve().await;
}
