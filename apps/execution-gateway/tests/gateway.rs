use execution_gateway::{AppState, config::Config, crypto, db, jobs, router, webhooks};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Method};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{
    collections::HashSet,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

const ADMIN: &str = "test-administrator-key-at-least-32-characters";

struct Database {
    name: String,
    admin_url: String,
    url: String,
    pool: PgPool,
}
impl Database {
    async fn new() -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must identify a PostgreSQL server where disposable databases may be created");
        let admin = PgPool::connect(&admin_url).await.unwrap();
        let name = format!("egw_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = url::Url::parse(&admin_url).unwrap();
        url.set_path(&format!("/{name}"));
        let url = url.to_string();
        let pool = db::pool(&url).await.unwrap();
        db::MIGRATOR.run(&pool).await.unwrap();
        db::MIGRATOR.run(&pool).await.unwrap();
        admin.close().await;
        Self {
            name,
            admin_url,
            url,
            pool,
        }
    }
    fn state(&self) -> AppState {
        AppState::new(
            self.pool.clone(),
            Config {
                database_url: self.url.clone(),
                admin_key: ADMIN.into(),
                encryption_key: [42; 32],
                listen: "127.0.0.1:0".parse().unwrap(),
                webhook_origins: HashSet::new(),
                retention_days: 7,
            },
        )
        .unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let url = self.admin_url.clone();
        let name = self.name.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let admin = PgPool::connect(&url).await.unwrap();
                sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&admin)
                    .await
                    .unwrap();
                admin.close().await;
            });
        })
        .join()
        .unwrap();
    }
}

struct Gateway {
    child: Child,
    base: String,
    client: Client,
}
impl Gateway {
    async fn start(db: &Database) -> Self {
        Self::start_at(db, "127.0.0.1:0".parse().unwrap()).await
    }
    async fn start_at(db: &Database, address: std::net::SocketAddr) -> Self {
        let reservation = std::net::TcpListener::bind(address).unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        let child = Command::new(env!("CARGO_BIN_EXE_execution-gateway"))
            .arg("serve")
            .env("DATABASE_URL", &db.url)
            .env("ADMIN_API_KEY", ADMIN)
            .env("ENCRYPTION_KEY", hex::encode([42; 32]))
            .env("LISTEN_ADDR", addr.to_string())
            .env("WEBHOOK_ALLOWED_ORIGINS", "")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let gateway = Self {
            child,
            base: format!("http://{addr}"),
            client: Client::new(),
        };
        let started = Instant::now();
        loop {
            if gateway
                .client
                .get(format!("{}/readyz", gateway.base))
                .send()
                .await
                .is_ok_and(|v| v.status().is_success())
            {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "gateway startup timed out"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        gateway
    }
    async fn stop(&mut self) {
        #[cfg(unix)]
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        #[cfg(windows)]
        self.child.kill().unwrap();
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                if cfg!(unix) {
                    assert!(status.success(), "gateway did not shut down cleanly");
                }
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "gateway shutdown timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut req = self
            .client
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(token);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req.send().await.unwrap();
        assert_eq!(response.headers()["cache-control"], "no-store");
        let status = response.status().as_u16();
        let bytes = response.bytes().await.unwrap();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| json!({"unparsed": String::from_utf8_lossy(&bytes)}))
        };
        (status, value)
    }
    async fn user(&self, name: &str) -> (Uuid, String, String) {
        let (status, value) = self
            .request(
                Method::POST,
                "/v1/admin/users",
                ADMIN,
                Some(json!({"name":name,"callbackUrl":"https://callback.example/events"})),
            )
            .await;
        assert_eq!(status, 201, "{value}");
        (
            uuid(&value["user"]["id"]),
            value["key"]["secret"].as_str().unwrap().into(),
            value["webhookSecret"].as_str().unwrap().into(),
        )
    }
    async fn machine(&self, key: &str) -> (Uuid, Uuid, String) {
        let (status, machine) = self
            .request(
                Method::POST,
                "/v1/machines",
                key,
                Some(json!({"name":"Test machine"})),
            )
            .await;
        assert_eq!(status, 201, "{machine}");
        let id = uuid(&machine["machineId"]);
        let installation = Uuid::new_v4();
        let token = machine["registrationToken"].as_str().unwrap();
        let (status, registration) = self
            .request(
                Method::POST,
                &format!("/v1/machines/{id}/register"),
                token,
                Some(json!({"installationId":installation})),
            )
            .await;
        assert_eq!(status, 200, "{registration}");
        assert_eq!(
            self.request(
                Method::POST,
                &format!("/v1/machines/{id}/register"),
                token,
                Some(json!({"installationId":installation}))
            )
            .await
            .0,
            401
        );
        (
            id,
            installation,
            registration["credential"].as_str().unwrap().into(),
        )
    }
    async fn job(&self, key: &str, machine: Uuid, idempotency: &str, request: Value) -> Uuid {
        let (status, job) = self
            .request(
                Method::POST,
                "/v1/jobs",
                key,
                Some(json!({"machineId":machine,"idempotencyKey":idempotency,"request":request})),
            )
            .await;
        assert_eq!(status, 202, "{job}");
        uuid(&job["id"])
    }
    async fn terminal(&self, key: &str, job: Uuid) -> Value {
        let started = Instant::now();
        loop {
            let (status, value) = self
                .request(
                    Method::GET,
                    &format!("/v1/jobs/{job}/wait?timeoutMs=5000"),
                    key,
                    None,
                )
                .await;
            assert_eq!(status, 200);
            if ["succeeded", "failed", "unknown"].contains(&value["status"].as_str().unwrap()) {
                return value;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "job did not finish: {value}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn uuid(value: &Value) -> Uuid {
    Uuid::parse_str(value.as_str().unwrap()).unwrap()
}

struct MachineSocket {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    generation: Uuid,
}
impl MachineSocket {
    async fn connect(
        gateway: &Gateway,
        machine: Uuid,
        installation: Uuid,
        credential: &str,
        generation: Uuid,
        legacy: bool,
    ) -> Self {
        Self::connect_with_recovery(
            gateway,
            machine,
            installation,
            credential,
            generation,
            legacy,
            false,
        )
        .await
    }
    async fn connect_with_recovery(
        gateway: &Gateway,
        machine: Uuid,
        installation: Uuid,
        credential: &str,
        generation: Uuid,
        legacy: bool,
        recovery: bool,
    ) -> Self {
        let resource = if legacy { "hosts" } else { "machines" };
        let mut request = format!(
            "{}/v1/{resource}/{machine}/connect",
            gateway.base.replace("http://", "ws://")
        )
        .into_client_request()
        .unwrap();
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {credential}").parse().unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket.send(Message::Text(json!({"type":"hello","protocol_version":process_execution_protocol::VERSION,"installation_id":installation,"host_id":machine,"request_recovery":recovery,
            "runtime":{"generation_id":generation,"default_shell":{"executable":"sh","kind":"sh"},"pty":true,"pipe_interrupt":true,"terminal_interrupt":true,"filesystem":{"max_read_bytes":5242880,"max_write_bytes":5242880,"conditional_mutations":true,"atomic_replace":true}},
            "binary":{"binary":"fixture","version":"0.1.0","platform":"test","architecture":"test"}}).to_string().into())).await.unwrap();
        let welcome = timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(welcome.to_text().unwrap()).unwrap()["type"],
            "welcome"
        );
        Self { socket, generation }
    }
    async fn message(&mut self) -> Value {
        timeout(Duration::from_secs(10), async {
            loop {
                match self.socket.next().await.unwrap().unwrap() {
                    Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                    Message::Ping(bytes) => self.socket.send(Message::Pong(bytes)).await.unwrap(),
                    _ => {}
                }
            }
        })
        .await
        .unwrap()
    }
    async fn request(&mut self) -> Value {
        let value = self.message().await;
        assert_eq!(value["type"], "request", "{value}");
        value["request"].clone()
    }
    async fn receipt(&mut self, kind: &str, job: Uuid) {
        self.socket
            .send(Message::Text(
                json!({"type":kind,"request_id":job,"generation_id":self.generation})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    async fn reply(&mut self, request: &Value, result: Value) {
        self.socket.send(Message::Text(json!({"type":"response","response":{"protocol_version":process_execution_protocol::VERSION,"request_id":request["request_id"],"generation_id":self.generation,"status":"ok","result":result}}).to_string().into())).await.unwrap();
    }
    async fn idle(&mut self) {
        let result = timeout(Duration::from_millis(350), async {
            loop {
                match self.socket.next().await {
                    Some(Ok(Message::Ping(bytes))) => {
                        self.socket.send(Message::Pong(bytes)).await.unwrap()
                    }
                    Some(Ok(Message::Text(text))) => {
                        panic!("unexpected automatic operation: {text}")
                    }
                    Some(Err(error)) => panic!("socket failed: {error}"),
                    None => panic!("socket closed"),
                    _ => {}
                }
            }
        })
        .await;
        assert!(result.is_err());
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops its own database"]
async fn api_registration_jobs_and_isolation() {
    let db = Database::new().await;
    let gateway = Gateway::start(&db).await;
    assert_eq!(
        gateway
            .request(Method::GET, "/v1/admin/users", "bad", None)
            .await
            .0,
        401
    );
    let (user, key, secret) = gateway.user("Owner").await;
    let (_, other, _) = gateway.user("Other").await;
    assert_eq!(
        gateway.request(Method::GET, "/v1/me", ADMIN, None).await.0,
        401
    );
    assert_eq!(
        gateway
            .request(Method::GET, "/v1/admin/users", &key, None)
            .await
            .0,
        401
    );
    let users = gateway
        .request(Method::GET, "/v1/admin/users?limit=1", ADMIN, None)
        .await
        .1;
    assert_eq!(users["data"].as_array().unwrap().len(), 1);
    let cursor = users["nextCursor"].as_str().unwrap();
    let second = gateway
        .request(
            Method::GET,
            &format!("/v1/admin/users?limit=1&cursor={cursor}"),
            ADMIN,
            None,
        )
        .await
        .1;
    assert_ne!(users["data"][0]["id"], second["data"][0]["id"]);
    assert!(!users.to_string().contains(&key));
    let stored: (String, Vec<u8>) = sqlx::query_as("SELECT k.key_hash,u.webhook_secret_encrypted FROM user_api_keys k JOIN users u ON u.id=k.user_id WHERE u.id=$1").bind(user).fetch_one(&db.pool).await.unwrap();
    assert_ne!(stored.0, key);
    assert_eq!(crypto::decrypt(&[42; 32], user, &stored.1).unwrap(), secret);
    assert_eq!(
        gateway.request(Method::GET, "/v1/me", &key, None).await.1["webhookPayloadVersion"],
        2
    );
    let patch = gateway
        .request(
            Method::PATCH,
            "/v1/me",
            &key,
            Some(json!({"name":"Updated","callbackUrl":"https://callback.example/new"})),
        )
        .await;
    assert_eq!(patch.0, 200);
    assert_eq!(patch.1["name"], "Updated");
    assert_eq!(patch.1["webhookPayloadVersion"], 2);
    assert_eq!(
        gateway
            .request(
                Method::PATCH,
                "/v1/me",
                &key,
                Some(json!({"webhookPayloadVersion":1}))
            )
            .await
            .1["webhookPayloadVersion"],
        1
    );
    assert_eq!(
        gateway
            .request(
                Method::PATCH,
                "/v1/me",
                &key,
                Some(json!({"webhookPayloadVersion":2}))
            )
            .await
            .1["webhookPayloadVersion"],
        2
    );
    assert_eq!(
        gateway
            .request(
                Method::PATCH,
                "/v1/me",
                &key,
                Some(json!({"webhookPayloadVersion":3}))
            )
            .await
            .0,
        400
    );
    assert_eq!(
        gateway
            .request(
                Method::PATCH,
                "/v1/me",
                &key,
                Some(json!({"enabled":false}))
            )
            .await
            .0,
        400
    );
    assert_eq!(
        gateway
            .request(Method::POST, "/v1/me/webhook-secret/rotate", &key, None)
            .await
            .0,
        200
    );
    let issued = gateway
        .request(
            Method::POST,
            &format!("/v1/admin/users/{user}/keys"),
            ADMIN,
            Some(json!({"name":"second"})),
        )
        .await
        .1;
    let second_key = issued["secret"].as_str().unwrap();
    assert_eq!(
        gateway
            .request(Method::GET, "/v1/me", second_key, None)
            .await
            .0,
        200
    );
    assert_eq!(
        gateway
            .request(
                Method::DELETE,
                &format!(
                    "/v1/admin/users/{user}/keys/{}",
                    issued["id"].as_str().unwrap()
                ),
                ADMIN,
                None
            )
            .await
            .0,
        204
    );
    assert_eq!(
        gateway
            .request(Method::GET, "/v1/me", second_key, None)
            .await
            .0,
        401
    );
    assert_eq!(
        gateway
            .request(
                Method::GET,
                &format!("/v1/admin/users/{user}/keys"),
                ADMIN,
                None
            )
            .await
            .1["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let (machine, installation, credential) = gateway.machine(&key).await;
    assert_eq!(
        gateway
            .request(
                Method::GET,
                &format!("/v1/machines/{machine}"),
                &other,
                None
            )
            .await
            .0,
        404
    );
    let offline = gateway.request(Method::POST, "/v1/jobs", &key, Some(json!({"machineId":machine,"idempotencyKey":"offline","request":{"operation":"runtime.info"}}))).await;
    assert_eq!(offline.1["error"]["code"], "machine_offline");
    let generation = Uuid::new_v4();
    let mut socket = MachineSocket::connect(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        true,
    )
    .await;
    assert_eq!(
        gateway
            .request(Method::GET, &format!("/v1/machines/{machine}"), &key, None)
            .await
            .1["online"],
        true
    );
    let list = gateway
        .request(Method::GET, "/v1/machines", &key, None)
        .await
        .1;
    assert!(!list.to_string().contains(&credential));
    assert!(list["data"][0].get("credentialHash").is_none());

    let profile = gateway
        .request(
            Method::PATCH,
            "/v1/me",
            &key,
            Some(json!({"callbackUrl":""})),
        )
        .await;
    assert_eq!(profile.0, 200);
    assert_eq!(profile.1["callbackUrl"], "");

    let input = json!({"mode":"parallel","operations":[{"request_id":"one","operation":"runtime.info"},{"request_id":"two","operation":"execution.list","params":{}}]});
    let futures = (0..8).map(|_| gateway.job(&key, machine, "batch", input.clone()));
    let ids = futures_util::future::join_all(futures).await;
    assert!(ids.iter().all(|id| *id == ids[0]));
    let request = socket.request().await;
    assert_eq!(request["request_id"], ids[0].to_string());
    assert_eq!(request["expected_generation_id"], generation.to_string());
    assert_eq!(request["mode"], "parallel");
    let timed = gateway
        .request(
            Method::GET,
            &format!("/v1/jobs/{}/wait?timeoutMs=20", ids[0]),
            &key,
            None,
        )
        .await;
    assert_eq!(timed.0, 200);
    assert!(["dispatching", "waiting_response"].contains(&timed.1["status"].as_str().unwrap()));
    let client = gateway.client.clone();
    let url = format!("{}/v1/jobs/{}/wait?timeoutMs=5000", gateway.base, ids[0]);
    let wait_key = key.clone();
    let waiting = tokio::spawn(async move {
        let response = client.get(url).bearer_auth(wait_key).send().await.unwrap();
        assert_eq!(response.status(), 200);
        response.json::<Value>().await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    socket.reply(&request, json!({"succeeded":false,"results":[{"request_id":"one","status":"ok","result":{}},{"request_id":"two","status":"error","error":{"code":"invalid_argument","message":"fixture"}}]})).await;
    let job = timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job["status"], "failed");
    assert_eq!(
        job["response"]["result"]["results"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        gateway
            .request(Method::GET, &format!("/v1/jobs/{}", ids[0]), &other, None)
            .await
            .0,
        404
    );
    assert_eq!(
        gateway
            .request(
                Method::GET,
                &format!("/v1/jobs/{}/wait?timeoutMs=1", ids[0]),
                &other,
                None
            )
            .await
            .0,
        404
    );
    for query in ["timeoutMs=0", "timeoutMs=300001", "extra=1"] {
        assert_eq!(
            gateway
                .request(
                    Method::GET,
                    &format!("/v1/jobs/{}/wait?{query}", ids[0]),
                    &key,
                    None
                )
                .await
                .0,
            400
        );
    }
    let deliveries = gateway
        .request(
            Method::GET,
            &format!("/v1/webhook-deliveries?jobId={}", ids[0]),
            &key,
            None,
        )
        .await
        .1;
    assert!(deliveries["data"].as_array().unwrap().is_empty());
    let conflict = gateway.request(Method::POST, "/v1/jobs", &key, Some(json!({"machineId":machine,"idempotencyKey":"batch","request":{"operation":"runtime.info"}}))).await;
    assert_eq!(conflict.0, 409);
    let bad_generation = gateway.request(Method::POST, "/v1/jobs", &key, Some(json!({"machineId":machine,"idempotencyKey":"old","request":{"operation":"runtime.info","expected_generation_id":Uuid::new_v4()}}))).await;
    assert_eq!(bad_generation.1["error"]["code"], "generation_mismatch");
    assert_eq!(gateway.request(Method::POST, "/v1/jobs", &key, Some(json!({"machineId":machine,"idempotencyKey":"shutdown","request":{"operation":"runtime.shutdown"}}))).await.0, 400);

    gateway
        .request(
            Method::PATCH,
            "/v1/me",
            &key,
            Some(json!({"callbackUrl":"https://callback.example/events"})),
        )
        .await;

    let running = gateway.job(&key, machine, "start", json!({"operation":"execution.start","params":{"start_id":"start-1","command":{"type":"program","executable":"fixture"}}})).await;
    let request = socket.request().await;
    socket.reply(&request, json!({"execution":{"handle":{"id":Uuid::new_v4(),"generation_id":generation},"state":"running"}})).await;
    assert_eq!(gateway.terminal(&key, running).await["status"], "succeeded");
    socket.idle().await;
    let deliveries = gateway
        .request(
            Method::GET,
            &format!("/v1/webhook-deliveries?jobId={running}"),
            &key,
            None,
        )
        .await
        .1;
    assert_eq!(deliveries["data"].as_array().unwrap().len(), 1);
    let delivery_id = uuid(&deliveries["data"][0]["id"]);
    let detail = gateway
        .request(
            Method::GET,
            &format!("/v1/webhook-deliveries/{delivery_id}"),
            &key,
            None,
        )
        .await;
    assert_eq!(detail.0, 200);
    let payload = &detail.1["payload"];
    assert_eq!(payload["type"], "job.succeeded");
    assert_eq!(payload["jobId"], running.to_string());
    assert_eq!(payload["machineId"], machine.to_string());
    assert_eq!(
        payload
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>(),
        HashSet::from([
            "completedAt",
            "eventId",
            "jobId",
            "machineId",
            "schemaVersion",
            "type"
        ])
    );
    assert_eq!(payload["schemaVersion"], 2);
    assert_eq!(
        gateway
            .request(
                Method::GET,
                &format!("/v1/webhook-deliveries/{delivery_id}"),
                &other,
                None
            )
            .await
            .0,
        404
    );

    sqlx::query(
        "UPDATE job_requests SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
    )
    .bind(ids[0])
    .execute(&db.pool)
    .await
    .unwrap();
    jobs::housekeeping_once(&db.state()).await.unwrap();
    assert_eq!(
        gateway.terminal(&key, ids[0]).await["requestStatus"],
        "expired"
    );
    assert_eq!(gateway.job(&key, machine, "batch", input).await, ids[0]);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM webhook_deliveries WHERE job_id=$1")
        .bind(ids[0])
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);

    assert_eq!(
        gateway
            .request(
                Method::PATCH,
                &format!("/v1/machines/{machine}"),
                &key,
                Some(json!({"enabled":false}))
            )
            .await
            .0,
        200
    );
    assert_eq!(gateway.request(Method::POST, "/v1/jobs", &key, Some(json!({"machineId":machine,"idempotencyKey":"disabled","request":{"operation":"runtime.info"}}))).await.1["error"]["code"], "machine_disabled");
    socket.idle().await;
    gateway
        .request(
            Method::PATCH,
            &format!("/v1/machines/{machine}"),
            &key,
            Some(json!({"enabled":true})),
        )
        .await;
    gateway
        .request(
            Method::PATCH,
            &format!("/v1/admin/users/{user}"),
            ADMIN,
            Some(json!({"enabled":false})),
        )
        .await;
    assert_eq!(
        gateway.request(Method::GET, "/v1/me", &key, None).await.0,
        401
    );
    socket.idle().await;
    gateway
        .request(
            Method::PATCH,
            &format!("/v1/admin/users/{user}"),
            ADMIN,
            Some(json!({"enabled":true})),
        )
        .await;
    assert_eq!(
        gateway
            .request(
                Method::DELETE,
                &format!("/v1/machines/{machine}"),
                &key,
                None
            )
            .await
            .0,
        204
    );
    assert_eq!(
        gateway
            .request(
                Method::DELETE,
                &format!("/v1/machines/{machine}"),
                &key,
                None
            )
            .await
            .0,
        204
    );
    assert_eq!(
        gateway
            .request(Method::GET, &format!("/v1/machines/{machine}"), &key, None)
            .await
            .0,
        404
    );
    assert_eq!(gateway.terminal(&key, running).await["status"], "succeeded");
    drop(gateway);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops its own database"]
async fn disconnect_restart_and_credential_replacement_do_not_replay_work() {
    let db = Database::new().await;
    let gateway = Gateway::start(&db).await;
    let (_, key, _) = gateway.user("Owner").await;
    let (machine, installation, credential) = gateway.machine(&key).await;
    let generation = Uuid::new_v4();
    let mut socket = MachineSocket::connect(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        false,
    )
    .await;
    let job = gateway
        .job(&key, machine, "lost", json!({"operation":"runtime.info"}))
        .await;
    socket.request().await;
    socket.socket.close(None).await.unwrap();
    assert_eq!(gateway.terminal(&key, job).await["status"], "unknown");
    let mut socket = MachineSocket::connect(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        false,
    )
    .await;
    socket.idle().await;
    assert_eq!(
        gateway
            .job(&key, machine, "lost", json!({"operation":"runtime.info"}))
            .await,
        job
    );
    socket.idle().await;
    let crash_job = gateway
        .job(&key, machine, "crash", json!({"operation":"runtime.info"}))
        .await;
    socket.request().await;
    let mut duplicate = Command::new(env!("CARGO_BIN_EXE_execution-gateway"))
        .arg("serve")
        .env("DATABASE_URL", &db.url)
        .env("ADMIN_API_KEY", ADMIN)
        .env("ENCRYPTION_KEY", hex::encode([42; 32]))
        .env("LISTEN_ADDR", "127.0.0.1:0")
        .env("WEBHOOK_ALLOWED_ORIGINS", "")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    assert!(!duplicate.wait().unwrap().success());
    drop(gateway);
    drop(socket);
    let gateway = Gateway::start(&db).await;
    assert_eq!(gateway.terminal(&key, crash_job).await["status"], "unknown");
    let mut socket = MachineSocket::connect(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        true,
    )
    .await;
    socket.idle().await;
    let first = gateway
        .request(
            Method::POST,
            &format!("/v1/machines/{machine}/registration"),
            &key,
            None,
        )
        .await
        .1;
    let second = gateway
        .request(
            Method::POST,
            &format!("/v1/machines/{machine}/registration"),
            &key,
            None,
        )
        .await
        .1;
    assert_eq!(
        gateway
            .request(
                Method::POST,
                &format!("/v1/machines/{machine}/register"),
                first["registrationToken"].as_str().unwrap(),
                Some(json!({"installationId":installation}))
            )
            .await
            .0,
        401
    );
    let replacement = gateway
        .request(
            Method::POST,
            &format!("/v1/machines/{machine}/register"),
            second["registrationToken"].as_str().unwrap(),
            Some(json!({"installationId":installation})),
        )
        .await;
    assert_eq!(replacement.0, 200);
    assert_ne!(replacement.1["credential"], credential);
    let mut old = format!(
        "{}/v1/machines/{machine}/connect",
        gateway.base.replace("http://", "ws://")
    )
    .into_client_request()
    .unwrap();
    old.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    assert!(tokio_tungstenite::connect_async(old).await.is_err());
    drop(gateway);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL and cargo build --workspace --bins"]
async fn daemon_registers_and_recovers_jobs_after_gateway_restart() {
    use base64::Engine;
    use std::io::Write;
    let db = Database::new().await;
    let mut gateway = Gateway::start(&db).await;
    let (_, key, _) = gateway.user("Real daemon").await;
    let (status, registration) = gateway
        .request(
            Method::POST,
            "/v1/machines",
            &key,
            Some(json!({"name":"Registered daemon"})),
        )
        .await;
    assert_eq!(status, 201);
    let machine = uuid(&registration["machineId"]);
    let token = registration["registrationToken"].as_str().unwrap();
    let profile = tempfile::tempdir().unwrap();
    let daemon_name = if cfg!(windows) {
        "process-execution-host-daemon.exe"
    } else {
        "process-execution-host-daemon"
    };
    let daemon_path =
        std::path::Path::new(env!("CARGO_BIN_EXE_execution-gateway")).with_file_name(daemon_name);
    assert!(
        daemon_path.is_file(),
        "build workspace binaries before this integration test"
    );
    let mut configure = Command::new(&daemon_path)
        .arg("--state-dir")
        .arg(profile.path())
        .arg("register")
        .arg("--gateway-url")
        .arg(&gateway.base)
        .arg("--machine-id")
        .arg(machine.to_string())
        .arg("--allow-insecure-loopback")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    configure
        .stdin
        .take()
        .unwrap()
        .write_all(token.as_bytes())
        .unwrap();
    let output = configure.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(token));
    let saved: Value =
        serde_json::from_slice(&std::fs::read(profile.path().join("credential.json")).unwrap())
            .unwrap();
    assert!(saved["token"].as_str().unwrap().starts_with("egm_"));
    assert_ne!(saved["token"], token);
    let identity: Uuid =
        serde_json::from_slice(&std::fs::read(profile.path().join("identity.json")).unwrap())
            .unwrap();
    let registered: Uuid = sqlx::query_scalar("SELECT installation_id FROM machines WHERE id=$1")
        .bind(machine)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(identity, registered);
    let mut daemon = Process(
        Command::new(&daemon_path)
            .arg("--state-dir")
            .arg(profile.path())
            .args(["run", "--allow-insecure-loopback"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let start = Instant::now();
    loop {
        if gateway
            .request(Method::GET, &format!("/v1/machines/{machine}"), &key, None)
            .await
            .1["online"]
            == true
        {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let info = gateway
        .job(&key, machine, "info", json!({"operation":"runtime.info"}))
        .await;
    let info = gateway.terminal(&key, info).await;
    assert_eq!(info["status"], "succeeded");
    assert_eq!(
        info["response"]["result"]["runtime"]["filesystem"]["conditional_mutations"],
        true
    );
    let file = profile.path().join("gateway-files").join("binary.dat");
    let write = gateway
        .job(
            &key,
            machine,
            "file-write",
            json!({
                "operation":"filesystem.write_file",
                "params":{
                    "mutation_id":"gateway-file-write",
                    "path":file,
                    "data_base64":"AP8B",
                    "create_parent_directories":true,
                    "precondition":{"type":"missing"}
                }
            }),
        )
        .await;
    let write = gateway.terminal(&key, write).await;
    assert_eq!(write["status"], "succeeded", "{write}");
    let expected_sha256 = write["response"]["result"]["sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(write["response"]["result"]["disposition"], "applied");
    let read = gateway
        .job(
            &key,
            machine,
            "file-read",
            json!({"operation":"filesystem.read_file","params":{"path":file}}),
        )
        .await;
    let read = gateway.terminal(&key, read).await;
    assert_eq!(read["status"], "succeeded", "{read}");
    assert_eq!(read["response"]["result"]["data_base64"], "AP8B");
    assert_eq!(read["response"]["result"]["sha256"], expected_sha256);
    let remove = gateway
        .job(
            &key,
            machine,
            "file-remove",
            json!({
                "operation":"filesystem.remove_file",
                "params":{
                    "mutation_id":"gateway-file-remove",
                    "path":file,
                    "precondition":{"type":"sha256","sha256":expected_sha256}
                }
            }),
        )
        .await;
    let remove = gateway.terminal(&key, remove).await;
    assert_eq!(remove["status"], "succeeded", "{remove}");
    assert_eq!(remove["response"]["result"]["disposition"], "applied");
    let batch = gateway.job(&key, machine, "real-batch", json!({"mode":"parallel","operations":[
        {"request_id":"version","operation":"execution.start","params":{"start_id":"version-command","command":{"type":"program","executable":env!("CARGO_BIN_EXE_execution-gateway"),"args":["version"]},"wait_ms":1000}},
        {"request_id":"info","operation":"runtime.info"}
    ]})).await;
    let result = gateway.terminal(&key, batch).await;
    assert_eq!(result["status"], "succeeded", "{result}");
    let observation = &result["response"]["result"]["results"][0]["result"];
    let chunks = observation["output"].as_array().unwrap();
    let mut bytes = Vec::new();
    for chunk in chunks {
        bytes.extend(
            base64::engine::general_purpose::STANDARD
                .decode(chunk["data_base64"].as_str().unwrap())
                .unwrap(),
        );
    }
    assert!(String::from_utf8_lossy(&bytes).contains("execution-gateway"));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 5);
    // An actual batch completes while its daemon reconnects to the restarted gateway.
    let source = profile.path().join("child.rs");
    let program = profile
        .path()
        .join(if cfg!(windows) { "once.exe" } else { "once" });
    let marker = profile.path().join("runs.txt");
    std::fs::write(&source, r#"use std::io::Write; fn main() {
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(std::env::args().nth(1).unwrap()).unwrap();
        file.write_all(b"once\n").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));
        println!("recovered");
    }"#).unwrap();
    assert!(
        Command::new("rustc")
            .arg(&source)
            .arg("-o")
            .arg(&program)
            .status()
            .unwrap()
            .success()
    );
    let recovering = gateway.job(&key, machine, "recover-real-batch", json!({"mode":"sequential","operations":[
        {"request_id":"once","operation":"execution.start","params":{"start_id":"run-once","command":{"type":"program","executable":program,"args":[marker]},"wait_ms":10000}},
        {"request_id":"info","operation":"runtime.info"}
    ]})).await;
    timeout(Duration::from_secs(5), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id=$1")
                .bind(recovering)
                .fetch_one(&db.pool)
                .await
                .unwrap();
            if status == "waiting_response" {
                break;
            }
            assert_ne!(status, "succeeded", "batch completed before restart test");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let address = gateway.base.trim_start_matches("http://").parse().unwrap();
    gateway.stop().await;
    assert!(
        daemon.0.try_wait().unwrap().is_none(),
        "gateway shutdown stopped the daemon"
    );
    gateway = Gateway::start_at(&db, address).await;
    let result = gateway.terminal(&key, recovering).await;
    assert_eq!(result["status"], "succeeded", "{result}");
    assert_eq!(
        result["response"]["result"]["results"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "once\n");
    drop(daemon);
    drop(gateway);
}

struct CallbackRequest {
    headers: String,
    body: Vec<u8>,
}
struct Callback {
    url: String,
    certificate: Vec<u8>,
    requests: tokio::sync::mpsc::Receiver<CallbackRequest>,
    replies: tokio::sync::mpsc::Sender<u16>,
    task: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}
impl Callback {
    async fn start() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_rustls::rustls::pki_types::pem::PemObject;
        use tokio_rustls::{
            TlsAcceptor,
            rustls::{
                ServerConfig,
                pki_types::{CertificateDer, PrivateKeyDer},
            },
        };
        let directory = tempfile::tempdir().unwrap();
        let certificate_path = directory.path().join("cert.pem");
        let key_path = directory.path().join("key.pem");
        let status = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
            ])
            .arg("-keyout")
            .arg(&key_path)
            .arg("-out")
            .arg(&certificate_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let certificate = std::fs::read(&certificate_path).unwrap();
        let certs = CertificateDer::pem_slice_iter(&certificate)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        let acceptor = TlsAcceptor::from(std::sync::Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "https://localhost:{}/events",
            listener.local_addr().unwrap().port()
        );
        let (request_tx, requests) = tokio::sync::mpsc::channel(8);
        let (replies, mut reply_rx) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let mut stream = acceptor.accept(stream).await.unwrap();
                let mut bytes = Vec::new();
                let header_end;
                loop {
                    let mut block = [0; 4096];
                    let n = stream.read(&mut block).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                    if let Some(at) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        header_end = at + 4;
                        break;
                    }
                }
                let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                let len: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < header_end + len {
                    let mut block = [0; 4096];
                    let n = stream.read(&mut block).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                }
                request_tx
                    .send(CallbackRequest {
                        headers,
                        body: bytes[header_end..header_end + len].to_vec(),
                    })
                    .await
                    .unwrap();
                let status = reply_rx.recv().await.unwrap();
                stream.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nRetry-After: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                let _ = stream.shutdown().await;
            }
        });
        Self {
            url,
            certificate,
            requests,
            replies,
            task,
            _directory: directory,
        }
    }
    async fn received(&mut self) -> CallbackRequest {
        timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
impl Drop for Callback {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL and openssl; uses a trusted local HTTPS callback"]
async fn webhook_signatures_retry_redelivery_and_lease_recovery() {
    use tower::ServiceExt;
    let db = Database::new().await;
    let mut callback = Callback::start().await;
    let user = Uuid::new_v4();
    let machine = Uuid::new_v4();
    let job = Uuid::new_v4();
    let secret = crypto::token("whsec_");
    let key = crypto::token("egw_");
    sqlx::query("INSERT INTO users(id,name,callback_url,webhook_secret_encrypted) VALUES($1,'fixture',$2,$3)")
        .bind(user).bind(&callback.url).bind(crypto::encrypt(&[42;32], user, &secret).unwrap()).execute(&db.pool).await.unwrap();
    sqlx::query(
        "INSERT INTO user_api_keys(id,user_id,key_hash,key_prefix) VALUES($1,$2,$3,'fixture')",
    )
    .bind(Uuid::new_v4())
    .bind(user)
    .bind(crypto::hash(key.as_bytes()))
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO machines(id,user_id,name) VALUES($1,$2,'fixture')")
        .bind(machine)
        .bind(user)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO jobs(id,user_id,machine_id,idempotency_key,request_hash) VALUES($1,$2,$3,'fixture','fixture')").bind(job).bind(user).bind(machine).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO job_requests(job_id,request) VALUES($1,$2)")
        .bind(job)
        .bind(json!({"operation":"runtime.info"}))
        .execute(&db.pool)
        .await
        .unwrap();
    let mut state = db.state();
    std::sync::Arc::make_mut(&mut state.config)
        .webhook_origins
        .insert(
            url::Url::parse(&callback.url)
                .unwrap()
                .origin()
                .ascii_serialization(),
        );
    state.http = Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&callback.certificate).unwrap())
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    jobs::finish(
        &state,
        job,
        "failed",
        json!({"code":"fixture","message":"fixture"}),
    )
    .await
    .unwrap();
    let delivery: Uuid = sqlx::query_scalar("SELECT id FROM webhook_deliveries WHERE job_id=$1")
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap();

    let task = tokio::spawn({
        let state = state.clone();
        async move { webhooks::run_once(&state).await.unwrap() }
    });
    let request = callback.received().await;
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["eventId"], delivery.to_string());
    assert_eq!(body["jobId"], job.to_string());
    assert_eq!(body["machineId"], machine.to_string());
    assert_eq!(body["type"], "job.failed");
    assert_eq!(body["error"]["code"], "fixture");
    assert!(body["response"].is_null());
    assert!(body.get("schemaVersion").is_none());
    assert_eq!(
        body.as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>(),
        HashSet::from([
            "completedAt",
            "error",
            "eventId",
            "jobId",
            "machineId",
            "response",
            "type"
        ])
    );
    let header = |name: &str| {
        request
            .headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_owned())
            })
            .unwrap()
    };
    let timestamp = header("x-execution-gateway-timestamp");
    // Verify independently against the exact received bytes.
    use hmac::{Hmac, Mac};
    let mut hmac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    hmac.update(format!("{timestamp}.{delivery}.").as_bytes());
    hmac.update(&request.body);
    assert_eq!(
        header("x-execution-gateway-signature"),
        format!("v1={}", hex::encode(hmac.finalize().into_bytes()))
    );
    callback.replies.send(503).await.unwrap();
    assert!(task.await.unwrap());
    let status: String = sqlx::query_scalar("SELECT status FROM webhook_deliveries WHERE id=$1")
        .bind(delivery)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(status, "retry_wait");
    sqlx::query("UPDATE webhook_deliveries SET next_attempt_at=clock_timestamp() WHERE id=$1")
        .bind(delivery)
        .execute(&db.pool)
        .await
        .unwrap();
    let task = tokio::spawn({
        let state = state.clone();
        async move { webhooks::run_once(&state).await.unwrap() }
    });
    assert_eq!(callback.received().await.body, request.body);
    callback.replies.send(200).await.unwrap();
    assert!(task.await.unwrap());
    let status: String = sqlx::query_scalar("SELECT status FROM webhook_deliveries WHERE id=$1")
        .bind(delivery)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(status, "delivered");

    let response = router(state.clone())
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v1/webhook-deliveries/{delivery}/redeliver"))
                .header("Authorization", format!("Bearer {key}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    let repeat = router(state.clone())
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v1/webhook-deliveries/{delivery}/redeliver"))
                .header("Authorization", format!("Bearer {key}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(repeat.status(), 409);
    // Simulate a delivery worker disappearing after dispatching attempt 3.
    sqlx::query("UPDATE webhook_deliveries SET status='delivering',lease_token=$2,lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(delivery).bind(Uuid::new_v4()).execute(&db.pool).await.unwrap();
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts(id,delivery_id,attempt_number) VALUES($1,$2,3)",
    )
    .bind(Uuid::new_v4())
    .bind(delivery)
    .execute(&db.pool)
    .await
    .unwrap();
    let task = tokio::spawn({
        let state = state.clone();
        async move { webhooks::run_once(&state).await.unwrap() }
    });
    assert_eq!(callback.received().await.body, request.body);
    callback.replies.send(200).await.unwrap();
    assert!(task.await.unwrap());
    let (finished, error): (bool, Value) = sqlx::query_as("SELECT finished_at IS NOT NULL,error FROM webhook_delivery_attempts WHERE delivery_id=$1 AND attempt_number=3").bind(delivery).fetch_one(&db.pool).await.unwrap();
    assert!(finished);
    assert_eq!(error["code"], "lease_expired");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_delivery_attempts WHERE delivery_id=$1")
            .bind(delivery)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(count, 4);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let final_job: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id=$1")
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(final_job, "failed");
    state.pool.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops its own database"]
async fn request_recovery_is_generation_fenced_and_acknowledged_after_commit() {
    let db = Database::new().await;
    let mut gateway = Gateway::start(&db).await;
    let (_, key, _) = gateway.user("Recovery").await;
    let (machine, installation, credential) = gateway.machine(&key).await;
    let generation = Uuid::new_v4();
    let mut socket = MachineSocket::connect_with_recovery(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        false,
        true,
    )
    .await;
    let job = gateway
        .job(
            &key,
            machine,
            "accepted",
            json!({"operation":"runtime.info"}),
        )
        .await;
    let request = socket.request().await;
    socket.receipt("accepted", job).await;
    timeout(Duration::from_secs(5), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id=$1")
                .bind(job)
                .fetch_one(&db.pool)
                .await
                .unwrap();
            if status == "waiting_response" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Abrupt gateway death leaves its dispatch record intact.
    gateway.child.kill().unwrap();
    gateway.child.wait().unwrap();
    drop(socket);
    gateway = Gateway::start(&db).await;
    let mut socket = MachineSocket::connect_with_recovery(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        false,
        true,
    )
    .await;
    let recovery = socket.message().await;
    assert_eq!(recovery["type"], "recover");
    assert_eq!(recovery["request_id"], job.to_string());
    assert_eq!(recovery["generation_id"], generation.to_string());
    socket.receipt("accepted", job).await;
    socket.reply(&request, json!({"recovered":true})).await;
    let ack = socket.message().await;
    assert_eq!(ack["type"], "acknowledge");
    assert_eq!(ack["request_id"], job.to_string());
    let stored = gateway.terminal(&key, job).await;
    assert_eq!(stored["status"], "succeeded");
    assert_eq!(stored["response"]["result"]["recovered"], true);
    // Pretend the acknowledgement was lost: a duplicate result is acknowledged
    // after the original durable result, without a second notification.
    socket.reply(&request, json!({"recovered":true})).await;
    assert_eq!(socket.message().await, ack);
    let deliveries: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_deliveries WHERE job_id=$1")
            .bind(job)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(deliveries, 1);
    socket.idle().await;

    let missing = gateway
        .job(
            &key,
            machine,
            "missing",
            json!({"operation":"runtime.info"}),
        )
        .await;
    socket.request().await;
    gateway.stop().await;
    drop(socket);
    gateway = Gateway::start(&db).await;
    let mut socket = MachineSocket::connect_with_recovery(
        &gateway,
        machine,
        installation,
        &credential,
        generation,
        false,
        true,
    )
    .await;
    assert_eq!(socket.message().await["type"], "recover");
    socket.receipt("missing", missing).await;
    assert_eq!(
        gateway.terminal(&key, missing).await["error"]["code"],
        "receipt_missing"
    );
    socket.idle().await;

    let replaced = gateway
        .job(
            &key,
            machine,
            "replacement",
            json!({"operation":"runtime.info"}),
        )
        .await;
    socket.request().await;
    gateway.stop().await;
    drop(socket);
    gateway = Gateway::start(&db).await;
    let mut socket = MachineSocket::connect_with_recovery(
        &gateway,
        machine,
        installation,
        &credential,
        Uuid::new_v4(),
        false,
        true,
    )
    .await;
    assert_eq!(
        gateway.terminal(&key, replaced).await["error"]["code"],
        "runtime_replaced"
    );
    socket.idle().await;

    let late = gateway
        .job(
            &key,
            machine,
            "late-result",
            json!({"operation":"runtime.info"}),
        )
        .await;
    let request = socket.request().await;
    sqlx::query(
        "UPDATE jobs SET recovery_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(late)
    .execute(&db.pool)
    .await
    .unwrap();
    socket.receipt("accepted", late).await;
    socket.reply(&request, json!({"late":true})).await;
    assert_eq!(socket.message().await["type"], "acknowledge");
    let outcome = gateway.terminal(&key, late).await;
    assert_eq!(outcome["status"], "unknown");
    assert!(outcome["response"].is_null());

    let expired = gateway
        .job(
            &key,
            machine,
            "expired",
            json!({"operation":"runtime.info"}),
        )
        .await;
    socket.request().await;
    gateway.stop().await;
    drop(socket);
    sqlx::query(
        "UPDATE jobs SET recovery_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(expired)
    .execute(&db.pool)
    .await
    .unwrap();
    gateway = Gateway::start(&db).await;
    assert_eq!(gateway.terminal(&key, expired).await["status"], "unknown");
    drop(gateway);
}
