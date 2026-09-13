use execution_gateway::{config, crypto, jobs};
use serde_json::json;
use uuid::Uuid;

#[test]
fn request_validation_preserves_protocol_semantics() {
    let normalized = jobs::normalize(json!({"operations":[
        {"request_id":"a","operation":"runtime.info"},
        {"request_id":"b","operation":"execution.list","params":{}}
    ]}))
    .unwrap();
    assert_eq!(normalized["mode"], "sequential");
    assert_eq!(normalized["operations"][1]["params"]["limit"], 50);
    let id = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let wire = jobs::wire_request(normalized, id, generation).unwrap();
    assert_eq!(wire.request_id, id.to_string());
    assert_eq!(wire.expected_generation_id, Some(generation));
    for invalid in [
        json!({"operations":[]}),
        json!({"operation":"runtime.shutdown"}),
        json!({"request_id":"client-controlled","operation":"runtime.info"}),
        json!({"operation":"runtime.info","operations":[]}),
        json!({"operations":[{"request_id":"a","operation":"runtime.info"},{"request_id":"a","operation":"runtime.info"}]}),
        json!({"operations":[{"request_id":"a","operations":[]}]}),
        json!({"mode":"unsupported","operations":[{"request_id":"a","operation":"runtime.info"}]}),
    ] {
        assert!(jobs::normalize(invalid).is_err());
    }
    let old = jobs::normalize(
        json!({"expected_generation_id":Uuid::new_v4(),"operation":"runtime.info"}),
    )
    .unwrap();
    assert!(jobs::wire_request(old, id, generation).is_err());
}

#[test]
fn secrets_are_random_and_encryption_is_bound_to_owner() {
    let owner = Uuid::new_v4();
    let key = [42; 32];
    let secret = crypto::token("whsec_");
    assert_ne!(secret, crypto::token("whsec_"));
    let mut sealed = crypto::encrypt(&key, owner, &secret).unwrap();
    assert_eq!(crypto::decrypt(&key, owner, &sealed).unwrap(), secret);
    assert!(crypto::decrypt(&key, Uuid::new_v4(), &sealed).is_err());
    assert!(crypto::decrypt(&[41; 32], owner, &sealed).is_err());
    sealed[15] ^= 1;
    assert!(crypto::decrypt(&key, owner, &sealed).is_err());
    assert!(crypto::decrypt(&key, owner, &[1, 2]).is_err());
}

#[test]
fn callback_configuration_does_not_allow_credentials_or_insecure_urls() {
    assert_eq!(config::callback_setting("").unwrap(), "");
    assert!(config::callback_url("https://callback.example/events").is_ok());
    for value in [
        "http://localhost/events",
        "https://user:secret@example.com/events",
        "https://example.com/events#fragment",
        "file:///tmp/callback",
    ] {
        assert!(config::callback_url(value).is_err());
    }
    assert!(config::name("  ".into()).is_err());
    assert_eq!(config::name(" Machine ".into()).unwrap(), "Machine");
}
