use process_execution_protocol::MAX_FRAME_BYTES;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

const MAX_RECEIPTS: usize = 128;
const MAX_BYTES: usize = 256 * 1024 * 1024;

pub enum Admission {
    New,
    Existing,
    Conflict,
    Full,
}

struct Receipt {
    fingerprint: [u8; 32],
    response: Option<Arc<str>>,
}

/// Reservations let every accepted request retain its response, even offline.
#[derive(Default)]
pub struct Receipts {
    entries: HashMap<String, Receipt>,
    bytes: usize,
}

impl Receipts {
    pub fn accept(&mut self, id: &str, fingerprint: [u8; 32], busy: bool) -> Admission {
        if let Some(receipt) = self.entries.get(id) {
            return if receipt.fingerprint == fingerprint {
                Admission::Existing
            } else {
                Admission::Conflict
            };
        }
        if busy || self.entries.len() >= MAX_RECEIPTS || self.bytes + MAX_FRAME_BYTES > MAX_BYTES {
            return Admission::Full;
        }
        self.bytes += MAX_FRAME_BYTES;
        self.entries.insert(
            id.to_owned(),
            Receipt {
                fingerprint,
                response: None,
            },
        );
        Admission::New
    }

    pub fn complete(&mut self, id: &str, response: String) {
        let receipt = self
            .entries
            .get_mut(id)
            .expect("accepted request has a receipt");
        debug_assert!(receipt.response.is_none() && response.len() <= MAX_FRAME_BYTES);
        self.bytes = self.bytes - MAX_FRAME_BYTES + response.len();
        receipt.response = Some(response.into());
    }

    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    pub fn response(&self, id: &str) -> Option<Arc<str>> {
        self.entries.get(id).and_then(|r| r.response.clone())
    }

    pub fn next_response(&self, sent: &HashSet<String>) -> Option<(String, Arc<str>)> {
        self.entries.iter().find_map(|(id, r)| {
            if sent.contains(id) {
                None
            } else {
                r.response.clone().map(|text| (id.clone(), text))
            }
        })
    }

    pub fn acknowledge(&mut self, id: &str) {
        if let Some(response) = self.response(id) {
            self.bytes -= response.len();
            self.entries.remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_requests_and_acks_preserve_pending_work() {
        let mut receipts = Receipts::default();
        assert!(matches!(
            receipts.accept("one", [1; 32], false),
            Admission::New
        ));
        assert!(matches!(
            receipts.accept("one", [1; 32], true),
            Admission::Existing
        ));
        assert!(matches!(
            receipts.accept("one", [2; 32], false),
            Admission::Conflict
        ));
        receipts.acknowledge("one");
        assert!(receipts.contains("one"));
        receipts.complete("one", "response".into());
        assert_eq!(receipts.response("one").as_deref(), Some("response"));
        receipts.acknowledge("one");
        assert!(!receipts.contains("one"));
        assert_eq!(receipts.bytes, 0);
    }

    #[test]
    fn capacity_reserves_responses_and_never_evicts_them() {
        let mut receipts = Receipts::default();
        for i in 0..MAX_BYTES / MAX_FRAME_BYTES {
            assert!(matches!(
                receipts.accept(&i.to_string(), [0; 32], false),
                Admission::New
            ));
        }
        assert!(matches!(
            receipts.accept("extra", [0; 32], false),
            Admission::Full
        ));
        for i in 0..MAX_BYTES / MAX_FRAME_BYTES {
            receipts.complete(&i.to_string(), "result".into());
        }
        for i in MAX_BYTES / MAX_FRAME_BYTES..MAX_RECEIPTS {
            let id = i.to_string();
            assert!(matches!(
                receipts.accept(&id, [0; 32], false),
                Admission::New
            ));
            receipts.complete(&id, "result".into());
        }
        assert!(matches!(
            receipts.accept("extra", [0; 32], false),
            Admission::Full
        ));
        assert_eq!(receipts.response("0").as_deref(), Some("result"));
        receipts.acknowledge("0");
        assert!(matches!(
            receipts.accept("extra", [0; 32], false),
            Admission::New
        ));
    }
}
