use std::collections::HashMap;
use std::hash::Hash;

/// Generic log for tracking seen responses.
///
/// While the basic version of this is an in-memory mapping, for very large
/// archives an alternate index may be desired.
pub trait ResponseLog<Digest: Eq> {
    /// Record a response for the specified URI with SHA-1 digest, captured at
    /// the specified date.
    ///
    /// Returns None if that response has not been seen before, otherwise the
    /// URI and date that it was first seen.
    fn add<'a, 'b>(
        &'a mut self,
        target_uri: String,
        date: String,
        digest: Digest,
    ) -> Option<(String, &'a str)>;
}

/// Basic implementation of a `ResponseLog`.
///
/// This implementation will only deduplicate identical responses from the same
/// URL because it currently lacks strong protection against hash collisions.
pub struct InMemoryResponseLog<Digest>(HashMap<(Digest, String), String>);

impl<Digest> InMemoryResponseLog<Digest> {
    pub fn new() -> Self {
        InMemoryResponseLog(HashMap::new())
    }
}

impl<Digest: Eq + Hash> ResponseLog<Digest> for InMemoryResponseLog<Digest> {
    fn add<'a, 'b>(
        &'a mut self,
        target_uri: String,
        date: String,
        digest: Digest,
    ) -> Option<(String, &'a str)> {
        // API limitation: we need to take an owned copy of the target URI to look up, even if we
        // don't need to insert since K: Borrow<Q> doesn't allow us to return a ref to a tuple that
        // the HashMap owns. But we also can't get a ref to the key to return without
        // HashMap::get_key_value (#49347), so we can at least reuse it.
        let key = (digest, target_uri.to_owned());
        if self.0.contains_key(&key) {
            let first_seen = self.0.get(&key).unwrap();
            Some((key.1, first_seen))
        } else {
            self.0.insert(key, date);
            None
        }
    }
}
