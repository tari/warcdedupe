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
    fn add(&mut self, target_uri: String, date: String, digest: Digest) -> Option<(String, &str)>;
}

/// Basic implementation of a `ResponseLog`.
///
/// This implementation will only deduplicate identical responses from the same
/// URL because it currently lacks strong protection against hash collisions.
pub struct InMemoryResponseLog<Digest>(HashMap<(Digest, String), String>);

impl<Digest> Default for InMemoryResponseLog<Digest> {
    fn default() -> Self {
        InMemoryResponseLog(HashMap::new())
    }
}

impl<Digest> InMemoryResponseLog<Digest> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<Digest: Eq + Hash> ResponseLog<Digest> for InMemoryResponseLog<Digest> {
    fn add(&mut self, target_uri: String, date: String, digest: Digest) -> Option<(String, &str)> {
        // The below alternate implementation would be nice because it allows us to return a
        // ref to the key, but borrowck doesn't understand that insert() can't alias the result
        // of get_key_value() so we're stuck with copying for now.
        /*
        if let Some(((_, uri), v)) = self.0.get_key_value(&key) {
            return Some((uri, v));
        }
        self.0.insert(key, date);
        None
         */
        use std::collections::hash_map::Entry;

        match self.0.entry((digest, target_uri.clone())) {
            Entry::Occupied(value) => {
                // subtle borrowck point: get() returns a ref to the entry, whereas
                // into_mut() is the only method on an Entry that returns a ref to the
                // underlying map: we need a ref to the map (not a local) in order
                // to return it.
                Some((target_uri, value.into_mut()))
            }
            Entry::Vacant(slot) => {
                slot.insert(date);
                None
            }
        }
    }
}
