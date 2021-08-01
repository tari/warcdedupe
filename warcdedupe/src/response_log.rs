use std::collections::HashMap;
use std::hash::Hash;

/// Generic log for tracking seen responses.
///
/// While the basic version of this is an in-memory mapping, for very large
/// archives an alternate index may be desired.
pub trait ResponseLog<Digest: Eq> {
    /// Record a response for the specified URI with digest, captured from
    /// the given URL at the given time.
    ///
    /// Returns None if that response has not been seen before, otherwise the
    /// ID of the original record, its URI, and date.
    fn add<Id: Into<String>, Uri: Into<String>, Date: Into<String>, IDigest: Into<Digest>>(
        &mut self,
        record_id: Id,
        target_uri: Option<Uri>,
        date: Option<Date>,
        digest: IDigest,
    ) -> Option<(&str, Option<&str>, Option<&str>)>;
}

/// Basic implementation of a `ResponseLog`.
pub struct InMemoryResponseLog<Digest>(HashMap<Digest, RecordInfo>);

struct RecordInfo {
    record_id: String,
    target_uri: Option<String>,
    date: Option<String>,
}

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
    fn add<T, U, V, W>(
        &mut self,
        record_id: T,
        target_uri: Option<U>,
        date: Option<V>,
        digest: W,
    ) -> Option<(&str, Option<&str>, Option<&str>)>
    where
        T: Into<String>,
        U: Into<String>,
        V: Into<String>,
        W: Into<Digest>,
    {
        use std::collections::hash_map::Entry;

        match self.0.entry(digest.into()) {
            Entry::Occupied(value) => {
                // subtle borrowck point: get() returns a ref to the entry, whereas
                // into_mut() is the only method on an Entry that returns a ref to the
                // underlying map: we need a ref to the map (not a local) in order
                // to return it.
                let RecordInfo {
                    record_id,
                    target_uri,
                    date,
                } = value.into_mut();
                Some((record_id, target_uri.as_deref(), date.as_deref()))
            }
            Entry::Vacant(slot) => {
                slot.insert(RecordInfo {
                    record_id: record_id.into(),
                    target_uri: target_uri.map(Into::into),
                    date: date.map(Into::into),
                });
                None
            }
        }
    }
}
