// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
    time::Duration,
};

use cached::stores::ExpiringSizedCache;
use object_store::{ObjectStore, path::Path};

use cache_entry::CacheEntry;

mod cache_entry;
mod metrics;
mod object_store_impl;

#[derive(Clone)]
pub(super) struct Cache<O> {
    entries: Arc<Mutex<ExpiringSizedCache<Path, CacheEntry>>>,
    object_store: O,
    retention: Duration,
}

impl<O> Debug for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("retention", &self.retention)
            .finish()
    }
}

impl<O> Display for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish()
    }
}

impl<O> Cache<O>
where
    O: ObjectStore,
{
    pub(super) fn new(object_store: O, retention: Duration) -> Self {
        let entries = Arc::new(Mutex::new(ExpiringSizedCache::new(retention)));

        Self {
            entries,
            object_store,
            retention,
        }
    }

    #[cfg(test)]
    fn inner(&self) -> &O {
        &self.object_store
    }
}

#[cfg(test)]
mod tests;
