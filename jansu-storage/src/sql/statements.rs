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

//! Assembly of the embedded SQL statement table from its partitions.

// `include_sql!` is textually in scope for the `statements_*` child modules
// declared below it.
macro_rules! include_sql {
    ($e: expr) => {
        $crate::sql::remove_comments(include_str!($e))
    };
}

mod statements_0;
mod statements_1;
mod statements_2;
mod statements_3;

/// Collect every embedded SQL statement `(name, body)` pair.
pub(super) fn all() -> Vec<(&'static str, String)> {
    let mut mapping = statements_0::chunk_0();
    mapping.append(&mut statements_1::chunk_1());
    mapping.append(&mut statements_2::chunk_2());
    mapping.append(&mut statements_3::chunk_3());
    mapping
}
