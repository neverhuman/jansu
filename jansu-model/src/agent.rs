// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::fmt;

/// A structured diagnostic for agent-facing error reports.
///
/// Encapsulates a machine-readable error code, the operation purpose, a human-readable
/// reason, common fixes, documentation URL, and a repair hint so that both agents and
/// operators can quickly triage and resolve issues.
#[derive(Clone, Debug)]
pub struct AgentException {
    pub code: &'static str,
    pub purpose: &'static str,
    pub reason: String,
    pub common_fixes: &'static [&'static str],
    pub docs_url: &'static str,
    pub repair_hint: &'static str,
}

impl AgentException {
    pub fn new(
        code: &'static str,
        purpose: &'static str,
        reason: impl Into<String>,
        common_fixes: &'static [&'static str],
        docs_url: &'static str,
        repair_hint: &'static str,
    ) -> Self {
        Self {
            code,
            purpose,
            reason: reason.into(),
            common_fixes,
            docs_url,
            repair_hint,
        }
    }
}

impl fmt::Display for AgentException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {}: {} (see {})",
            self.code, self.purpose, self.reason, self.docs_url
        )
    }
}
