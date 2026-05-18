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

/// Represents whether an ACL grants or denies permissions
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Permission {
    /// Represents any permission which this client cannot understand,
    /// perhaps because this client predates the relevant protocol revision.
    #[default]
    Unknown = 0,

    /// In a filter, matches any permission.
    Any = 1,

    /// Disallows access
    Deny = 2,

    /// Grants access
    Allow = 3,
}

impl From<i8> for Permission {
    fn from(value: i8) -> Self {
        match value {
            1 => Permission::Any,
            2 => Permission::Deny,
            3 => Permission::Allow,

            _ => Permission::Unknown,
        }
    }
}

/// Represents an operation which an ACL grants or denies permission to perform.
///
/// Some operations imply other operations:
/// <ul>
/// <li>[`Allow`] [`All`] implies [`Allow`] everything
/// <li>[`Deny`] [`All`] implies [`Deny`] everything
///
/// <li>[`Allow`] [`Read`] implies [`Allow`] [`Describe`]
/// <li>[`Allow`] [`Write`] implies [`Allow`] [`Describe`]
/// <li>[`Allow`] [`Delete`] implies [`Allow`] [`Describe`]
///
/// <li>[`Allow`] [`Alter`] implies [`Allow`] [`Describe`]
///
/// <li>[`Allow`] [`AlterConfigs`] implies [`Allow`] [`DescribeConfigs`]
/// </ul>
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Operation {
    #[default]
    Unknown = 0,

    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
    CreateTokens = 13,
    DescribeTokens = 14,
    TwoPhaseCommit = 15,
}

impl From<i8> for Operation {
    fn from(value: i8) -> Self {
        match value {
            1 => Operation::Any,
            2 => Operation::All,
            3 => Operation::Read,
            4 => Operation::Write,
            5 => Operation::Create,
            6 => Operation::Delete,
            7 => Operation::Alter,
            8 => Operation::Describe,
            9 => Operation::ClusterAction,
            10 => Operation::DescribeConfigs,
            11 => Operation::AlterConfigs,
            12 => Operation::IdempotentWrite,
            13 => Operation::CreateTokens,
            14 => Operation::DescribeTokens,
            15 => Operation::TwoPhaseCommit,

            _ => Operation::Unknown,
        }
    }
}

/// Represents a type of resource which an ACL can be applied to.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Resource {
    /// Represents any ResourceType which this client cannot understand,
    /// perhaps because this client predates the relevant protocol revision.
    #[default]
    Unknown = 0,

    /// In a filter, matches any ResourceType
    Any = 1,

    /// A topic
    Topic = 2,

    /// A consumer group
    Group = 3,

    /// The whole cluster
    Cluster = 4,

    /// A transactional ID
    TransactionalId = 5,

    /// A token ID
    DelegationToken = 6,

    /// A user principal
    User = 7,
}

impl From<i8> for Resource {
    fn from(value: i8) -> Self {
        match value {
            1 => Resource::Any,
            2 => Resource::Topic,
            3 => Resource::Group,
            4 => Resource::Cluster,
            5 => Resource::TransactionalId,
            6 => Resource::DelegationToken,
            7 => Resource::User,

            _ => Resource::Unknown,
        }
    }
}
