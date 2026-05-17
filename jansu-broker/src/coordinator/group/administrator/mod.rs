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

mod formed;
mod forming;
#[cfg(test)]
mod tests;

use std::{
    collections::BTreeMap,
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{Arc, LazyLock, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use jansu_sans_io::{
    Body, ErrorCode,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use jansu_storage::{GroupDetail, GroupMember, GroupState, Storage, UpdateError, Version};
use opentelemetry::{KeyValue, metrics::Counter};
use tokio::time::{Duration, sleep};
use tracing::{debug, info};

use crate::{Error, METER, Result};

use super::{Coordinator, OffsetCommit};

const PAUSE_MS: u128 = 3_000;

fn timeout_millis(timeout_ms: i32) -> u128 {
    u128::from(timeout_ms.max(0).unsigned_abs())
}

fn member_timed_out(last_contact: Option<SystemTime>, timeout_ms: i32, now: SystemTime) -> bool {
    last_contact
        .map(|last_contact| now.duration_since(last_contact).unwrap_or(Duration::ZERO))
        .inspect(|duration| {
            debug!("since last contact: {}ms", duration.as_millis());
        })
        .is_some_and(|duration| duration.as_millis() > timeout_millis(timeout_ms))
}

static COORDINATOR_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("jansu_group_coordinator_requests")
        .with_description("consumer group coordinator requests")
        .build()
});

#[async_trait]
pub trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    #[allow(clippy::too_many_arguments)]
    async fn join(
        self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body);

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body);

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body);

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body);

    #[allow(clippy::too_many_arguments)]
    async fn offset_commit(
        self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body);

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body);
}

#[derive(Clone, Debug)]
pub enum Wrapper<O> {
    Forming(Inner<O, Forming>),
    Formed(Inner<O, Formed>),
}

impl<O> PartialEq for Wrapper<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Forming(sinner), Self::Forming(oinner)) => sinner == oinner,
            (Self::Formed(sinner), Self::Formed(oinner)) => sinner == oinner,
            _ => false,
        }
    }
}

impl<O> Hash for Wrapper<O>
where
    O: Storage,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Forming(inner) => {
                state.write_u8(3);
                inner.hash(state)
            }
            Self::Formed(inner) => {
                state.write_u8(5);
                inner.hash(state)
            }
        }
    }
}

impl<O> From<Inner<O, Forming>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Forming>) -> Self {
        Self::Forming(value)
    }
}

impl<O> From<Inner<O, Formed>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Formed>) -> Self {
        Self::Formed(value)
    }
}

impl<O> From<&Wrapper<O>> for GroupDetail
where
    O: Storage,
{
    fn from(value: &Wrapper<O>) -> Self {
        match value {
            Wrapper::Forming(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                members,
                generation_id,
                state,
                skip_assignment,
                inception,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
                members: members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            GroupMember {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: *generation_id,
                skip_assignment: *skip_assignment,
                inception: *inception,
                state: GroupState::Forming {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                },
            },
            Wrapper::Formed(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                members,
                generation_id,
                state,
                skip_assignment,
                inception,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
                members: members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            GroupMember {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: *generation_id,
                skip_assignment: *skip_assignment,
                inception: *inception,
                state: GroupState::Formed {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                    assignments: state.assignments.clone(),
                },
            },
        }
    }
}

impl<O> Wrapper<O>
where
    O: Storage,
{
    pub fn with_storage_group_detail(storage: O, gd: GroupDetail) -> Self {
        match gd.state {
            GroupState::Forming {
                protocol_type,
                protocol_name,
                mut leader,
            } => {
                if let Some(ref leader_id) = leader
                    && !gd
                        .members
                        .iter()
                        .any(|(member_id, _)| member_id == leader_id)
                {
                    _ = leader.take();
                }

                Self::Forming(Inner {
                    session_timeout_ms: gd.session_timeout_ms,
                    rebalance_timeout_ms: gd.rebalance_timeout_ms,
                    members: gd
                        .members
                        .iter()
                        .map(|(id, member)| {
                            (
                                id.to_owned(),
                                Member {
                                    join_response: member.join_response.clone(),
                                    last_contact: member.last_contact,
                                },
                            )
                        })
                        .collect(),
                    generation_id: gd.generation_id,
                    state: Forming {
                        protocol_type,
                        protocol_name,
                        leader,
                    },
                    storage,
                    skip_assignment: gd.skip_assignment,
                    inception: gd.inception,
                })
            }
            GroupState::Formed {
                protocol_type,
                protocol_name,
                leader,
                assignments,
            } => Self::Formed(Inner {
                session_timeout_ms: gd.session_timeout_ms,
                rebalance_timeout_ms: gd.rebalance_timeout_ms,
                members: gd
                    .members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            Member {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: gd.generation_id,
                state: Formed {
                    protocol_type,
                    protocol_name,
                    leader,
                    assignments,
                },
                storage,
                skip_assignment: gd.skip_assignment,
                inception: gd.inception,
            }),
        }
    }

    pub fn generation_id(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.generation_id,
            Self::Formed(inner) => inner.generation_id,
        }
    }

    pub fn inception(&self) -> SystemTime {
        match self {
            Self::Forming(inner) => inner.inception,
            Self::Formed(inner) => inner.inception,
        }
    }

    pub fn session_timeout_ms(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.session_timeout_ms,
            Self::Formed(inner) => inner.session_timeout_ms,
        }
    }

    pub fn rebalance_timeout_ms(&self) -> Option<i32> {
        match self {
            Self::Forming(inner) => inner.rebalance_timeout_ms,
            Self::Formed(inner) => inner.rebalance_timeout_ms,
        }
    }

    pub fn protocol_type(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_type.as_deref(),
            Self::Formed(inner) => Some(inner.state.protocol_type.as_str()),
        }
    }

    pub fn protocol_name(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_name.as_deref(),
            Self::Formed(inner) => Some(inner.state.protocol_name.as_str()),
        }
    }

    pub fn leader(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.leader.as_deref(),
            Self::Formed(inner) => Some(inner.state.leader.as_str()),
        }
    }

    pub fn skip_assignment(&self) -> Option<&bool> {
        match self {
            Self::Forming(inner) => inner.skip_assignment.as_ref(),
            Self::Formed(inner) => inner.skip_assignment.as_ref(),
        }
    }

    fn members(&self) -> Vec<JoinGroupResponseMember> {
        match self {
            Self::Forming(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),

            Self::Formed(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),
        }
    }

    fn is_forming(&self) -> bool {
        matches!(self, Self::Forming(..))
    }

    #[cfg(test)]
    fn assignments(&self) -> Option<BTreeMap<String, Bytes>> {
        match self {
            Self::Forming(..) => None,
            Self::Formed(inner) => Some(inner.state.assignments.clone()),
        }
    }

    fn missed_heartbeat(self, group_id: &str, now: SystemTime) -> Self {
        debug!(?group_id, ?now);

        match self {
            Self::Forming(mut inner) => {
                if inner.missed_heartbeat(group_id, now) {
                    inner.generation_id += 1;
                    inner.inception = now;

                    if inner.state.leader.is_none() {
                        inner.state.leader = inner.members.keys().next().cloned();
                    }
                }

                Self::Forming(inner)
            }
            Self::Formed(mut inner) => {
                if inner.missed_heartbeat(group_id, now) {
                    info!("missed heartbeat for {group_id} in {}", inner.generation_id);

                    let leader = if inner.members.contains_key(&inner.state.leader) {
                        Some(inner.state.leader.clone())
                    } else {
                        inner.members.keys().next().cloned()
                    };

                    Self::Forming(Inner {
                        session_timeout_ms: inner.session_timeout_ms,
                        rebalance_timeout_ms: inner.rebalance_timeout_ms,
                        members: inner.members,
                        generation_id: inner.generation_id + 1,
                        state: Forming {
                            protocol_type: Some(inner.state.protocol_type),
                            protocol_name: Some(inner.state.protocol_name),
                            leader,
                        },
                        storage: inner.storage,
                        skip_assignment: inner.skip_assignment,
                        inception: now,
                    })
                } else {
                    Self::Formed(inner)
                }
            }
        }
    }
}

#[async_trait]
impl<O> Group for Wrapper<O>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Wrapper<O>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Wrapper<O>;
    type OffsetFetchState = Wrapper<O>;

    #[allow(clippy::too_many_arguments)]
    async fn join(
        self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Self::Forming(inner) => {
                let (state, body) = inner
                    .join(
                        now,
                        client_id,
                        group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocols,
                        reason,
                    )
                    .await;
                (state.into(), body)
            }

            Self::Formed(inner) => {
                let (state, body) = inner
                    .join(
                        now,
                        client_id,
                        group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocols,
                        reason,
                    )
                    .await;
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .sync(
                        now,
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await;
                (state, body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .sync(
                        now,
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn offset_commit(self, now: SystemTime, detail: &OffsetCommit<'_>) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.offset_commit(now, detail).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.offset_commit(now, detail).await;
                (state.into(), body)
            }
        }
    }

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }
        }
    }
}

type WrapperMap<O> = Arc<Mutex<BTreeMap<String, (Wrapper<O>, Option<Version>)>>>;

#[derive(Clone, Debug)]
pub struct Controller<O> {
    storage: O,
    wrappers: WrapperMap<O>,
}

impl<O> Controller<O>
where
    O: Storage + Clone,
{
    pub fn with_storage(storage: O) -> Result<Self> {
        Ok(Self {
            storage,
            wrappers: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }
}

#[async_trait]
impl<O> Coordinator for Controller<O>
where
    O: Storage + Clone,
{
    async fn join(
        &mut self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        debug!(
            ?client_id,
            ?group_id,
            ?session_timeout_ms,
            ?rebalance_timeout_ms,
            ?member_id,
            ?group_instance_id,
            ?protocol_type,
            ?protocols,
            ?reason,
        );

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join")]);

        let started_at = SystemTime::now();

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_loop")]);

            let now = SystemTime::now();

            let (mut original, version) =
                self.wrappers
                    .lock()
                    .map(|mut wrappers| match wrappers.remove(group_id) {
                        Some(existing) => existing,
                        None => {
                            debug!(?iteration, ?group_id);

                            let inner = Inner {
                                session_timeout_ms,
                                rebalance_timeout_ms,
                                members: Default::default(),
                                generation_id: -1,
                                state: Forming::default(),
                                skip_assignment: Some(false),
                                storage: self.storage.clone(),
                                inception: SystemTime::now(),
                            };

                            (Wrapper::Forming(inner), None)
                        }
                    })?;

            original = original.missed_heartbeat(group_id, now);

            if iteration == 0
                && !member_id.is_empty()
                && original.leader().is_some_and(|leader| leader != member_id)
                && group_instance_id.is_none()
            {
                debug!(?member_id);
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_follower_pause")]);
                sleep(Duration::from_millis(PAUSE_MS as u64)).await;
            }

            debug!(?group_id, ?original, ?version, ?iteration);

            let (updated, body) = original
                .join(
                    now,
                    client_id,
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                    reason,
                )
                .await;

            debug!(group_id, ?updated, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&updated), version)
                .await
            {
                Ok(version) => {
                    let elapsed = SystemTime::now()
                        .duration_since(started_at)
                        .map(|duration| duration.as_millis())
                        .unwrap_or(0);

                    debug!(
                        group_id,
                        ?version,
                        iteration,
                        elapsed,
                        is_forming = updated.is_forming()
                    );

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (updated, Some(version)))
                    })?;

                    if group_instance_id.is_some() && elapsed < PAUSE_MS {
                        let pause = PAUSE_MS.saturating_sub(elapsed);
                        debug!(pause);

                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "join_group_instance_pause")]);
                        sleep(Duration::from_millis(pause as u64)).await;

                        iteration += 1;
                        continue;
                    } else {
                        return Ok(body);
                    }
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(group_id, ?current, ?version, iteration);

                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    });

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    async fn sync(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        debug!(
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id,
            ?protocol_type,
            ?protocol_name,
            ?assignments
        );

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync")]);

        let started_at = SystemTime::now();

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_loop")]);

            let now = SystemTime::now();

            let (mut original, version) =
                self.wrappers
                    .lock()
                    .map(|mut wrappers| match wrappers.remove(group_id) {
                        Some(existing) => existing,
                        None => (Wrapper::Forming(Inner::new(self.storage.clone())), None),
                    })?;

            debug!(?group_id, ?original, ?version, ?iteration);

            original = original.missed_heartbeat(group_id, now);

            let (updated, body) = original
                .sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocol_name,
                    assignments,
                )
                .await;

            debug!(group_id, ?updated, ?version, iteration,);
            match self
                .storage
                .update_group(group_id, GroupDetail::from(&updated), version)
                .await
            {
                Ok(version) => {
                    let elapsed = SystemTime::now()
                        .duration_since(started_at)
                        .map(|duration| duration.as_millis())
                        .unwrap_or(0);

                    debug!(
                        group_id,
                        ?version,
                        iteration,
                        elapsed,
                        is_forming = updated.is_forming()
                    );

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (updated, Some(version)))
                    })?;

                    if group_instance_id.is_some() && elapsed < PAUSE_MS {
                        let pause = PAUSE_MS.saturating_sub(elapsed);
                        debug!(pause);

                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "sync_group_instance_pause")]);
                        sleep(Duration::from_millis(pause as u64)).await;

                        iteration += 1;
                        continue;
                    } else {
                        return Ok(body);
                    }
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    async fn leave(
        &mut self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        debug!(?group_id, ?member_id, ?members);

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave")]);

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_loop")]);

            let (wrapper, version) =
                self.wrappers
                    .lock()
                    .map(|mut wrappers| match wrappers.remove(group_id) {
                        Some(existing) => existing,
                        None => (Wrapper::Forming(Inner::new(self.storage.clone())), None),
                    })?;

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper.leave(now, group_id, member_id, members).await;
            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (wrapper, Some(version)))
                    })?;

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    async fn offset_commit(&mut self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        debug!(?offset_commit);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit")]);

        let group_id = offset_commit.group_id;
        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit_loop")]);

            let (wrapper, version) =
                self.wrappers
                    .lock()
                    .map(|mut wrappers| match wrappers.remove(group_id) {
                        Some(existing) => existing,
                        None => (Wrapper::Forming(Inner::new(self.storage.clone())), None),
                    })?;

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper.offset_commit(now, &offset_commit).await;
            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (wrapper, Some(version)))
                    })?;

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "offset_commit_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    async fn offset_fetch(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        debug!(?group_id, ?topics, ?groups, ?require_stable);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_fetch")]);

        let now = SystemTime::now();
        let wrapper = Wrapper::Forming(Inner::new(self.storage.clone()))
            .missed_heartbeat(group_id.unwrap_or(""), now);
        let (_wrapper, body) = wrapper
            .offset_fetch(now, group_id, topics, groups, require_stable)
            .await;
        Ok(body)
    }

    async fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat")]);

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_loop")]);

            let (wrapper, version) =
                self.wrappers
                    .lock()
                    .map(|mut wrappers| match wrappers.remove(group_id) {
                        Some(existing) => existing,
                        None => (Wrapper::Forming(Inner::new(self.storage.clone())), None),
                    })?;

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, now);

            let (wrapper, body) = wrapper
                .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                .await;

            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (wrapper, Some(version)))
                    })?;

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Forming {
    pub(super) protocol_type: Option<String>,
    pub(super) protocol_name: Option<String>,
    pub(super) leader: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Formed {
    pub(super) protocol_type: String,
    pub(super) protocol_name: String,
    pub(super) leader: String,
    pub(super) assignments: BTreeMap<String, Bytes>,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Member {
    pub(super) join_response: JoinGroupResponseMember,
    pub(super) last_contact: Option<SystemTime>,
}

#[derive(Clone, Debug)]
pub struct Inner<O, S> {
    pub(super) session_timeout_ms: i32,
    pub(super) rebalance_timeout_ms: Option<i32>,
    pub(super) members: BTreeMap<String, Member>,
    pub(super) generation_id: i32,
    pub(super) state: S,
    pub(super) storage: O,
    pub(super) skip_assignment: Option<bool>,
    pub(super) inception: SystemTime,
}

impl<O, S> PartialEq for Inner<O, S>
where
    S: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.session_timeout_ms == other.session_timeout_ms
            && self.rebalance_timeout_ms == other.rebalance_timeout_ms
            && self.members == other.members
            && self.generation_id == other.generation_id
            && self.state == other.state
            && self.skip_assignment == other.skip_assignment
            && self.inception == other.inception
    }
}

impl<O, S> Hash for Inner<O, S>
where
    S: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.session_timeout_ms.hash(state);
        self.rebalance_timeout_ms.hash(state);
        self.members.hash(state);
        self.generation_id.hash(state);
        self.state.hash(state);
        self.skip_assignment.hash(state);
        self.inception.hash(state);
    }
}

impl<O, S> Inner<O, S>
where
    O: Storage,
    S: Debug,
{
    fn member_id_for_group_instance(&self, group_instance_id: &str) -> Option<&str> {
        self.members.iter().find_map(|(member_id, member)| {
            member
                .join_response
                .group_instance_id
                .as_deref()
                .is_some_and(|existing| existing == group_instance_id)
                .then_some(member_id.as_str())
        })
    }

    fn has_fenced_instance(&self, member_id: &str, group_instance_id: Option<&str>) -> bool {
        !member_id.is_empty()
            && group_instance_id.is_some_and(|group_instance_id| {
                self.member_id_for_group_instance(group_instance_id)
                    .is_some_and(|existing_member_id| existing_member_id != member_id)
            })
    }

    fn offset_commit_error_code(&self, detail: &OffsetCommit<'_>) -> Option<ErrorCode> {
        detail
            .generation_id_or_member_epoch
            .and_then(|generation_id| {
                if generation_id < 0 || generation_id == self.generation_id {
                    None
                } else if generation_id < self.generation_id
                    && detail
                        .member_id
                        .is_some_and(|member_id| !member_id.is_empty())
                {
                    Some(ErrorCode::RebalanceInProgress)
                } else {
                    Some(ErrorCode::IllegalGeneration)
                }
            })
    }
}

impl<O> Inner<O, Forming>
where
    O: Storage,
{
    fn protocol_mismatch(&self, protocol_type: Option<&str>, protocol_name: Option<&str>) -> bool {
        protocol_type.is_some_and(|protocol_type| {
            self.state
                .protocol_type
                .as_deref()
                .is_some_and(|existing| existing != protocol_type)
        }) || protocol_name.is_some_and(|protocol_name| {
            self.state
                .protocol_name
                .as_deref()
                .is_some_and(|existing| existing != protocol_name)
        })
    }
}

impl<O> Inner<O, Formed>
where
    O: Storage,
{
    fn protocol_mismatch(&self, protocol_type: Option<&str>, protocol_name: Option<&str>) -> bool {
        protocol_type.is_some_and(|protocol_type| self.state.protocol_type != protocol_type)
            || protocol_name.is_some_and(|protocol_name| self.state.protocol_name != protocol_name)
    }
}

impl<O> Inner<O, PhantomData<Forming>>
where
    O: Storage,
{
    pub fn new(storage: O) -> Inner<O, Forming> {
        Inner {
            session_timeout_ms: Default::default(),
            rebalance_timeout_ms: Default::default(),
            members: Default::default(),
            generation_id: -1,
            state: Forming::default(),
            skip_assignment: Some(false),
            storage,
            inception: SystemTime::now(),
        }
    }
}
