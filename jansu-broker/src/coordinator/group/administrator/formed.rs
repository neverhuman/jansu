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

use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use jansu_sans_io::{
    Body, ErrorCode,
    heartbeat_response::HeartbeatResponse,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    leave_group_request::MemberIdentity,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    offset_fetch_response::OffsetFetchResponse,
    sync_group_request::SyncGroupRequestAssignment,
    sync_group_response::SyncGroupResponse,
};
use jansu_storage::Storage;
use tracing::debug;
use uuid::Uuid;


use super::OffsetCommit;

use super::*;

#[async_trait]
impl<O> Group for Inner<O, Formed>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Inner<O, Formed>;
    type HeartbeatState = Inner<O, Formed>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Inner<O, Formed>;
    type OffsetFetchState = Inner<O, Formed>;

    async fn join(
        mut self,
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
    ) -> (Self::JoinState, Body) {
        debug!(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            ?protocols,
            reason
        );

        let Some(protocols) = protocols else {
            debug!(join_outcome = ?ErrorCode::InvalidRequest);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InvalidRequest.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        };

        if self.has_fenced_instance(member_id, group_instance_id) {
            debug!(join_outcome = ?ErrorCode::FencedInstanceId);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::FencedInstanceId.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id.to_owned())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        }

        let Some(protocol) = protocols
            .iter()
            .find(|protocol| protocol.name == self.state.protocol_name)
        else {
            debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InconsistentGroupProtocol.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        };

        if member_id.is_empty() && group_instance_id.is_none() {
            let member_id = if let Some(client_id) = client_id {
                format!("{client_id}-{}", Uuid::new_v4())
            } else {
                format!("{}", Uuid::new_v4())
            };
            debug!(?member_id, join_outcome = ?ErrorCode::MemberIdRequired);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::MemberIdRequired.into())
                .generation_id(-1)
                .protocol_type(None)
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id.clone())
                .members(Some([].into()));

            _ = self.members.insert(
                member_id.clone(),
                Member {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id)
                        .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                        .metadata(protocol.metadata.clone()),
                    last_contact: Some(now),
                },
            );

            return (
                Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader.clone()),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: now,
                }
                .into(),
                join_group_response.into(),
            );
        }

        let member_id = group_instance_id.map_or(member_id.to_owned(), |group_instance_id| {
            if member_id.is_empty() {
                if let Some((member_id, _)) = self.members.iter().find(|(_, member)| {
                    member.join_response.group_instance_id.as_deref() == Some(group_instance_id)
                }) {
                    member_id.into()
                } else {
                    format!("{group_instance_id}-{}", Uuid::new_v4())
                }
            } else {
                member_id.into()
            }
        });

        debug!(?member_id, ?self.members);

        match self.members.get_mut(&member_id) {
            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) if *metadata == protocol.metadata => {
                debug!(
                    member_metadata = "existing",
                    member_id,
                    generation_id = self.generation_id
                );

                let state: Wrapper<O> = self.into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );
                    let protocol_type = state.protocol_type().map(ToOwned::to_owned);
                    let protocol_name = state.protocol_name().map(ToOwned::to_owned);

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(state.skip_assignment().map(ToOwned::to_owned))
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }

            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) => {
                debug!(
                    member_metadata = if group_instance_id.is_none() {"update"} else { "soft_update"},
                    member_id,
                    updated = ?protocol.metadata,
                    existing = ?metadata,
                );

                *metadata = protocol.metadata.clone();

                let state: Wrapper<O> = Inner {
                    generation_id: if group_instance_id.is_none() {
                        self.generation_id + 1
                    } else {
                        self.generation_id
                    },
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader.clone()),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: now,
                }
                .into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );
                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(self.skip_assignment)
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }

            None => {
                debug!(
                    member_metadata = "new",
                    member_id,
                    generation_id = self.generation_id + 1
                );

                _ = self.members.insert(
                    member_id.clone(),
                    Member {
                        join_response: JoinGroupResponseMember::default()
                            .member_id(member_id.to_string())
                            .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                            .metadata(protocol.metadata.clone()),
                        last_contact: Some(now),
                    },
                );

                let state: Wrapper<O> = Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader.clone()),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: now,
                }
                .into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );

                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(self.skip_assignment)
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }
        }
    }

    async fn sync(
        mut self,
        _now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        let _ = group_id;
        let _ = group_instance_id;
        let _ = protocol_type;
        let _ = protocol_name;
        let _ = assignments;

        if !self.members.contains_key(member_id) {
            debug!(sync_outcome = ?ErrorCode::UnknownMemberId);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        if self.protocol_mismatch(protocol_type, protocol_name) {
            debug!(sync_outcome = ?ErrorCode::InconsistentGroupProtocol);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InconsistentGroupProtocol.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            debug!(sync_outcome = ?ErrorCode::IllegalGeneration);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::IllegalGeneration.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        if generation_id < self.generation_id {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        let body = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(Some(self.state.protocol_type.clone()))
            .protocol_name(Some(self.state.protocol_name.clone()))
            .assignment(
                self.state
                    .assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or(Bytes::from_static(b"")),
            )
            .into();

        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = self.state.assignments.contains_key(member_id));

        (self, body)
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);

        if !self.members.contains_key(member_id) {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::UnknownMemberId.into())
                    .into(),
            );
        }

        if generation_id > self.generation_id {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::IllegalGeneration.into())
                    .into(),
            );
        }

        if self.missed_heartbeat(group_id, now) || (generation_id < self.generation_id) {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
                    .into(),
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        let body = HeartbeatResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .into();

        (self, body)
    }

    async fn leave(
        mut self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let _ = group_id;

        let leader_before = self.state.leader.clone();

        let members = if let Some(member_id) = member_id {
            vec![
                MemberResponse::default()
                    .member_id(member_id.to_owned())
                    .group_instance_id(None)
                    .error_code({
                        if self.members.remove(member_id).is_some() {
                            ErrorCode::None.into()
                        } else {
                            ErrorCode::UnknownMemberId.into()
                        }
                    }),
            ]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| {
                        MemberResponse::default()
                            .member_id(member.member_id.clone())
                            .group_instance_id(member.group_instance_id.clone())
                            .error_code({
                                if self.members.remove(&member.member_id).is_some() {
                                    ErrorCode::None.into()
                                } else {
                                    ErrorCode::UnknownMemberId.into()
                                }
                            })
                    })
                    .collect::<Vec<MemberResponse>>()
            })
        };

        let state: Wrapper<O> = if members
            .iter()
            .any(|member| member.error_code == i16::from(ErrorCode::None))
        {
            let leader = if self.members.contains_key(&leader_before) {
                Some(self.state.leader.clone())
            } else {
                self.members.keys().next().cloned()
            };

            Inner {
                generation_id: self.generation_id + 1,
                session_timeout_ms: self.session_timeout_ms,
                rebalance_timeout_ms: self.rebalance_timeout_ms,

                members: self.members,
                state: Forming {
                    protocol_type: Some(self.state.protocol_type),
                    protocol_name: Some(self.state.protocol_name),
                    leader,
                },
                storage: self.storage,
                skip_assignment: self.skip_assignment,
                inception: now,
            }
            .into()
        } else {
            self.into()
        };

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        (state, body)
    }

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;

        if let Some(member_id) = detail.member_id {
            if !member_id.is_empty() && !self.members.contains_key(member_id) {
                return (
                    self,
                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(detail.topics.map(|topics| {
                            topics
                                .as_ref()
                                .iter()
                                .map(|topic| {
                                    OffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(topic.partitions.as_ref().map(|partitions| {
                                            partitions
                                                .iter()
                                                .map(|partition| {
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(
                                                            ErrorCode::UnknownMemberId.into(),
                                                        )
                                                })
                                                .collect()
                                        }))
                                })
                                .collect()
                        }))
                        .into(),
                );
            }
        }

        if let Some(error_code) = self.offset_commit_error_code(detail) {
            return (
                self,
                OffsetCommitResponse::default()
                    .throttle_time_ms(Some(0))
                    .topics(detail.topics.map(|topics| {
                        topics
                            .as_ref()
                            .iter()
                            .map(|topic| {
                                OffsetCommitResponseTopic::default()
                                    .name(topic.name.clone())
                                    .partitions(topic.partitions.as_ref().map(|partitions| {
                                        partitions
                                            .iter()
                                            .map(|partition| {
                                                OffsetCommitResponsePartition::default()
                                                    .partition_index(partition.partition_index)
                                                    .error_code(error_code.into())
                                            })
                                            .collect()
                                    }))
                            })
                            .collect()
                    }))
                    .into(),
            );
        }

        match self.commit_offset(detail).await {
            Ok(body) => (self, body),
            Err(reason) => {
                debug!(?reason);
                (
                    self,
                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(detail.topics.map(|topics| {
                            topics
                                .as_ref()
                                .iter()
                                .map(|topic| {
                                    OffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(topic.partitions.as_ref().map(|partitions| {
                                            partitions
                                                .iter()
                                                .map(|partition| {
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(
                                                            ErrorCode::UnknownMemberId.into(),
                                                        )
                                                })
                                                .collect()
                                        }))
                                })
                                .collect()
                        }))
                        .into(),
                )
            }
        }
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;

        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                debug!(?error);
                (
                    self,
                    OffsetFetchResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(Some(ErrorCode::CoordinatorNotAvailable.into()))
                        .into(),
                )
            }
        }
    }
}
