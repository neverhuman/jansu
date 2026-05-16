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
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    ops::Deref,
    time::SystemTime,
};

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
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
    sync_group_response::SyncGroupResponse,
};
use jansu_storage::{
    OffsetCommitRequest, OffsetFetchRecord, Storage,
    Topition,
};
use tokio::time::Duration;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{Error, Result};

use super::{Coordinator, OffsetCommit};

use super::*;

impl<O> Inner<O, Forming>
where
    O: Storage,
{
    pub(super) fn missed_heartbeat(&mut self, group_id: &str, now: SystemTime) -> bool {
        let original = self.members.len();
        let timeout_ms = self.rebalance_timeout_ms.unwrap_or(self.session_timeout_ms);

        self.members.retain(|member_id, member| {
            let timed_out = member_timed_out(member.last_contact, timeout_ms, now);

            if timed_out {
                let leader_removed = self
                    .state
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader == member_id);
                if leader_removed {
                    info!(
                        "missed heartbeat for leader {member_id} for {group_id} in generation: {}",
                        self.generation_id
                    );
                    _ = self.state.leader.take();
                } else {
                    info!(
                        "missed heartbeat for {member_id} for {group_id} in generation: {}",
                        self.generation_id
                    );
                }
            }

            !timed_out
        });

        original > self.members.len()
    }
}

impl<O> Inner<O, Formed>
where
    O: Storage,
{
    pub(super) fn missed_heartbeat(&mut self, group_id: &str, now: SystemTime) -> bool {
        debug!(?group_id, ?now);

        let original = self.members.len();

        self.members.retain(|member_id, member| {
            debug!(?member_id, ?member);
            let timed_out = member_timed_out(member.last_contact, self.session_timeout_ms, now);
            if timed_out {
                info!(
                    "missed heartbeat for {member_id} for {group_id} in generation: {}",
                    self.generation_id
                );
            }
            !timed_out
        });

        original > self.members.len()
    }
}

impl<O, S> Inner<O, S>
where
    O: Storage,
    S: Debug,
{
    pub(super) async fn fetch_offset(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        debug!(?group_id, ?topics, ?groups, ?require_stable);

        let topics = if let Some(topics) = topics {
            let topics: Vec<Topition> = topics
                .iter()
                .flat_map(|topic| {
                    topic
                        .partition_indexes
                        .as_ref()
                        .into_iter()
                        .flatten()
                        .map(|partition_index| {
                            Topition::new(topic.name.clone(), *partition_index)
                        })
                        .collect::<Vec<_>>()
                })
                .collect();

            self.storage
                .offset_fetch_records(group_id, topics.deref(), require_stable)
                .await
                .map(|offsets| {
                    offsets
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            OffsetFetchResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    offsets
                                        .iter()
                                        .filter_map(|(topition, record)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetFetchResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .committed_offset(record.committed_offset())
                                                        .committed_leader_epoch(Some(
                                                            record.leader_epoch().unwrap_or(-1),
                                                        ))
                                                        .metadata(
                                                            record.metadata().map(|m| m.into()),
                                                        )
                                                        .error_code(ErrorCode::None.into()),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect()
                })
                .map(Some)?
        } else {
            None
        };

        let groups = if let Some(groups) = groups {
            let mut responses = vec![];

            for group in groups {
                debug!(?group);

                let response = if let Some(topics) = group.topics.as_ref().map(|topics| {
                    topics
                        .iter()
                        .flat_map(|topic| {
                            topic
                                .partition_indexes
                                .as_ref()
                                .into_iter()
                                .flatten()
                                .map(|partition_index| {
                                    Topition::new(topic.name.clone(), *partition_index)
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                }) {
                    self.storage
                        .offset_fetch_records(
                            Some(group.group_id.as_str()),
                            topics.deref(),
                            require_stable,
                        )
                        .await
                        .inspect(|offsets| debug!(?offsets))
                        .inspect_err(|err| error!(?err, ?group))
                } else {
                    self.storage
                        .committed_offset_topitions(&group.group_id)
                        .await
                        .map(|offsets| {
                            offsets
                                .into_iter()
                                .map(|(topition, offset)| {
                                    (topition, OffsetFetchRecord::default().with_offset(offset))
                                })
                                .collect()
                        })
                        .inspect(|offsets| debug!(?offsets))
                        .inspect_err(|err| error!(?err, ?group))
                }
                .map(|offsets| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some(
                            offsets
                                .iter()
                                .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                                    _ = topics.insert(topition.topic());
                                    topics
                                })
                                .iter()
                                .map(|topic_name| {
                                    OffsetFetchResponseTopics::default()
                                        .name((*topic_name).into())
                                        .partitions(Some(
                                            offsets
                                                .iter()
                                                .filter_map(|(topition, record)| {
                                                    if topition.topic() == *topic_name {
                                                        Some(
                                                            OffsetFetchResponsePartitions::default(
                                                            )
                                                            .partition_index(topition.partition())
                                                            .committed_offset(
                                                                record.committed_offset(),
                                                            )
                                                            .committed_leader_epoch(
                                                                record.leader_epoch().unwrap_or(-1),
                                                            )
                                                            .metadata(
                                                                record.metadata().map(|m| m.into()),
                                                            )
                                                            .error_code(ErrorCode::None.into()),
                                                        )
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect(),
                        ))
                        .error_code(ErrorCode::None.into())
                })?;

                responses.push(response);
            }

            Some(responses)
        } else {
            None
        };

        Ok(OffsetFetchResponse::default()
            .throttle_time_ms(Some(0))
            .topics(topics)
            .error_code(Some(ErrorCode::None.into()))
            .groups(groups)
            .into())
    }

    pub(super) async fn commit_offset(&mut self, detail: &OffsetCommit<'_>) -> Result<Body> {
        let retention_time_ms = detail.retention_time_ms.map_or(Ok(None), |ms| {
            u64::try_from(ms)
                .map(Duration::from_millis)
                .map_err(Error::from)
                .map(Some)
        })?;

        if let Some(topics) = detail.topics {
            let mut offsets = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let topition = Topition::new(topic.name.clone(), partition.partition_index);
                        let offset = OffsetCommitRequest::try_from(partition)?;

                        offsets.push((topition, offset));
                    }
                }
            }

            self.storage
                .offset_commit(detail.group_id, retention_time_ms, offsets.deref())
                .await
                .map(|value| {
                    let topics = value
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            OffsetCommitResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    value
                                        .iter()
                                        .filter_map(|(topition, error_code)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .error_code(i16::from(*error_code)),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect();

                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(Some(topics))
                        .into()
                })
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
        } else {
            Ok(OffsetCommitResponse::default()
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
                                                .error_code(ErrorCode::UnknownMemberId.into())
                                        })
                                        .collect()
                                }))
                        })
                        .collect()
                }))
                .into())
        }
    }
}

#[async_trait]
impl<O> Group for Inner<O, Forming>
where
    O: Storage,
{
    type JoinState = Inner<O, Forming>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Inner<O, Forming>;
    type LeaveState = Inner<O, Forming>;
    type OffsetCommitState = Inner<O, Forming>;
    type OffsetFetchState = Inner<O, Forming>;

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
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self, join_group_response.into());
        };

        if self.has_fenced_instance(member_id, group_instance_id) {
            debug!(join_outcome = ?ErrorCode::FencedInstanceId);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::FencedInstanceId.into())
                .generation_id(self.generation_id)
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id.to_owned())
                .members(Some([].into()));

            return (self, join_group_response.into());
        }

        let protocol = if let Some(protocol_name) = self.state.protocol_name.as_deref() {
            debug!(protocol_name);

            if let Some(protocol) = protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
            {
                debug!(?protocol);

                protocol
            } else {
                debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);

                let join_group_response = JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::InconsistentGroupProtocol.into())
                    .generation_id(self.generation_id)
                    .protocol_type(Some(protocol_type.into()))
                    .protocol_name(self.state.protocol_name.clone())
                    .leader("".into())
                    .skip_assignment(self.skip_assignment)
                    .member_id("".into())
                    .members(Some([].into()));

                return (self, join_group_response.into());
            }
        } else {
            self.state.protocol_type = Some(protocol_type.to_owned());
            self.state.protocol_name = Some(protocols[0].name.as_str().to_owned());

            self.session_timeout_ms = session_timeout_ms;
            self.rebalance_timeout_ms = rebalance_timeout_ms;

            &protocols[0]
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
                .protocol_type(self.state.protocol_type.clone())
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

            self.generation_id += 1;
            self.inception = now;

            return (self, join_group_response.into());
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

        if let Some(member) = self.members.get_mut(&member_id) {
            if member.join_response.metadata == protocol.metadata {
                debug!(
                    member_metadata = "existing",
                    member_id,
                    generation_id = self.generation_id
                );
            } else if group_instance_id.is_some() {
                debug!(
                    member_metadata = "soft_update",
                    member_id,
                    group_instance_id,
                    updated = ?protocol.metadata,
                    existing = ?member.join_response.metadata,
                    generation_id = self.generation_id
                );

                member.join_response.metadata = protocol.metadata.clone();
                self.inception = now;
            } else {
                self.generation_id += 1;

                debug!(
                    member_metadata = "update",
                    member_id,
                    updated = ?protocol.metadata,
                    existing = ?member.join_response.metadata,
                    generation_id = self.generation_id
                );

                member.join_response.metadata = protocol.metadata.clone();
                self.inception = now;
            }
        } else {
            self.generation_id += 1;

            debug!(
                member_metadata = "new",
                member_id,
                generation_id = self.generation_id
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
            self.inception = now;
        }

        debug!(?member_id, ?self.members);

        if self.state.leader.is_none() {
            info!(member_id, group_id, self.generation_id);

            _ = self.state.leader.replace(member_id.clone());
        }

        let join_group_response = JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .generation_id(self.generation_id)
            .protocol_type(self.state.protocol_type.clone())
            .protocol_name(self.state.protocol_name.clone())
            .leader(
                self.state
                    .leader
                    .as_ref()
                    .map_or(String::from(""), |leader| leader.clone()),
            )
            .skip_assignment(self.skip_assignment)
            .members(Some(
                if self
                    .state
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader == member_id.as_str())
                {
                    self.members
                        .values()
                        .cloned()
                        .map(|member| member.join_response)
                        .collect()
                } else {
                    [].into()
                },
            ))
            .member_id(member_id);

        debug!(join_outcome = ?ErrorCode::None);

        (self, join_group_response.into())
    }

    async fn sync(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        debug!(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            ?assignments
        );

        if !self.members.contains_key(member_id) {
            debug!(?self.members, sync_outcome = ?ErrorCode::UnknownMemberId);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if self.protocol_mismatch(protocol_type, protocol_name) {
            debug!(sync_outcome = ?ErrorCode::InconsistentGroupProtocol);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InconsistentGroupProtocol.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            debug!(self.generation_id, sync_outcome = ?ErrorCode::IllegalGeneration);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::IllegalGeneration.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if generation_id < self.generation_id {
            debug!(self.generation_id, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if self
            .state
            .leader
            .as_ref()
            .is_some_and(|leader_id| member_id != leader_id.as_str())
        {
            debug!(?self.state.leader, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        let Some(assignments) = assignments else {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        };

        let assignments = assignments
            .iter()
            .fold(BTreeMap::new(), |mut acc, assignment| {
                _ = acc.insert(assignment.member_id.clone(), assignment.assignment.clone());
                acc
            });

        debug!(?assignments);

        let sync_group_response = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(self.state.protocol_type.clone())
            .protocol_name(self.state.protocol_name.clone())
            .assignment(
                assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or(Bytes::from_static(b"")),
            );

        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = assignments.contains_key(member_id));

        let state = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,

            members: self.members,
            generation_id: self.generation_id,
            state: Formed {
                protocol_name: self.state.protocol_name.expect("protocol_name"),
                protocol_type: self.state.protocol_type.expect("protocol_type"),
                leader: member_id.to_owned(),
                assignments,
            },
            storage: self.storage,
            skip_assignment: self.skip_assignment,
            inception: now,
        };

        (state.into(), sync_group_response.into())
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        let _ = group_instance_id;

        if !self.members.contains_key(member_id) {
            debug!(?self.members);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::UnknownMemberId.into())
                    .into(),
            );
        }

        if generation_id > self.generation_id {
            debug!(?self.generation_id);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::IllegalGeneration.into())
                    .into(),
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        if self.missed_heartbeat(group_id, now) || (generation_id < self.generation_id) {
            debug!(self.generation_id);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
                    .into(),
            );
        }

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
        debug!(?group_id, member_id, ?members);

        let leader_before = self.state.leader.clone();

        let members = if let Some(member_id) = member_id {
            debug!(member_id);

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

        let removed = members.iter().any(|member| {
            let error_code = i16::from(ErrorCode::None);

            member.error_code == error_code
        });

        if removed {
            self.generation_id += 1;
            self.inception = now;
            if leader_before
                .as_deref()
                .is_some_and(|leader| !self.members.contains_key(leader))
            {
                self.state.leader = self.members.keys().next().cloned();
            }
        }

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        (self, body)
    }

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;
        debug!(?detail);

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
        debug!(group_id, ?topics, ?groups, ?require_stable);

        // Kafka semantics: when require_stable=true and the group is rebalancing
        // (Forming state), return UNSTABLE_OFFSET_COMMIT per-partition.
        if require_stable == Some(true) {
            let response_topics = topics.map(|topics| {
                topics
                    .iter()
                    .map(|topic| {
                        OffsetFetchResponseTopic::default()
                            .name(topic.name.clone())
                            .partitions(topic.partition_indexes.as_ref().map(|indexes| {
                                indexes
                                    .iter()
                                    .map(|&partition_index| {
                                        OffsetFetchResponsePartition::default()
                                            .partition_index(partition_index)
                                            .committed_offset(-1)
                                            .committed_leader_epoch(Some(-1))
                                            .metadata(None)
                                            .error_code(ErrorCode::UnstableOffsetCommit.into())
                                    })
                                    .collect()
                            }))
                    })
                    .collect()
            });

            let response_groups = groups.map(|groups| {
                groups
                    .iter()
                    .map(|group| {
                        OffsetFetchResponseGroup::default()
                            .group_id(group.group_id.clone())
                            .topics(group.topics.as_ref().map(|topics| {
                                topics
                                    .iter()
                                    .map(|topic| {
                                        OffsetFetchResponseTopics::default()
                                            .name(topic.name.clone())
                                            .partitions(topic.partition_indexes.as_ref().map(
                                                |indexes| {
                                                    indexes
                                                        .iter()
                                                        .map(|&partition_index| {
                                                            OffsetFetchResponsePartitions::default()
                                                                .partition_index(partition_index)
                                                                .committed_offset(-1)
                                                                .committed_leader_epoch(-1)
                                                                .metadata(None)
                                                                .error_code(
                                                                    ErrorCode::UnstableOffsetCommit
                                                                        .into(),
                                                                )
                                                        })
                                                        .collect()
                                                },
                                            ))
                                    })
                                    .collect()
                            }))
                            .error_code(ErrorCode::None.into())
                    })
                    .collect()
            });

            return (
                self,
                OffsetFetchResponse::default()
                    .throttle_time_ms(Some(0))
                    .topics(response_topics)
                    .error_code(Some(ErrorCode::None.into()))
                    .groups(response_groups)
                    .into(),
            );
        }

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
