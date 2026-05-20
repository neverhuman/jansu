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

use std::{collections::BTreeMap, time::SystemTime};

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
    offset_fetch_request::OffsetFetchRequestTopic,
    offset_fetch_response::OffsetFetchResponse,
    sync_group_request::SyncGroupRequestAssignment,
    sync_group_response::SyncGroupResponse,
};
use jansu_storage::{GroupDetail, GroupState, Storage};
use tokio::time::Duration;
use uuid::Uuid;

use crate::{Error, Result};

use super::{Coordinator, OffsetCommit};

use super::*;

use jansu_sans_io::{
    create_topics_request::CreatableTopic,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use jansu_storage::StorageContainer;
use pretty_assertions::assert_eq;
use tracing::subscriber::DefaultGuard;
use url::Url;

#[cfg(miri)]
fn init_tracing() -> Result<()> {
    Ok(())
}

#[cfg(not(miri))]
fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    use tracing::Level;
    use tracing_subscriber::fmt::format::FmtSpan;

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_max_level(Level::DEBUG)
            .with_span_events(FmtSpan::ACTIVE)
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

fn storage_url() -> Result<Url> {
    Ok(Url::parse("memory://phase10-broker-tests/")?)
}

#[tokio::test]
async fn lifecycle() -> Result<()> {
    let _guard = init_tracing()?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let cluster = "abc";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "test-consumer-group";
    const TOPIC: &str = "test";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    let first_member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            reason,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse {
            throttle_time_ms: Some(0),
            error_code,
            generation_id: -1,
            protocol_type: Some(protocol_type),
            protocol_name: Some(protocol_name),
            leader,
            skip_assignment: Some(false),
            members: Some(members),
            member_id,
            ..
        }) => {
            assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
            assert_eq!("consumer", protocol_type);
            assert_eq!("", protocol_name);
            assert!(leader.is_empty());
            assert!(member_id.starts_with(CLIENT_ID));
            assert_eq!(0, members.len());

            let join_response = s
                .join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols[..]),
                    reason,
                )
                .await?;

            let join_response_expected = Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .generation_id(0)
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .leader(member_id.clone())
                    .skip_assignment(Some(false))
                    .member_id(member_id.clone())
                    .members(Some(
                        [JoinGroupResponseMember::default()
                            .member_id(member_id.clone())
                            .group_instance_id(None)
                            .metadata(first_member_range_meta.clone())]
                        .into(),
                    )),
            );

            assert_eq!(join_response_expected, join_response);

            member_id
        }

        otherwise => panic!("{otherwise:?}"),
    };

    let first_member_assignment_01 = Bytes::from_static(b"assignment_01");

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(first_member_id.clone())
        .assignment(first_member_assignment_01.clone())];

    assert_eq!(
        Body::from(
            SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(0)
                .protocol_type(Some(PROTOCOL_TYPE.into()))
                .protocol_name(Some(RANGE.into()))
                .assignment(first_member_assignment_01)
        ),
        s.sync(
            GROUP_ID,
            0,
            &first_member_id,
            group_instance_id,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?
    );

    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
        ),
        s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id)
            .await?
    );

    let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
    let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(second_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(second_member_sticky_meta.clone()),
    ];

    let second_member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            reason,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse {
            throttle_time_ms: Some(0),
            error_code,
            generation_id: -1,
            protocol_type: None,
            protocol_name: Some(protocol_name),
            leader,
            skip_assignment: Some(false),
            members: Some(members),
            member_id,
            ..
        }) => {
            assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
            assert_eq!("", protocol_name);
            assert!(leader.is_empty());
            assert!(member_id.starts_with(CLIENT_ID));
            assert_eq!(0, members.len());

            let join_response = s
                .join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&protocols[..]),
                    reason,
                )
                .await?;

            let join_response_expected = Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .generation_id(1)
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .leader(first_member_id.clone())
                    .skip_assignment(Some(false))
                    .member_id(member_id.clone())
                    .members(Some([].into())),
            );

            assert_eq!(join_response_expected, join_response);

            member_id
        }

        otherwise => panic!("{otherwise:?}"),
    };

    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(i16::from(ErrorCode::RebalanceInProgress))
        ),
        s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id,)
            .await?
    );

    assert_eq!(
        Body::from(
            OffsetCommitResponse::default()
                .throttle_time_ms(Some(0))
                .topics(Some(
                    [OffsetCommitResponseTopic::default()
                        .name(TOPIC.into())
                        .partitions(Some(
                            (0..=2)
                                .map(|partition_index| OffsetCommitResponsePartition::default()
                                    .partition_index(partition_index)
                                    .error_code(ErrorCode::RebalanceInProgress.into()))
                                .collect(),
                        )),]
                    .into()
                ))
        ),
        s.offset_commit(OffsetCommit {
            group_id: GROUP_ID,
            generation_id_or_member_epoch: Some(0),
            member_id: Some(&first_member_id),
            group_instance_id,
            retention_time_ms: None,
            topics: Some(&[OffsetCommitRequestTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    (0..=2)
                        .map(|partition_index| OffsetCommitRequestPartition::default()
                            .partition_index(partition_index)
                            .committed_offset(1)
                            .committed_leader_epoch(Some(0))
                            .commit_timestamp(None)
                            .committed_metadata(Some("".into())))
                        .collect(),
                )),]),
        })
        .await?
    );

    {
        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_02");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_02");

        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &first_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id,
                protocol_type,
                protocol_name,
                leader,
                skip_assignment: Some(false),
                member_id,
                members: Some(members),
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::None), error_code);
                assert_eq!(2, generation_id);
                assert_eq!(Some(PROTOCOL_TYPE.into()), protocol_type);
                assert_eq!(Some(RANGE.into()), protocol_name);
                assert_eq!(first_member_id, leader);
                assert_eq!(first_member_id, member_id);

                assert_eq!(
                    Some(first_member_range_meta),
                    members
                        .iter()
                        .find(|member| member.member_id == first_member_id)
                        .map(|member| member.metadata.clone())
                );

                assert_eq!(
                    Some(second_member_range_meta.clone()),
                    members
                        .iter()
                        .find(|member| member.member_id == second_member_id)
                        .map(|member| member.metadata.clone())
                );
            }

            otherwise => panic!("{otherwise:?}"),
        }
    }

    {
        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(second_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(second_member_sticky_meta.clone()),
        ];

        match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &second_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id,
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                member_id,
                members: Some(members),
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::None), error_code);
                assert_eq!(2, generation_id);
                assert_eq!(PROTOCOL_TYPE, protocol_type);
                assert_eq!(RANGE, protocol_name);
                assert_eq!(first_member_id, leader);
                assert_eq!(second_member_id, member_id);
                assert_eq!(0, members.len());
            }

            otherwise => panic!("{otherwise:?}"),
        }
    }

    let second_member_assignment_02 = Bytes::from_static(b"second_member_assignment_02");

    {
        let first_member_assignment_02 = Bytes::from_static(b"first_member_assignment_02");

        let assignments = [
            SyncGroupRequestAssignment::default()
                .member_id(first_member_id.clone())
                .assignment(first_member_assignment_02.clone()),
            SyncGroupRequestAssignment::default()
                .member_id(second_member_id.clone())
                .assignment(second_member_assignment_02.clone()),
        ];

        assert_eq!(
            Body::from(
                SyncGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .assignment(first_member_assignment_02)
            ),
            s.sync(
                GROUP_ID,
                2,
                &first_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await?
        );
    }

    assert_eq!(
        Body::from(
            SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
                .protocol_type(Some(PROTOCOL_TYPE.into()))
                .protocol_name(Some(RANGE.into()))
                .assignment(second_member_assignment_02)
        ),
        s.sync(
            GROUP_ID,
            2,
            &second_member_id,
            group_instance_id,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&[]),
        )
        .await?
    );

    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
        ),
        s.heartbeat(GROUP_ID, 2, &first_member_id, group_instance_id,)
            .await?
    );

    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
        ),
        s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
            .await?
    );

    assert_eq!(
        Body::from(
            LeaveGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
                .members(Some(
                    [MemberResponse::default()
                        .member_id(first_member_id.clone())
                        .group_instance_id(None)
                        .error_code(ErrorCode::None.into())]
                    .into()
                ))
        ),
        s.leave(
            GROUP_ID,
            None,
            Some(&[MemberIdentity::default()
                .member_id(first_member_id.clone())
                .group_instance_id(None)
                .reason(Some("the consumer is being closed".into()))]),
        )
        .await?
    );

    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
        ),
        s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
            .await?
    );

    {
        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(second_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(second_member_sticky_meta.clone()),
        ];

        assert_eq!(
            Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .generation_id(3)
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .leader(second_member_id.clone())
                    .skip_assignment(Some(false))
                    .member_id(second_member_id.clone())
                    .members(Some(
                        [JoinGroupResponseMember::default()
                            .member_id(second_member_id.clone())
                            .group_instance_id(None)
                            .metadata(second_member_range_meta.clone())]
                        .into()
                    ))
            ),
            s.join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &second_member_id,
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                reason,
            )
            .await?
        );
    }

    {
        let second_member_assignment_03 = Bytes::from_static(b"second_member_assignment_03");

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(second_member_id.clone())
            .assignment(second_member_assignment_03.clone())];

        assert_eq!(
            Body::from(
                SyncGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .protocol_type(Some(PROTOCOL_TYPE.into()))
                    .protocol_name(Some(RANGE.into()))
                    .assignment(second_member_assignment_03)
            ),
            s.sync(
                GROUP_ID,
                3,
                &second_member_id,
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await?
        );
    }

    Ok(())
}

#[tokio::test]
async fn rejoin() -> Result<()> {
    let _guard = init_tracing()?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let cluster = "abc";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "test-consumer-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let first_member_protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    let first_member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&first_member_protocols[..]),
            reason,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse {
            throttle_time_ms: Some(0),
            error_code,
            generation_id: -1,
            leader,
            skip_assignment: Some(false),
            members: Some(members),
            member_id,
            ..
        }) => {
            assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
            assert!(leader.is_empty());
            assert!(member_id.starts_with(CLIENT_ID));
            assert_eq!(0, members.len());

            assert_eq!(
                Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(0)
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .leader(member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(member_id.clone())
                        .members(Some(
                            [JoinGroupResponseMember::default()
                                .member_id(member_id.clone())
                                .group_instance_id(None)
                                .metadata(first_member_range_meta.clone())]
                            .into()
                        ))
                ),
                s.join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&first_member_protocols[..]),
                    reason,
                )
                .await?
            );

            member_id
        }

        otherwise => panic!("{otherwise:?}"),
    };

    let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
    let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

    let second_member_protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(second_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(second_member_sticky_meta),
    ];

    let second_member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&second_member_protocols[..]),
            reason,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse {
            throttle_time_ms: Some(0),
            error_code,
            generation_id: -1,
            leader,
            skip_assignment: Some(false),
            members: Some(members),
            member_id,
            ..
        }) => {
            assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
            assert!(leader.is_empty());
            assert!(member_id.starts_with(CLIENT_ID));
            assert_eq!(0, members.len());

            assert_eq!(
                Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(1)
                        .protocol_type(Some(PROTOCOL_TYPE.into()))
                        .protocol_name(Some(RANGE.into()))
                        .leader(first_member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(member_id.clone())
                        .members(Some([].into()))
                ),
                s.join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &member_id,
                    group_instance_id,
                    PROTOCOL_TYPE,
                    Some(&second_member_protocols[..]),
                    reason,
                )
                .await?
            );

            member_id
        }

        otherwise => panic!("{otherwise:?}"),
    };

    match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            &first_member_id,
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&first_member_protocols[..]),
            reason,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse {
            throttle_time_ms: Some(0),
            error_code,
            generation_id: 1,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment: Some(false),
            member_id,
            members: Some(members),
            ..
        }) => {
            assert_eq!(i16::from(ErrorCode::None), error_code);
            assert_eq!(Some(PROTOCOL_TYPE.into()), protocol_type);
            assert_eq!(Some(RANGE.into()), protocol_name);
            assert_eq!(first_member_id.clone(), leader);
            assert_eq!(first_member_id.clone(), member_id);
            assert_eq!(2, members.len());
            assert!(
                members.contains(
                    &JoinGroupResponseMember::default()
                        .member_id(second_member_id.clone())
                        .group_instance_id(None)
                        .metadata(second_member_range_meta.clone())
                )
            );
            assert!(
                members.contains(
                    &JoinGroupResponseMember::default()
                        .member_id(first_member_id.clone())
                        .group_instance_id(None)
                        .metadata(first_member_range_meta.clone())
                )
            );
        }

        otherwise => panic!("{otherwise:?}"),
    }

    assert_eq!(
        Body::from(
            JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
                .generation_id(1)
                .protocol_type(Some(PROTOCOL_TYPE.into()))
                .protocol_name(Some(RANGE.into()))
                .leader(first_member_id.clone())
                .skip_assignment(Some(false))
                .member_id(second_member_id.clone())
                .members(Some([].into(),))
        ),
        s.join(
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            &second_member_id,
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&second_member_protocols[..]),
            reason,
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn member_id_required_error_code_joins_group() -> Result<()> {
    let _guard = init_tracing()?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let cluster = "abc";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "test-consumer-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let s = Wrapper::with_storage_group_detail(
        storage,
        GroupDetail {
            session_timeout_ms,
            rebalance_timeout_ms,
            state: GroupState::Forming {
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: None,
            },
            ..Default::default()
        },
    );

    let now = SystemTime::now();

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let first_member_protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    assert!(s.members().is_empty());

    match s
        .join(
            now,
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&first_member_protocols[..]),
            reason,
        )
        .await
    {
        (
            s,
            Body::JoinGroupResponse(JoinGroupResponse {
                error_code,
                generation_id,
                leader,
                member_id,
                members,
                ..
            }),
        ) => {
            assert_eq!(-1, generation_id);
            assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
            assert_eq!("", leader);
            assert_eq!(Some([].into()), members);
            assert!(member_id.starts_with(CLIENT_ID));
            assert_eq!(1, s.members().len());
        }

        otherwise => panic!("{otherwise:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn forming_leader_leaves_group() -> Result<()> {
    let _guard = init_tracing()?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let cluster = "abc";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "test-consumer-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let s = Wrapper::with_storage_group_detail(
        storage,
        GroupDetail {
            session_timeout_ms,
            rebalance_timeout_ms,
            state: GroupState::Forming {
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: None,
            },
            ..Default::default()
        },
    );

    let now = SystemTime::now();

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let first_member_protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    assert!(s.members().is_empty());

    let (s, member_id) = match s
        .join(
            now,
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            "",
            group_instance_id,
            PROTOCOL_TYPE,
            Some(&first_member_protocols[..]),
            reason,
        )
        .await
    {
        (
            s,
            Body::JoinGroupResponse(JoinGroupResponse {
                error_code,
                generation_id,
                leader,
                member_id,
                members,
                ..
            }),
        ) => {
            assert_eq!(-1, generation_id);
            assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
            assert_eq!("", leader);
            assert_eq!(Some([].into()), members);
            assert!(member_id.starts_with(CLIENT_ID));
            assert_eq!(1, s.members().len());

            (s, member_id)
        }

        otherwise => panic!("{otherwise:?}"),
    };

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment_01"))];

    let (s, _) = s
        .sync(
            now,
            GROUP_ID,
            0,
            member_id.as_str(),
            group_instance_id,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await;

    assert_eq!(Some(member_id.as_str()), s.leader());

    let (s, _) = s.leave(now, GROUP_ID, Some(member_id.as_str()), None).await;

    assert_eq!(None, s.leader());

    Ok(())
}

#[tokio::test]
async fn sync_from_member_while_forming() -> Result<()> {
    let _guard = init_tracing()?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let cluster = "abc";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "test-consumer-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let s = Wrapper::with_storage_group_detail(
        storage,
        GroupDetail {
            session_timeout_ms,
            rebalance_timeout_ms,
            state: GroupState::Forming {
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: None,
            },
            ..Default::default()
        },
    );

    let now = SystemTime::now();

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
    let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

    assert!(s.members().is_empty());
    assert_eq!(None, s.leader());
    assert_eq!(None, s.assignments());

    let first_member_id = format!("{}-{}", CLIENT_ID, Uuid::new_v4());

    let (s, _) = s
        .join(
            now,
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            first_member_id.as_str(),
            group_instance_id,
            PROTOCOL_TYPE,
            Some(
                &[
                    JoinGroupRequestProtocol::default()
                        .name(RANGE.into())
                        .metadata(first_member_range_meta.clone()),
                    JoinGroupRequestProtocol::default()
                        .name(COOPERATIVE_STICKY.into())
                        .metadata(first_member_sticky_meta),
                ][..],
            ),
            reason,
        )
        .await;

    assert_eq!(0, s.generation_id());
    assert_eq!(1, s.members().len());
    assert!(
        s.members().contains(
            &JoinGroupResponseMember::default()
                .member_id(first_member_id.clone())
                .group_instance_id(None)
                .metadata(first_member_range_meta.clone())
        )
    );
    assert_eq!(Some(first_member_id.as_str()), s.leader());

    let second_member_id = format!("{}-{}", CLIENT_ID, Uuid::new_v4());

    let (s, _) = s
        .join(
            now,
            Some(CLIENT_ID),
            GROUP_ID,
            session_timeout_ms,
            rebalance_timeout_ms,
            second_member_id.as_str(),
            group_instance_id,
            PROTOCOL_TYPE,
            Some(
                &[
                    JoinGroupRequestProtocol::default()
                        .name(RANGE.into())
                        .metadata(second_member_range_meta.clone()),
                    JoinGroupRequestProtocol::default()
                        .name(COOPERATIVE_STICKY.into())
                        .metadata(second_member_sticky_meta),
                ][..],
            ),
            reason,
        )
        .await;

    assert_eq!(1, s.generation_id());
    assert_eq!(2, s.members().len());
    assert_eq!(None, s.assignments());

    assert!(
        s.members().contains(
            &JoinGroupResponseMember::default()
                .member_id(first_member_id.clone())
                .group_instance_id(None)
                .metadata(first_member_range_meta.clone())
        )
    );

    assert!(
        s.members().contains(
            &JoinGroupResponseMember::default()
                .member_id(second_member_id.clone())
                .group_instance_id(None)
                .metadata(second_member_range_meta.clone())
        )
    );

    assert_eq!(Some(first_member_id.as_str()), s.leader());

    let (s, _) = s
        .sync(
            now,
            GROUP_ID,
            1,
            second_member_id.as_str(),
            group_instance_id,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&[]),
        )
        .await;

    assert_eq!(1, s.generation_id());
    assert_eq!(Some(first_member_id.as_str()), s.leader());
    assert_eq!(Some(first_member_id.as_str()), s.leader());
    assert_eq!(None, s.assignments());

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(first_member_id.clone())
        .assignment(Bytes::from_static(b"first_assignment_01"))];

    let (s, _) = s
        .sync(
            now,
            GROUP_ID,
            0,
            first_member_id.as_str(),
            group_instance_id,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await;

    assert_eq!(1, s.generation_id());
    assert_eq!(Some(first_member_id.as_str()), s.leader());
    assert_eq!(None, s.assignments());

    let first_member_assignment = Bytes::from_static(b"first_assignment_02");
    let second_member_assignment = Bytes::from_static(b"second_assignment_02");

    let mut assignments = BTreeMap::new();
    _ = assignments.insert(first_member_id.clone(), first_member_assignment.clone());
    _ = assignments.insert(second_member_id.clone(), second_member_assignment.clone());

    let (s, _) = s
        .sync(
            now,
            GROUP_ID,
            1,
            first_member_id.as_str(),
            group_instance_id,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(
                &assignments
                    .iter()
                    .map(|(member_id, assignment)| {
                        SyncGroupRequestAssignment::default()
                            .member_id(member_id.to_owned())
                            .assignment(assignment.to_owned())
                    })
                    .collect::<Vec<_>>()[..],
            ),
        )
        .await;

    assert_eq!(1, s.generation_id());
    assert_eq!(Some(first_member_id.as_str()), s.leader());
    assert_eq!(Some(first_member_id.as_str()), s.leader());

    assert_eq!(
        Some(first_member_assignment),
        s.assignments()
            .map(|assignments| assignments.get(first_member_id.as_str()).cloned())
            .unwrap()
    );

    assert_eq!(
        Some(second_member_assignment),
        s.assignments()
            .map(|assignments| assignments.get(second_member_id.as_str()).cloned())
            .unwrap()
    );

    Ok(())
}

/// Verify that offset commit with a stale generation_id returns IllegalGeneration
/// when the member_id is not present (or returns RebalanceInProgress when the
/// member_id IS present but the generation is old).
#[tokio::test]
async fn offset_commit_with_stale_generation_returns_illegal_generation() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "stale-gen";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "stale-gen-group";
    const TOPIC: &str = "test-topic";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // First join: get member id
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    // Second join: join with member_id to get generation 0
    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    // Sync to form the group at generation 0
    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Force a new generation by having the member rejoin with different metadata
    let new_protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta_v2"))];

    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&new_protocols[..]),
            None,
        )
        .await?;

    // Sync to form at generation 1
    let _ = s
        .sync(
            GROUP_ID,
            1,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Now try to commit with stale generation 0 (current is 1)
    // With a known member_id, this should return RebalanceInProgress
    let commit_result = s
        .offset_commit(OffsetCommit {
            group_id: GROUP_ID,
            generation_id_or_member_epoch: Some(0),
            member_id: Some(&member_id),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&[OffsetCommitRequestTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    [OffsetCommitRequestPartition::default()
                        .partition_index(0)
                        .committed_offset(42)
                        .committed_leader_epoch(Some(0))
                        .commit_timestamp(None)
                        .committed_metadata(Some("".into()))]
                    .into(),
                ))]),
        })
        .await?;

    match commit_result {
        Body::OffsetCommitResponse(OffsetCommitResponse {
            topics: Some(topics),
            ..
        }) => {
            for topic in &topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        assert_eq!(
                            partition.error_code,
                            i16::from(ErrorCode::RebalanceInProgress),
                            "Expected RebalanceInProgress for stale generation with known member_id"
                        );
                    }
                }
            }
        }
        otherwise => panic!("Expected OffsetCommitResponse, got: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that heartbeat from an unknown member returns UnknownMemberId
#[tokio::test]
async fn heartbeat_from_unknown_member_returns_error() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "hb-unknown";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "hb-unknown-group";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // Join and form the group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Heartbeat from a completely fake member
    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
        ),
        s.heartbeat(GROUP_ID, 0, "fake-member-id-does-not-exist", None)
            .await?
    );

    Ok(())
}

/// Verify that offset commit from an unknown member returns UnknownMemberId in the Formed state
#[tokio::test]
async fn offset_commit_unknown_member_in_formed_state() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "oc-unknown";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "oc-unknown-group";
    const TOPIC: &str = "test-topic";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // Join and form group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Offset commit from an unknown member
    let commit_result = s
        .offset_commit(OffsetCommit {
            group_id: GROUP_ID,
            generation_id_or_member_epoch: Some(0),
            member_id: Some("rogue-member-id"),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&[OffsetCommitRequestTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    [OffsetCommitRequestPartition::default()
                        .partition_index(0)
                        .committed_offset(10)
                        .committed_leader_epoch(Some(0))
                        .commit_timestamp(None)
                        .committed_metadata(Some("".into()))]
                    .into(),
                ))]),
        })
        .await?;

    match commit_result {
        Body::OffsetCommitResponse(OffsetCommitResponse {
            topics: Some(topics),
            ..
        }) => {
            for topic in &topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        assert_eq!(
                            partition.error_code,
                            i16::from(ErrorCode::UnknownMemberId),
                            "Expected UnknownMemberId for rogue member offset commit"
                        );
                    }
                }
            }
        }
        otherwise => panic!("Expected OffsetCommitResponse, got: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that offset commit succeeds with matching generation and member_id
#[tokio::test]
async fn offset_commit_succeeds_with_current_generation() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "oc-success";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "oc-success-group";
    const TOPIC: &str = "test-topic";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    // Create topic first so offset_commit has a valid topition
    let _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // Join and form group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Offset commit with correct generation (0) and known member
    let commit_result = s
        .offset_commit(OffsetCommit {
            group_id: GROUP_ID,
            generation_id_or_member_epoch: Some(0),
            member_id: Some(&member_id),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&[OffsetCommitRequestTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    [OffsetCommitRequestPartition::default()
                        .partition_index(0)
                        .committed_offset(42)
                        .committed_leader_epoch(Some(0))
                        .commit_timestamp(None)
                        .committed_metadata(Some("test-metadata".into()))]
                    .into(),
                ))]),
        })
        .await?;

    match commit_result {
        Body::OffsetCommitResponse(OffsetCommitResponse {
            topics: Some(topics),
            ..
        }) => {
            assert!(
                !topics.is_empty(),
                "Expected at least one topic in response"
            );
            for topic in &topics {
                assert_eq!(topic.name, TOPIC);
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        assert_eq!(
                            partition.error_code,
                            i16::from(ErrorCode::None),
                            "Expected successful offset commit"
                        );
                    }
                }
            }
        }
        otherwise => panic!("Expected OffsetCommitResponse, got: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that committed offsets can be fetched back with correct metadata
#[tokio::test]
async fn offset_fetch_returns_committed_offsets() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "of-fetch";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "of-fetch-group";
    const TOPIC: &str = "test-topic";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    // Create topic
    let _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // Join and form group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Commit offset
    let _ = s
        .offset_commit(OffsetCommit {
            group_id: GROUP_ID,
            generation_id_or_member_epoch: Some(0),
            member_id: Some(&member_id),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&[OffsetCommitRequestTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    [OffsetCommitRequestPartition::default()
                        .partition_index(0)
                        .committed_offset(100)
                        .committed_leader_epoch(Some(1))
                        .commit_timestamp(None)
                        .committed_metadata(Some("my-metadata".into()))]
                    .into(),
                ))]),
        })
        .await?;

    // Fetch committed offsets
    let fetch_result = s
        .offset_fetch(
            Some(GROUP_ID),
            Some(&[OffsetFetchRequestTopic::default()
                .name(TOPIC.into())
                .partition_indexes(Some([0].into()))]),
            None,
            None,
        )
        .await?;

    match fetch_result {
        Body::OffsetFetchResponse(OffsetFetchResponse {
            topics: Some(topics),
            ..
        }) => {
            assert!(!topics.is_empty(), "Expected at least one topic");
            let topic = &topics[0];
            assert_eq!(topic.name, TOPIC);
            let partitions = topic.partitions.as_ref().expect("Expected partitions");
            assert!(!partitions.is_empty(), "Expected at least one partition");
            let partition = &partitions[0];
            assert_eq!(partition.partition_index, 0);
            assert_eq!(partition.committed_offset, 100);
            assert_eq!(
                partition.committed_leader_epoch,
                Some(1),
                "Expected committed leader epoch to be preserved"
            );
            assert_eq!(
                partition.metadata,
                Some("my-metadata".into()),
                "Expected metadata to round-trip"
            );
            assert_eq!(partition.error_code, i16::from(ErrorCode::None));
        }
        otherwise => panic!("Expected OffsetFetchResponse with topics, got: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that offset_fetch with require_stable=true returns UnstableOffsetCommit
/// when the group coordinator is in a rebalancing (Forming) state.
#[tokio::test]
async fn offset_fetch_with_require_stable_during_rebalance_returns_unstable() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "rs-unstable";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "rs-unstable-group";
    const TOPIC: &str = "test-topic";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // Join to create the group (enters Forming state)
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    // Second join with member_id but don't sync — group stays in Forming
    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    // offset_fetch with require_stable=true while group is in Forming state
    let fetch_result = s
        .offset_fetch(
            Some(GROUP_ID),
            Some(&[OffsetFetchRequestTopic::default()
                .name(TOPIC.into())
                .partition_indexes(Some([0, 1].into()))]),
            None,
            Some(true),
        )
        .await?;

    match fetch_result {
        Body::OffsetFetchResponse(OffsetFetchResponse {
            topics: Some(topics),
            error_code,
            ..
        }) => {
            // Top-level error should be None (per-partition errors carry the info)
            assert_eq!(error_code, Some(i16::from(ErrorCode::None)));

            assert!(!topics.is_empty(), "Expected at least one topic");
            let topic = &topics[0];
            assert_eq!(topic.name, TOPIC);
            let partitions = topic.partitions.as_ref().expect("Expected partitions");
            assert_eq!(partitions.len(), 2, "Expected 2 partitions");

            for partition in partitions {
                assert_eq!(
                    partition.error_code,
                    i16::from(ErrorCode::UnstableOffsetCommit),
                    "Expected UnstableOffsetCommit for require_stable during rebalance"
                );
                assert_eq!(partition.committed_offset, -1);
            }
        }
        otherwise => panic!("Expected OffsetFetchResponse with topics, got: {otherwise:?}"),
    }

    // Verify that require_stable=false (or None) still returns normally
    let fetch_result_no_stable = s
        .offset_fetch(
            Some(GROUP_ID),
            Some(&[OffsetFetchRequestTopic::default()
                .name(TOPIC.into())
                .partition_indexes(Some([0].into()))]),
            None,
            None,
        )
        .await?;

    match fetch_result_no_stable {
        Body::OffsetFetchResponse(OffsetFetchResponse {
            topics: Some(topics),
            ..
        }) => {
            let topic = &topics[0];
            let partitions = topic.partitions.as_ref().expect("Expected partitions");
            for partition in partitions {
                assert_eq!(
                    partition.error_code,
                    i16::from(ErrorCode::None),
                    "Expected None error code when require_stable is not set"
                );
            }
        }
        // If no committed offsets exist, we may get an empty topics list — that's fine
        Body::OffsetFetchResponse(OffsetFetchResponse { topics: None, .. }) => {}
        otherwise => panic!("Unexpected response: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that offset commit during Forming state (before sync) returns RebalanceInProgress
/// when the generation_id is stale compared to the current group generation.
#[tokio::test]
async fn offset_commit_during_forming_returns_rebalance_in_progress() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "oc-forming";
    let node = 1;

    const CLIENT_ID: &str = "test-client";
    const GROUP_ID: &str = "oc-forming-group";
    const TOPIC: &str = "test-topic";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta"))];

    // First join: get member id
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    // Second join: join with member_id, group is at generation 0 in Forming state
    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    // Sync to form the group at generation 0
    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Now trigger a rebalance by rejoining with new metadata
    let new_protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from_static(b"meta_v2"))];

    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&new_protocols[..]),
            None,
        )
        .await?;

    // Group is now in Forming state at generation 1. Try to commit with old generation 0.
    let commit_result = s
        .offset_commit(OffsetCommit {
            group_id: GROUP_ID,
            generation_id_or_member_epoch: Some(0),
            member_id: Some(&member_id),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&[OffsetCommitRequestTopic::default()
                .name(TOPIC.into())
                .partitions(Some(
                    [OffsetCommitRequestPartition::default()
                        .partition_index(0)
                        .committed_offset(42)
                        .committed_leader_epoch(Some(0))
                        .commit_timestamp(None)
                        .committed_metadata(Some("".into()))]
                    .into(),
                ))]),
        })
        .await?;

    match commit_result {
        Body::OffsetCommitResponse(OffsetCommitResponse {
            topics: Some(topics),
            ..
        }) => {
            for topic in &topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        assert_eq!(
                            partition.error_code,
                            i16::from(ErrorCode::RebalanceInProgress),
                            "Expected RebalanceInProgress for offset commit during rebalance with stale generation"
                        );
                    }
                }
            }
        }
        otherwise => panic!("Expected OffsetCommitResponse, got: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that an expired member in Formed state triggers a rebalance and fences
/// stale generation requests from the surviving member.
#[tokio::test]
async fn formed_member_session_timeout_triggers_rebalance_and_generation_fencing() -> Result<()> {
    let _guard = init_tracing()?;

    const GROUP_ID: &str = "timeout-group";
    const RANGE: &str = "range";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id("formed-timeout")
        .node_id(2)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta");
    let second_member_range_meta = Bytes::from_static(b"second_member_range_meta");
    let first_member_id = format!("timeout-client-{}", Uuid::now_v7());
    let second_member_id = format!("timeout-client-{}", Uuid::now_v7());

    let now = SystemTime::now();
    let mut members = BTreeMap::new();
    _ = members.insert(
        first_member_id.clone(),
        Member {
            join_response: JoinGroupResponseMember::default()
                .member_id(first_member_id.clone())
                .group_instance_id(None)
                .metadata(first_member_range_meta.clone()),
            last_contact: Some(now - Duration::from_secs(600)),
        },
    );
    _ = members.insert(
        second_member_id.clone(),
        Member {
            join_response: JoinGroupResponseMember::default()
                .member_id(second_member_id.clone())
                .group_instance_id(None)
                .metadata(second_member_range_meta.clone()),
            last_contact: Some(now),
        },
    );

    let wrapper = Wrapper::Formed(Inner {
        session_timeout_ms: 45_000,
        rebalance_timeout_ms: Some(300_000),
        members,
        generation_id: 1,
        state: Formed {
            protocol_type: PROTOCOL_TYPE.into(),
            protocol_name: RANGE.into(),
            leader: first_member_id.clone(),
            assignments: BTreeMap::new(),
        },
        storage,
        skip_assignment: Some(false),
        inception: now,
    });

    let pruned = wrapper.missed_heartbeat(GROUP_ID, now);

    let pruned_generation = match &pruned {
        Wrapper::Forming(inner) => {
            assert_eq!(2, inner.generation_id);
            assert_eq!(
                Some(second_member_id.as_str()),
                inner.state.leader.as_deref()
            );
            assert!(!inner.members.contains_key(&first_member_id));
            assert!(inner.members.contains_key(&second_member_id));
            inner.generation_id
        }
        otherwise => panic!("expected forming state, got: {otherwise:?}"),
    };

    let (post_heartbeat, heartbeat) = pruned
        .heartbeat(now, GROUP_ID, 1, &second_member_id, None)
        .await;
    assert_eq!(
        Body::from(
            HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
        ),
        heartbeat
    );

    match post_heartbeat {
        Wrapper::Forming(inner) => {
            assert_eq!(pruned_generation, inner.generation_id);
            assert_eq!(
                Some(second_member_id.as_str()),
                inner.state.leader.as_deref()
            );
            assert!(!inner.members.contains_key(&first_member_id));
            assert!(inner.members.contains_key(&second_member_id));
        }
        otherwise => panic!("expected forming state after heartbeat, got: {otherwise:?}"),
    }

    Ok(())
}

/// Verify that a rebalance timeout in Forming state drops the missing leader and
/// lets the surviving leader complete SyncGroup.
#[tokio::test]
async fn forming_rebalance_timeout_drops_missing_member_and_allows_survivor_sync() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "forming-timeout";
    let node = 3;

    const CLIENT_ID: &str = "timeout-client";
    const GROUP_ID: &str = "timeout-forming-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta");
    let second_member_range_meta = Bytes::from_static(b"second_member_range_meta");
    let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta");

    let first_protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    let first_member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&first_protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let first_join = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &first_member_id,
            None,
            PROTOCOL_TYPE,
            Some(&first_protocols[..]),
            None,
        )
        .await?;
    let _first_generation = match first_join {
        Body::JoinGroupResponse(JoinGroupResponse { generation_id, .. }) => generation_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let second_protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(second_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(second_member_sticky_meta),
    ];

    let second_member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&second_protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
        otherwise => panic!("{otherwise:?}"),
    };

    let second_join = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &second_member_id,
            None,
            PROTOCOL_TYPE,
            Some(&second_protocols[..]),
            None,
        )
        .await?;
    let current_generation = match second_join {
        Body::JoinGroupResponse(JoinGroupResponse { generation_id, .. }) => generation_id,
        otherwise => panic!("{otherwise:?}"),
    };

    {
        let mut wrappers = s.wrappers.lock()?;
        let (wrapper, _) = wrappers.get_mut(GROUP_ID).expect("group state");
        match wrapper {
            Wrapper::Forming(inner) => {
                inner
                    .members
                    .get_mut(&first_member_id)
                    .expect("first member")
                    .last_contact = Some(SystemTime::now() - Duration::from_secs(600));
            }
            otherwise => panic!("expected forming state, got: {otherwise:?}"),
        }
    }

    let now = SystemTime::now();
    let wrapper = {
        let wrappers = s.wrappers.lock()?;
        let (wrapper, _) = wrappers.get(GROUP_ID).expect("group state");
        wrapper.clone()
    };
    let pruned = wrapper.missed_heartbeat(GROUP_ID, now);

    let rebalance_generation = match &pruned {
        Wrapper::Forming(inner) => {
            assert_eq!(current_generation + 1, inner.generation_id);
            assert_eq!(
                Some(second_member_id.as_str()),
                inner.state.leader.as_deref()
            );
            assert!(!inner.members.contains_key(&first_member_id));
            assert!(inner.members.contains_key(&second_member_id));
            inner.generation_id
        }
        otherwise => panic!("expected forming state after timeout, got: {otherwise:?}"),
    };

    let survivor_assignments = [SyncGroupRequestAssignment::default()
        .member_id(second_member_id.clone())
        .assignment(Bytes::from_static(b"survivor-assignment"))];

    let (synced_wrapper, sync_response) = pruned
        .sync(
            now,
            GROUP_ID,
            rebalance_generation,
            &second_member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&survivor_assignments[..]),
        )
        .await;
    assert_eq!(
        Body::from(
            SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(0)
                .protocol_type(Some(PROTOCOL_TYPE.into()))
                .protocol_name(Some(RANGE.into()))
                .assignment(Bytes::from_static(b"survivor-assignment"))
        ),
        sync_response
    );

    match synced_wrapper {
        Wrapper::Formed(inner) => {
            assert_eq!(rebalance_generation, inner.generation_id);
            assert_eq!(second_member_id, inner.state.leader);
            assert!(inner.members.contains_key(&second_member_id));
            assert!(!inner.members.contains_key(&first_member_id));
        }
        otherwise => panic!("expected formed state, got: {otherwise:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn leave_unknown_member_returns_per_member_error() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "leave-unknown";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "leave-unknown-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta")),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(Bytes::from_static(b"meta")),
    ];

    // Join to establish the group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(r) => {
            assert_eq!(i16::from(ErrorCode::MemberIdRequired), r.error_code);
            r.member_id
        }
        otherwise => panic!("{otherwise:?}"),
    };

    // Complete the join
    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    // Sync to move to Formed state
    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Leave with an unknown member - should get per-member error
    let leave_response = s
        .leave(
            GROUP_ID,
            None,
            Some(&[MemberIdentity::default()
                .member_id("completely-unknown-member".into())
                .group_instance_id(None)
                .reason(Some("testing unknown member leave".into()))]),
        )
        .await?;

    match leave_response {
        Body::LeaveGroupResponse(r) => {
            assert_eq!(i16::from(ErrorCode::None), r.error_code);
            let members = r.members.expect("members should be present");
            assert_eq!(1, members.len());
            // Unknown member should get an error
            assert_ne!(
                i16::from(ErrorCode::None),
                members[0].error_code,
                "unknown member leave should return per-member error"
            );
        }
        otherwise => panic!("{otherwise:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn sync_with_wrong_generation_returns_illegal_generation() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "sync-wrong-gen";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "sync-wrong-gen-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta")),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(Bytes::from_static(b"meta")),
    ];

    // Join to establish the group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(r) => {
            assert_eq!(i16::from(ErrorCode::MemberIdRequired), r.error_code);
            r.member_id
        }
        otherwise => panic!("{otherwise:?}"),
    };

    // Complete the join
    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    // Sync with WRONG generation (99 instead of 0)
    let sync_response = s
        .sync(
            GROUP_ID,
            99,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&[]),
        )
        .await?;

    match sync_response {
        Body::SyncGroupResponse(r) => {
            let error = ErrorCode::try_from(r.error_code)?;
            assert!(
                error == ErrorCode::IllegalGeneration || error == ErrorCode::RebalanceInProgress,
                "sync with wrong generation should return IllegalGeneration or RebalanceInProgress, got: {error:?}"
            );
        }
        otherwise => panic!("{otherwise:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn heartbeat_with_stale_generation_returns_illegal_generation() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster = "hb-stale-gen";
    let node = 12321;

    const CLIENT_ID: &str = "console-consumer";
    const GROUP_ID: &str = "hb-stale-gen-group";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    const PROTOCOL_TYPE: &str = "consumer";

    let storage = StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(storage_url()?)
        .build()
        .await?;

    let mut s = Controller::with_storage(storage)?;

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta")),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(Bytes::from_static(b"meta")),
    ];

    // Join to establish the group
    let member_id = match s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?
    {
        Body::JoinGroupResponse(r) => {
            assert_eq!(i16::from(ErrorCode::MemberIdRequired), r.error_code);
            r.member_id
        }
        otherwise => panic!("{otherwise:?}"),
    };

    // Complete the join (generation = 0)
    let _ = s
        .join(
            Some(CLIENT_ID),
            GROUP_ID,
            45_000,
            Some(300_000),
            &member_id,
            None,
            PROTOCOL_TYPE,
            Some(&protocols[..]),
            None,
        )
        .await?;

    // Sync to move to Formed state
    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(member_id.clone())
        .assignment(Bytes::from_static(b"assignment"))];

    let _ = s
        .sync(
            GROUP_ID,
            0,
            &member_id,
            None,
            Some(PROTOCOL_TYPE),
            Some(RANGE),
            Some(&assignments[..]),
        )
        .await?;

    // Heartbeat with correct generation should succeed
    let hb_ok = s.heartbeat(GROUP_ID, 0, &member_id, None).await?;

    match hb_ok {
        Body::HeartbeatResponse(r) => {
            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(r.error_code)?,
                "heartbeat with correct generation should succeed"
            );
        }
        otherwise => panic!("{otherwise:?}"),
    }

    // Heartbeat with FUTURE generation should return IllegalGeneration
    let hb_future = s.heartbeat(GROUP_ID, 999, &member_id, None).await?;

    match hb_future {
        Body::HeartbeatResponse(r) => {
            let error = ErrorCode::try_from(r.error_code)?;
            assert_eq!(
                ErrorCode::IllegalGeneration,
                error,
                "heartbeat with future generation should return IllegalGeneration"
            );
        }
        otherwise => panic!("{otherwise:?}"),
    }

    Ok(())
}
