use std::collections::HashMap;
use std::str::FromStr;

use serde_json::{Value, json};
use syn::{Expr, Ident};

use crate::kind::type_mapping;
use crate::wv::{As, Wv};
use crate::{
    CommonStruct, Error, Field, Kind, Listener, Message, MessageKind, Result, Version, VersionRange,
};

const PRIMITIVES: [&str; 10] = [
    "bool", "bytes", "float64", "int16", "int32", "int64", "int8", "string", "uint16", "uuid",
];

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Header {
    name: String,
    valid: VersionRange,
    flexible: VersionRange,
    fields: Vec<Field>,
}

impl<'a> TryFrom<&Wv<'a>> for Header {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let fields: &[Value] = value.as_a("fields")?;

        Ok(Header {
            name: value.as_a("name")?,
            valid: value.as_a("validVersions")?,
            flexible: value.as_a("flexibleVersions")?,
            fields: fields.iter().try_fold(Vec::new(), |mut acc, field| {
                Field::try_from(&Wv::from(field)).map(|f| {
                    acc.push(f);
                    acc
                })
            })?,
        })
    }
}

#[test]
fn type_mapping_test() {
    assert_eq!("bytes::Bytes", type_mapping("bytes"));
    assert_eq!("f64", type_mapping("float64"));
    assert_eq!("i16", type_mapping("int16"));
    assert_eq!("i32", type_mapping("int32"));
    assert_eq!("i64", type_mapping("int64"));
    assert_eq!("i8", type_mapping("int8"));
    assert_eq!("String", type_mapping("string"));
    assert_eq!("u16", type_mapping("uint16"));
    assert_eq!("[u8; 16]", type_mapping("uuid"));

    assert_eq!("i16", type_mapping("[]int16"));
    assert_eq!("SomeType", type_mapping("[]SomeType"));

    assert_eq!("SomeType", type_mapping("SomeType"));
}

#[test]
fn kind_is_sequence() {
    for primitive in PRIMITIVES {
        let k = Kind::new(primitive);
        assert!(!k.is_sequence());
    }

    let sequences = vec!["[]int16", "[]string", "[]SomeType"];

    for sequence in sequences {
        let k = Kind::new(sequence);
        assert!(k.is_sequence());
    }
}

#[test]
fn kind_is_sequence_of_primitive() {
    for primitive in PRIMITIVES {
        let k = Kind::new(primitive);
        assert!(!k.is_sequence_of_primitive());
    }

    let primitive_sequences: Vec<String> = PRIMITIVES
        .iter()
        .map(|primitive| format!("[]{primitive}"))
        .collect();

    for sequence in primitive_sequences {
        let k = Kind::new(sequence.as_str());
        assert!(k.is_sequence_of_primitive());
    }

    let sequences = vec!["[]SomeType", "[]OtherType"];

    for sequence in sequences {
        let k = Kind::new(sequence);
        assert!(!k.is_sequence_of_primitive());
    }
}

#[test]
fn kind_is_primitive() {
    for primitive in PRIMITIVES {
        let k = Kind::new(primitive);
        assert!(k.is_primitive());
    }

    let primitive_sequences: Vec<String> = PRIMITIVES
        .iter()
        .map(|primitive| format!("[]{primitive}"))
        .collect();

    for sequence in primitive_sequences {
        let k = Kind::new(sequence.as_str());
        assert!(!k.is_primitive());
    }
}

#[test]
fn listener_from_value() -> Result<()> {
    assert_eq!(Listener::ZkBroker, Listener::try_from(&json!("zkBroker"))?);

    assert_eq!(Listener::Broker, Listener::try_from(&json!("broker"))?);

    assert_eq!(
        Listener::Controller,
        Listener::try_from(&json!("controller"))?
    );

    Ok(())
}

#[test]
fn message_kind_from_value() -> Result<()> {
    assert_eq!(
        MessageKind::Request,
        MessageKind::try_from(&json!({
            "type": "request"
        }
        ))?
    );

    assert_eq!(
        MessageKind::Response,
        MessageKind::try_from(&json!({
            "type": "response"
        }
        ))?
    );

    Ok(())
}

#[test]
fn version_range_from_str() -> Result<()> {
    assert_eq!(
        VersionRange { start: -2, end: -1 },
        VersionRange::from_str("none")?
    );

    assert_eq!(
        VersionRange {
            start: 3,
            end: i16::MAX
        },
        VersionRange::from_str("3+")?
    );

    assert_eq!(
        VersionRange { start: 6, end: 9 },
        VersionRange::from_str("6-9")?
    );

    assert_eq!(
        VersionRange { start: 1, end: 1 },
        VersionRange::from_str("1")?
    );

    Ok(())
}

#[test]
fn version_range_within() {
    {
        let range = VersionRange { start: 0, end: 0 };
        assert!(!range.within(i16::MIN));
        assert!(range.within(0));
        assert!(!range.within(1));
        assert!(!range.within(i16::MAX));
    }

    {
        let range = VersionRange {
            start: 3,
            end: i16::MAX,
        };
        assert!(!range.within(i16::MIN));
        assert!(!range.within(2));
        assert!(range.within(3));
        assert!(range.within(i16::MAX));
    }

    {
        let range = VersionRange { start: 6, end: 9 };
        assert!(!range.within(i16::MIN));
        assert!(!range.within(5));
        assert!(range.within(6));
        assert!(range.within(7));
        assert!(range.within(8));
        assert!(range.within(9));
        assert!(!range.within(10));
        assert!(!range.within(i16::MAX));
    }
}

#[test]
fn field_from_value() -> Result<()> {
    assert_eq!(
        Field {
            name: String::from("Topics"),
            kind: Kind(String::from("[]CreatableTopic")),
            about: Some(String::from("The topics to create.")),
            versions: VersionRange::from_str("0+")?,
            map_key: None,
            nullable: None,
            tag: None,
            tagged: None,
            entity_type: None,
            default: None,
            fields: Some(vec![Field {
                name: String::from("Name"),
                kind: Kind(String::from("string")),
                versions: VersionRange::from_str("0+")?,
                map_key: Some(true),
                entity_type: Some(String::from("topicName")),
                about: Some(String::from("The topic name.")),
                default: None,
                nullable: None,
                tag: None,
                tagged: None,
                fields: None,
            }]),
        },
        serde_json::from_str::<Value>(
            r#"
            {
                "name": "Topics",
                "type": "[]CreatableTopic",
                "versions": "0+",
                "about": "The topics to create.",
                 "fields": [
                    { "name": "Name",
                      "type": "string",
                      "versions": "0+",
                      "mapKey": true,
                      "entityType": "topicName",
                      "about": "The topic name."
                    }]
            }
            "#
        )
        .map_err(Into::into)
        .and_then(|v| Field::try_from(&Wv::from(&v)))?
    );

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn tagged_field_from_value() -> Result<()> {
    assert_eq!(
        Field {
            name: "SupportedFeatures".into(),
            kind: Kind("[]SupportedFeatureKey".into()),
            about: Some("Features supported by the broker.".into()),
            versions: VersionRange {
                start: 3,
                end: 32767
            },
            map_key: None,
            nullable: None,
            tag: Some(0),
            tagged: Some(VersionRange {
                start: 3,
                end: 32767
            }),
            entity_type: None,
            default: None,
            fields: Some(
                [
                    Field {
                        name: "Name".into(),
                        kind: Kind("string".into()),
                        about: Some("The name of the feature.".into()),
                        versions: VersionRange {
                            start: 3,
                            end: 32767
                        },
                        map_key: Some(true),
                        nullable: None,
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    },
                    Field {
                        name: "MinVersion".into(),
                        kind: Kind("int16".into()),
                        about: Some("The minimum supported version for the feature.".into()),
                        versions: VersionRange {
                            start: 3,
                            end: 32767
                        },
                        map_key: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    },
                    Field {
                        name: "MaxVersion".into(),
                        kind: Kind("int16".into()),
                        about: Some("The maximum supported version for the feature.".into()),
                        versions: VersionRange {
                            start: 3,
                            end: 32767
                        },
                        map_key: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    }
                ]
                .into()
            )
        },
        serde_json::from_str::<Value>(
            r#"
            { "name":  "SupportedFeatures",
              "type": "[]SupportedFeatureKey",
              "ignorable": true,
              "versions":  "3+",
              "tag": 0,
              "taggedVersions": "3+",
              "about": "Features supported by the broker.",
              "fields":  [
                            { "name": "Name",
                              "type": "string",
                              "versions": "3+",
                              "mapKey": true,
                              "about": "The name of the feature." },
                            { "name": "MinVersion",
                              "type": "int16",
                              "versions": "3+",
                              "about": "The minimum supported version for the feature." },
                            { "name": "MaxVersion",
                              "type": "int16",
                              "versions": "3+",
                              "about": "The maximum supported version for the feature." }
                         ]
            }
            "#
        )
        .map_err(Into::into)
        .and_then(|v| Field::try_from(&Wv::from(&v)))?
    );

    Ok(())
}

#[test]
fn untagged_message() -> Result<()> {
    let m = serde_json::from_str::<Value>(
        r#"
        {
          "apiKey": 25,
          "type": "request",
          "listeners": ["zkBroker", "broker"],
          "name": "AddOffsetsToTxnRequest",
          "validVersions": "0-3",
          "flexibleVersions": "3+",
          "fields": [
            { "name": "TransactionalId", "type": "string", "versions": "0+", "entityType": "transactionalId",
              "about": "The transactional id corresponding to the transaction."},
            { "name": "ProducerId", "type": "int64", "versions": "0+", "entityType": "producerId",
              "about": "Current producer id in use by the transactional id." },
            { "name": "ProducerEpoch", "type": "int16", "versions": "0+",
              "about": "Current epoch associated with the producer id." },
            { "name": "GroupId", "type": "string", "versions": "0+", "entityType": "groupId",
              "about": "The unique group identifier." }
          ]
        }
        "#,
    ).map_err(Into::into)
    .and_then(|v| Message::try_from(&Wv::from(&v)))?;

    assert_eq!(MessageKind::Request, m.kind());

    assert!(!m.has_tags());

    Ok(())
}

#[test]
fn tagged_message() -> Result<()> {
    let m = Message::try_from(&Wv::from(&json!(
        {
          "apiKey": 63,
          "type": "request",
          "listeners": ["controller"],
          "name": "BrokerHeartbeatRequest",
          "validVersions": "0-1",
          "flexibleVersions": "0+",
          "fields": [
            { "name": "BrokerId", "type": "int32", "versions": "0+", "entityType": "brokerId",
              "about": "The broker ID." },
            { "name": "BrokerEpoch", "type": "int64", "versions": "0+", "default": "-1",
              "about": "The broker epoch." },
            { "name": "CurrentMetadataOffset", "type": "int64", "versions": "0+",
              "about": "The highest metadata offset which the broker has reached." },
            { "name": "WantFence", "type": "bool", "versions": "0+",
              "about": "True if the broker wants to be fenced, false otherwise." },
            { "name": "WantShutDown", "type": "bool", "versions": "0+",
              "about": "True if the broker wants to be shut down, false otherwise." },
            { "name": "OfflineLogDirs", "type":  "[]uuid", "versions": "1+", "taggedVersions": "1+", "tag": "0",
              "about": "Log directories that failed and went offline." }
          ]
        }
    )))?;

    assert_eq!(MessageKind::Request, m.kind());
    assert!(m.has_tags());

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn message_from_value() -> Result<()> {
    assert_eq!(
        Message {
            api_key: 19,
            kind: MessageKind::Request,
            listeners: Some(vec![
                Listener::ZkBroker,
                Listener::Broker,
                Listener::Controller
            ]),
            name: String::from("CreateTopicsRequest"),
            versions: Version {
                valid: VersionRange::from_str("0-7")?,
                superseded: Some(VersionRange::from_str("0-1")?),
                flexible: VersionRange::from_str("5+")?,
            },
            common_structs: Some(vec![CommonStruct {
                name: String::from("AddPartitionsToTxnTopic"),
                fields: vec![
                    Field {
                        name: String::from("Name"),
                        kind: Kind(String::from("string")),
                        versions: VersionRange::from_str("0+")?,
                        map_key: Some(true),
                        nullable: None,
                        tag: None,
                        tagged: None,
                        default: None,
                        fields: None,
                        entity_type: Some(String::from("topicName")),
                        about: Some(String::from("The name of the topic.")),
                    },
                    Field {
                        name: String::from("Partitions"),
                        kind: Kind(String::from("[]int32")),
                        versions: VersionRange::from_str("0+")?,
                        about: Some(String::from(
                            "The partition indexes to add to the transaction"
                        )),
                        map_key: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        default: None,
                        fields: None,
                        entity_type: None,
                    }
                ],
            }]),
            fields: vec![Field {
                name: String::from("Topics"),
                kind: Kind(String::from("[]CreatableTopic")),
                about: Some(String::from("The topics to create.")),
                versions: VersionRange::from_str("0+")?,
                map_key: None,
                nullable: None,
                tag: None,
                tagged: None,
                entity_type: None,
                default: None,
                fields: Some(vec![Field {
                    name: String::from("Name"),
                    kind: Kind(String::from("string")),
                    versions: VersionRange::from_str("0+")?,
                    map_key: Some(true),
                    entity_type: Some(String::from("topicName")),
                    about: Some(String::from("The topic name.")),
                    default: None,
                    nullable: None,
                    tag: None,
                    tagged: None,
                    fields: None,
                }])
            }],
        },
        serde_json::from_str::<Value>(
            r#"
            {
                "apiKey": 19,
                "type": "request",
                "listeners": ["zkBroker", "broker", "controller"],
                "name": "CreateTopicsRequest",
                "validVersions": "0-7",
                "deprecatedVersions": "0-1",
                "flexibleVersions": "5+",
                "fields": [
                    {"name": "Topics",
                    "type": "[]CreatableTopic",
                    "versions": "0+",
                    "about": "The topics to create.",
                     "fields": [
                        { "name": "Name",
                          "type": "string",
                          "versions": "0+",
                          "mapKey": true,
                          "entityType": "topicName",
                          "about": "The topic name."
                    }]}],
                    "commonStructs": [
                        { "name": "AddPartitionsToTxnTopic",
                          "versions": "0+",
                          "fields": [
                            { "name": "Name",
                              "type": "string",
                              "versions": "0+",
                              "mapKey": true,
                              "entityType": "topicName",
                              "about": "The name of the topic."
                            },
                            { "name": "Partitions",
                              "type": "[]int32",
                              "versions": "0+",
                              "about": "The partition indexes to add to the transaction"
                            }]}]
            }
            "#
        )
        .map_err(Into::into)
        .and_then(|v| Message::try_from(&Wv::from(&v)))?
    );

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn header_from_value() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
          "type": "header",
          "name": "RequestHeader",
          "validVersions": "0-2",
          "flexibleVersions": "2+",
          "fields": [
            { "name": "RequestApiKey", "type": "int16", "versions": "0+",
              "about": "The API key of this request." },
            { "name": "RequestApiVersion", "type": "int16", "versions": "0+",
              "about": "The API version of this request." },
            { "name": "CorrelationId", "type": "int32", "versions": "0+",
              "about": "The correlation ID of this request." },

            { "name": "ClientId", "type": "string", "versions": "1+", "nullableVersions": "1+", "ignorable": true,
              "flexibleVersions": "none", "about": "The client ID string." }
          ]
        }
        "#,
    )?;

    let wv = Wv::from(&v);

    assert_eq!(
        Header {
            name: String::from("RequestHeader"),
            valid: VersionRange { start: 0, end: 2 },
            flexible: VersionRange {
                start: 2,
                end: i16::MAX
            },
            fields: vec![
                Field {
                    name: String::from("RequestApiKey"),
                    kind: Kind(String::from("int16")),
                    about: Some(String::from("The API key of this request.")),
                    versions: VersionRange {
                        start: 0,
                        end: 32767
                    },
                    map_key: None,
                    nullable: None,
                    tag: None,
                    tagged: None,
                    entity_type: None,
                    default: None,
                    fields: None
                },
                Field {
                    name: String::from("RequestApiVersion"),
                    kind: Kind(String::from("int16")),
                    about: Some(String::from("The API version of this request.")),
                    versions: VersionRange {
                        start: 0,
                        end: 32767
                    },
                    map_key: None,
                    nullable: None,
                    tag: None,
                    tagged: None,
                    entity_type: None,
                    default: None,
                    fields: None
                },
                Field {
                    name: String::from("CorrelationId"),
                    kind: Kind(String::from("int32")),
                    about: Some(String::from("The correlation ID of this request.")),
                    versions: VersionRange {
                        start: 0,
                        end: 32767
                    },
                    map_key: None,
                    nullable: None,
                    tag: None,
                    tagged: None,
                    entity_type: None,
                    default: None,
                    fields: None
                },
                Field {
                    name: String::from("ClientId"),
                    kind: Kind(String::from("string")),
                    about: Some(String::from("The client ID string.")),
                    versions: VersionRange {
                        start: 1,
                        end: 32767
                    },
                    map_key: None,
                    nullable: Some(VersionRange {
                        start: 1,
                        end: 32767
                    }),
                    tag: None,
                    tagged: None,
                    entity_type: None,
                    default: None,
                    fields: None
                }
            ],
        },
        Header::try_from(&wv)?
    );
    Ok(())
}

#[test]
fn parse_expression() -> Result<()> {
    let Expr::Array(expression) =
        syn::parse_str::<Expr>(r#"[(one, "a/b/c"), (abc, 123), (pqr, a::b::c)]"#)?
    else {
        return Err(Error::Message(String::from("expecting an array")));
    };

    let mut mappings = HashMap::new();

    for expression in expression.elems {
        let Expr::Tuple(tuple) = expression else {
            return Err(Error::Message(String::from("expecting a tuple")));
        };

        assert_eq!(2, tuple.elems.len());

        let Expr::Path(ref lhs) = tuple.elems[0] else {
            return Err(Error::Message(String::from(
                "lhs expecting a path expression",
            )));
        };

        let Some(lhs) = lhs.path.get_ident() else {
            return Err(Error::Message(String::from(
                "lhs expecting a path ident expression",
            )));
        };

        _ = mappings.insert(lhs.clone(), tuple.elems[1].clone());
    }

    let one = syn::parse_str::<Ident>("one")?;
    assert!(mappings.contains_key(&one));

    Ok(())
}

#[test]
fn find_coordinator_request() -> Result<()> {
    let m = serde_json::from_str::<Value>(
        r#"
            {
                "apiKey": 10,
                "type": "request",
                "listeners": ["zkBroker", "broker"],
                "name": "FindCoordinatorRequest",
                "validVersions": "0-4",
                "deprecatedVersions": "0",
                "flexibleVersions": "3+",
                "fields": [
                    { "name": "Key", "type": "string", "versions": "0-3",
                    "about": "The coordinator key." },
                    { "name": "KeyType", "type": "int8", "versions": "1+", "default": "0", "ignorable": false,
                    "about": "The coordinator key type. (Group, transaction, etc.)" },
                    { "name": "CoordinatorKeys", "type": "[]string", "versions": "4+",
                    "about": "The coordinator keys." }
                ]
            }
        "#,
    )
    .map_err(Into::into)
    .and_then(|v| Message::try_from(&Wv::from(&v)))?;

    assert!(!m.has_records());
    assert_eq!(MessageKind::Request, m.kind());
    assert_eq!("FindCoordinatorRequest", m.name());
    assert_eq!(
        Version {
            valid: VersionRange { start: 0, end: 4 },
            superseded: Some(VersionRange { start: 0, end: 0 }),
            flexible: VersionRange {
                start: 3,
                end: i16::MAX
            },
        },
        m.version()
    );

    assert_eq!("Key", m.fields()[0].name());
    assert_eq!(Kind::new("string"), m.fields()[0].kind);
    assert_eq!(VersionRange { start: 0, end: 3 }, m.fields()[0].versions());
    assert_eq!(Some("The coordinator key."), m.fields()[0].about());

    Ok(())
}

#[test]
fn fetch_response() -> Result<()> {
    let m = serde_json::from_str::<Value>(
        r#"
            {
                "apiKey": 1,
                "type": "response",
                "name": "FetchResponse",
                "validVersions": "0-16",
                "flexibleVersions": "12+",
                "fields": [
                    { "name": "NodeEndpoints", "type": "[]NodeEndpoint", "versions": "16+", "taggedVersions": "16+", "tag": 0,
                      "about": "Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.", "fields": [
                      { "name": "NodeId", "type": "int32", "versions": "16+",
                        "mapKey": true, "entityType": "brokerId", "about": "The ID of the associated node."},
                      { "name": "Host", "type": "string", "versions": "16+", "about": "The node's hostname." },
                      { "name": "Port", "type": "int32", "versions": "16+", "about": "The node's port." },
                      { "name": "Rack", "type": "string", "versions": "16+", "nullableVersions": "16+", "default": "null",
                        "about": "The rack of the node, or null if it has not been assigned to a rack." }
                    ]}
                ]
            }
        "#,
    )
    .map_err(Into::into)
    .and_then(|v| Message::try_from(&Wv::from(&v)))?;

    assert_eq!(MessageKind::Response, m.kind());
    assert_eq!("FetchResponse", m.name());
    assert_eq!(
        Version {
            valid: VersionRange { start: 0, end: 16 },
            superseded: None,
            flexible: VersionRange {
                start: 12,
                end: i16::MAX
            },
        },
        m.version()
    );

    assert_eq!("NodeEndpoints", m.fields()[0].name());
    assert_eq!(Kind::new("[]NodeEndpoint"), m.fields()[0].kind);
    assert_eq!(
        VersionRange {
            start: 16,
            end: i16::MAX
        },
        m.fields()[0].versions()
    );

    let node_id = &m.fields()[0]
        .fields()
        .expect("test fixture field has sub-fields")[0];

    assert_eq!("NodeId", node_id.name());
    assert_eq!(Kind::new("int32"), node_id.kind);
    assert_eq!(
        VersionRange {
            start: 16,
            end: i16::MAX
        },
        node_id.versions()
    );

    Ok(())
}
