use std::{collections::VecDeque, fmt};

use bytes::{BufMut, Bytes, BytesMut};
use jansu_model::FieldMeta;
use serde::Serializer;
use tracing::{debug, instrument};

use super::context::{Container, Kind, Meta, PARSE_DEPTH};
use crate::{Error, Result, RootMessageMeta};

/// Serialize the serde data model into the Kafka protocol.
pub struct Encoder {
    pub(super) working: BytesMut,
    pub(super) containers: VecDeque<Container>,
    pub(super) field: Option<&'static str>,
    pub(super) kind: Option<Kind>,
    pub(super) api_key: Option<i16>,
    pub(super) api_version: Option<i16>,
    pub(super) meta: Meta,
}

impl From<Encoder> for BytesMut {
    fn from(encoder: Encoder) -> Self {
        encoder.working
    }
}

impl From<Encoder> for Bytes {
    fn from(encoder: Encoder) -> Self {
        BytesMut::from(encoder).freeze()
    }
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl Encoder {
    pub(crate) fn request(working: BytesMut) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Request),
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    pub(crate) fn response(working: BytesMut, api_key: i16, api_version: i16) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Response),
            field: None,
            api_key: Some(api_key),
            api_version: Some(api_version),
            meta: RootMessageMeta::messages()
                .responses()
                .get(&api_key)
                .map_or(Meta::default(), |meta| {
                    let mut parse = VecDeque::with_capacity(PARSE_DEPTH);
                    parse.push_front(meta.fields.into());

                    Meta {
                        message: Some(*meta),
                        parse,
                        ..Default::default()
                    }
                }),
        }
    }

    pub fn new(working: BytesMut) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: None,
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    #[instrument(skip(self))]
    pub(super) fn field_meta(&self, name: &str) -> Option<&'static FieldMeta> {
        debug!(
            parse_front = ?self.meta.parse.front().and_then(|front| front.field(name)),
            meta = ?self.meta.message.and_then(|mm| mm.field(name))
        );

        self.meta
            .parse
            .front()
            .and_then(|front| front.field(name))
            .or(self.meta.message.and_then(|mm| mm.field(name)))
    }

    #[allow(dead_code)]
    pub(super) fn field_name(&self) -> String {
        self.containers.iter().fold(
            self.field.map_or(String::new(), str::to_owned),
            |acc, container| {
                if acc.is_empty() {
                    container.name()
                } else {
                    format!("{}.{acc}", container.name())
                }
            },
        )
    }

    pub(super) fn unsigned_varint(&mut self, mut v: u32) -> Result<()> {
        const CONTINUATION: u8 = 0b1000_0000;

        while v >= u32::from(CONTINUATION) {
            self.working.put_u8(v as u8 | CONTINUATION);
            v >>= 7;
        }

        self.working.put_u8(v as u8);
        Ok(())
    }

    pub(super) fn in_header(&self) -> bool {
        matches!(
            self.containers.front(),
            Some(Container::StructVariant {
                name: "HeaderMezzanine",
                ..
            })
        )
    }

    #[must_use]
    pub(super) fn is_flexible(&self) -> bool {
        debug!(
            "api_key: {:?}, api_version: {:?}, in_header: {}, is_client_id: {}",
            self.api_key,
            self.api_version,
            self.in_header(),
            self.field.is_some_and(|field| field == "client_id")
        );

        if self.in_header()
            && ((self.kind.is_some_and(|kind| kind == Kind::Request)
                && self.field.is_some_and(|field| field == "client_id"))
                || (self.kind.is_some_and(|kind| kind == Kind::Response)
                    && self.api_key.is_some_and(|api_key| api_key == 18)))
        {
            false
        } else {
            self.meta.message.is_some_and(|meta| {
                self.api_version
                    .is_some_and(|api_version| meta.is_flexible(api_version))
            })
        }
    }

    pub(super) fn is_nullable(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.is_nullable(api_version))
        })
    }

    #[must_use]
    pub(super) fn is_valid(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.version.within(api_version))
        })
    }

    #[must_use]
    pub(super) fn is_sequence(&self) -> bool {
        self.meta
            .field
            .is_some_and(|field| field.kind.is_sequence())
    }

    #[must_use]
    pub(super) fn is_structure(&self) -> bool {
        self.meta.field.is_some_and(|field| field.is_structure())
    }

    #[must_use]
    pub(super) fn is_string(&self) -> bool {
        self.in_header() && self.field.is_some_and(|field| field == "client_id")
            || self.meta.field.is_some_and(|field| field.kind.is_string())
    }

    #[must_use]
    pub(super) fn is_records(&self) -> bool {
        self.meta.field.is_some_and(|field| field.kind.is_records())
    }

    /// Serialize a Kafka schema `default` when serde emits `None` for an `Option` field
    /// that is present on the wire for this API version (flexible messages).
    pub(super) fn serialize_schema_default_for_none(&mut self) -> Result<bool> {
        if !(self.is_valid() && self.is_flexible()) {
            return Ok(false);
        }
        let Some(field) = self.meta.field else {
            return Ok(false);
        };
        let Some(spec) = field.default else {
            return Ok(false);
        };
        let kind = field.kind.name();
        if kind == "bool" {
            return match spec {
                "true" => self.serialize_bool(true).map(|()| true),
                "false" => self.serialize_bool(false).map(|()| true),
                _ => Ok(false),
            };
        }

        let v = parse_kafka_int_default(spec).map_err(|e| {
            Error::Message(format!(
                "invalid Kafka numeric default {spec:?} for {kind}: {e}"
            ))
        })?;

        match kind {
            "int8" => self
                .serialize_i8(i8::try_from(v).map_err(|_| {
                    Error::Message(format!("default {spec} out of range for int8"))
                })?),
            "int16" => {
                self.serialize_i16(i16::try_from(v).map_err(|_| {
                    Error::Message(format!("default {spec} out of range for int16"))
                })?)
            }
            "int32" => {
                self.serialize_i32(i32::try_from(v).map_err(|_| {
                    Error::Message(format!("default {spec} out of range for int32"))
                })?)
            }
            "int64" => {
                self.serialize_i64(i64::try_from(v).map_err(|_| {
                    Error::Message(format!("default {spec} out of range for int64"))
                })?)
            }
            "uint16" => {
                self.serialize_u16(u16::try_from(v).map_err(|_| {
                    Error::Message(format!("default {spec} out of range for uint16"))
                })?)
            }
            _ => return Ok(false),
        }?;
        Ok(true)
    }
}

fn parse_kafka_int_default(spec: &str) -> std::result::Result<i128, std::num::ParseIntError> {
    if let Some(hex) = spec.strip_prefix("0x").or(spec.strip_prefix("0X")) {
        i128::from_str_radix(hex, 16)
    } else {
        spec.parse::<i128>()
    }
}
