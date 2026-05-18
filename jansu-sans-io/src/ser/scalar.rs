use std::{any::type_name, mem::size_of};

use bytes::{BufMut, BytesMut};
use serde::{Serialize, Serializer};
use tracing::{debug, instrument};

use super::{
    Encoder,
    context::{Container, Kind},
    record_batch::RecordBatchEncoder,
};
use crate::{Encode, Error, Result, RootMessageMeta, primitive::varint::UnsignedVarInt};

impl Serializer for &mut Encoder {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[instrument(skip(self))]
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.working.put_u8(u8::from(v));
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.working.put_i8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        match (self.containers.front(), self.field) {
            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_key"),
            ) => {
                _ = self.api_key.replace(v);

                if let Some(meta) = RootMessageMeta::messages().requests().get(&v) {
                    self.meta.message = Some(*meta);
                    self.meta.parse.push_front(meta.fields.into());
                }
            }

            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_version"),
            ) => {
                _ = self.api_version.replace(v);
            }

            _ => (),
        }

        self.working.put_i16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.working.put_i32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.working.put_i64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.working.put_u8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.working.put_u16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.working.put_u32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.working.put_u64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.working.put_f32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.working.put_f64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{v}")))
    }

    #[instrument(skip(self))]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            v.len()
                .try_into()
                .map_err(Into::into)
                .and_then(|len| self.serialize_i16(len))?;

            self.working.put_slice(v.as_bytes());
        } else if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_i16(len))?;
            }

            self.working.put_slice(v.as_bytes());
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        debug!(
            ?v,
            is_valid = self.is_valid(),
            is_flexible = self.is_flexible()
        );

        if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_u32(len))?;
            }

            self.working.put_slice(v);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        debug!(
            name = self.field_name(),
            is_valid = self.is_valid(),
            is_nullable = self.is_nullable(),
            is_structure = self.is_structure(),
            is_sequence = self.is_sequence(),
            is_flexible = self.is_flexible(),
        );

        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            self.serialize_i16(-1)
        } else if self.is_valid() && self.is_records() {
            if self.is_flexible() {
                self.unsigned_varint(1)
            } else {
                self.serialize_i32(0)
            }
        } else if self.is_valid() && self.is_nullable() {
            if self.is_structure() && !self.is_sequence() {
                self.serialize_i8(-1)
            } else if self.is_flexible() {
                self.unsigned_varint(0)
            } else if self.is_sequence() {
                self.serialize_i32(-1)
            } else if self.is_string() {
                self.serialize_i16(-1)
            } else {
                Ok(())
            }
        } else {
            let _ = self.serialize_schema_default_for_none()?;
            Ok(())
        }
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        debug!(
            is_tag_buffer = self.field.is_some_and(|field| field == "tag_buffer"),
            is_flexible = self.is_flexible(),
            is_records = self.is_records(),
        );

        if self.field.is_some_and(|field| field == "tag_buffer") && !self.is_flexible() {
            Ok(())
        } else if self.is_records() {
            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len()
            );

            let records = {
                let records = self
                    .working
                    .split_off(self.working.len() + size_of::<u32>());
                debug!(
                    records_capacity = records.capacity(),
                    records_len = records.len(),
                );

                let mut e = RecordBatchEncoder::new(records);
                value.serialize(&mut e)?;
                BytesMut::from(e)
            };

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
                records_capacity = records.capacity(),
                records_len = records.len(),
            );

            let length = u32::try_from(records.len())?;

            if self.is_flexible() {
                let encoded = UnsignedVarInt(length + 1).encode()?;
                self.working.put(encoded);
            } else {
                self.working.put_u32(length);
            }

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
            );

            self.working.unsplit(records);

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
            );

            Ok(())
        } else {
            value.serialize(self)
        }
    }

    #[instrument(skip_all)]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }

    #[instrument(skip(self))]
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(name.into()))
    }

    #[instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!(
            "{name}:{variant_index}:{variant}"
        )))
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    #[instrument(skip(self))]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if self.is_valid()
            && let Some(len) = len
        {
            if self.is_flexible() {
                (len + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.unsigned_varint(l))?;
            } else {
                len.try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.serialize_i32(l))?;
            }
        }

        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.containers.push_front(Container::Struct { name, len });

        if let Some(fm) = self.field_meta(name) {
            self.meta.field = Some(fm);
            self.meta.parse.push_front(fm.fields.into());
        }

        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.containers.push_front(Container::StructVariant {
            name,
            variant_index,
            variant,
            len,
        });

        Ok(self)
    }
}
