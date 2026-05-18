use std::any::{type_name, type_name_of_val};

use bytes::{BufMut, Bytes, BytesMut};
use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use tracing::instrument;

use crate::Error;

pub struct RecordBatchEncoder {
    working: BytesMut,
}

impl RecordBatchEncoder {
    pub fn new(working: BytesMut) -> Self {
        Self { working }
    }
}

impl From<RecordBatchEncoder> for BytesMut {
    fn from(value: RecordBatchEncoder) -> Self {
        value.working
    }
}

impl From<RecordBatchEncoder> for Bytes {
    fn from(value: RecordBatchEncoder) -> Self {
        BytesMut::from(value).into()
    }
}

impl Serializer for &mut RecordBatchEncoder {
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
    fn serialize_bool(self, v: bool) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u8(u8::from(v));
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_f32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_f64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("char serialization is not used in Kafka record batch encoding: {v}")
    }

    #[instrument(skip(self))]
    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("str serialization is not used in Kafka record batch encoding: {v}")
    }

    #[instrument(skip(self))]
    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    #[instrument(skip_all, fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!(
            "serialize_some is not used in Kafka record batch encoding: {}",
            type_name_of_val(value)
        )
    }

    #[instrument(skip_all)]
    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("serialize_unit is not used in Kafka record batch encoding")
    }

    #[instrument(skip(self))]
    fn serialize_unit_struct(
        self,
        name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("serialize_unit_struct is not used in Kafka record batch encoding")
    }

    #[instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!(
            "serialize_unit_variant is not used in Kafka record batch encoding: {name}, {variant_index}, {variant}"
        )
    }

    #[instrument(skip_all, fields(name, value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!(
            "serialize_newtype_struct is not used in Kafka record batch encoding: {}, {name}",
            type_name_of_val(value)
        )
    }

    #[instrument(skip_all, fields(name, variant_index, variant, value = type_name::<T>()))]
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!(
            "serialize_newtype_variant is not used in Kafka record batch encoding: {}, {name}, {variant_index}, {variant}",
            type_name_of_val(value)
        );
    }

    #[instrument(skip(self))]
    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        unreachable!("serialize_tuple is not used in Kafka record batch encoding: {len}")
    }

    #[instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        unreachable!(
            "serialize_tuple_struct is not used in Kafka record batch encoding: {name}, {len}"
        )
    }

    #[instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        unreachable!(
            "serialize_tuple_variant is not used in Kafka record batch encoding: {name}, {variant_index}, {variant}, {len}"
        )
    }

    #[instrument(skip(self))]
    fn serialize_map(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        unreachable!("serialize_map is not used in Kafka record batch encoding: {len:?}")
    }

    #[instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self)
    }
}

impl SerializeSeq for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(type_name = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!("serialize_element is not used in Kafka record batch encoding")
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("SerializeTuple::end is not used in Kafka record batch encoding")
    }
}

impl SerializeTupleVariant for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!(
            "SerializeTupleVariant::serialize_field is not used in Kafka record batch encoding"
        )
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("SerializeTupleVariant::end is not used in Kafka record batch encoding")
    }
}

impl SerializeMap for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_key<T>(&mut self, _key: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!("SerializeMap::serialize_key is not used in Kafka record batch encoding")
    }

    fn serialize_value<T>(&mut self, _value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!("SerializeMap::serialize_value is not used in Kafka record batch encoding")
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("SerializeMap::end is not used in Kafka record batch encoding")
    }
}

impl SerializeStruct for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(key, type_name = type_name::<T>()))]
    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unreachable!(
            "SerializeTupleStruct::serialize_field is not used in Kafka record batch encoding"
        )
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        unreachable!("SerializeTupleStruct::end is not used in Kafka record batch encoding")
    }
}

impl SerializeStructVariant for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(key, type_name = type_name::<T>()))]
    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
