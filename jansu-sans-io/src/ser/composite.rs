use std::any::{type_name, type_name_of_val};

use serde::{
    Serialize,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use tracing::{debug, instrument};

use super::{Encoder, context::FieldLookup};
use crate::{Error, Result};

impl SerializeSeq for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeTupleVariant for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeMap for &mut Encoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, key), fields(key = type_name::<T>()))]
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(key).into()))
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeStruct for &mut Encoder {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            debug!(field = self.field_name(), meta = ?fm, is_valid = self.is_valid());

            _ = self.meta.field.replace(fm);
            self.meta.parse.push_front(fm.fields.into());
            let outcome = if self.is_valid() {
                value.serialize(&mut **self)
            } else {
                Ok(())
            };
            _ = self.meta.parse.pop_front();
            _ = self.meta.field.take();
            outcome
        } else {
            debug!(field = self.field_name());

            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}

impl SerializeStructVariant for &mut Encoder {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            if self
                .api_version
                .is_some_and(|api_version| fm.version.within(api_version))
            {
                debug!(field_name = self.field_name(), meta = ?fm);

                _ = self.meta.field.replace(fm);
                self.meta.parse.push_front(fm.fields.into());
                let outcome = value.serialize(&mut **self);
                _ = self.meta.parse.pop_front();
                _ = self.meta.field.take();
                outcome
            } else {
                debug!(
                    field_name = self.field_name(),
                    api_version = self.api_version
                );
                Ok(())
            }
        } else {
            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}
