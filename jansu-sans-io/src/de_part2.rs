struct Batch {
    encoded: Bytes,
}

impl Batch {
    fn new(encoded: Bytes) -> Self {
        Self { encoded }
    }
}

impl<'de> SeqAccess<'de> for Batch {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(
            seed = type_name::<T>(),
            value = type_name::<T::Value>(),
            encoded = ?&self.encoded[..]
        );

        if self.encoded.has_remaining() {
            let base_offset = self.encoded.try_get_i64()?;
            let batch_length = self.encoded.try_get_i32()?;
            debug!(base_offset, batch_length);

            let mut batch = BytesMut::with_capacity(batch_length as usize);
            batch.put_i64(base_offset);
            batch.put_i32(batch_length);

            if (batch_length as usize) > self.encoded.len() {
                return Err(Error::Overflow);
            }

            batch.put(self.encoded.split_to(batch_length as usize));

            let decoder = BatchDecoder {
                encoded: batch.freeze(),
            };

            seed.deserialize(decoder).map(Some)
        } else {
            Ok(None)
        }
    }
}

pub struct BatchDecoder {
    encoded: Bytes,
}

impl BatchDecoder {
    pub fn new(encoded: Bytes) -> Self {
        Self { encoded }
    }
}

impl<'de> Deserializer<'de> for BatchDecoder {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(&self.encoded[..])
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(Vec::from(self.encoded))
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_tuple<V>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{fields:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{variants:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }
}

#[derive(Debug)]
struct Seq<'de, 'a> {
    de: &'a mut Decoder<'de>,
    length: Option<usize>,
}

impl<'de, 'a> Seq<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, length: Option<usize>) -> Self {
        Self { de, length }
    }
}

impl<'de> SeqAccess<'de> for Seq<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(
            "seq, next seed: {}, length: {:?}",
            type_name_of_val(&seed),
            self.length
        );

        match self.length {
            Some(0) => Ok(None),

            Some(length) => {
                _ = self.length.replace(length - 1);
                seed.deserialize(&mut *self.de).map(Some)
            }

            None => seed.deserialize(&mut *self.de).map(Some),
        }
    }
}

#[derive(Debug)]
struct Struct<'de, 'a> {
    de: &'a mut Decoder<'de>,
    name: &'static str,
    fields: &'static [&'static str],
    index: usize,
}

impl<'de, 'a> Struct<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, name: &'static str, fields: &'static [&'static str]) -> Self {
        Self {
            de,
            name,
            fields,
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for Struct<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let field = self.fields[self.index];
        self.de.field = Some(field);
        self.index += 1;

        if let Some(fl) = self.de.meta.parse.front() {
            self.de.meta.field = fl.field(field);
        }

        debug!(
            struct = self.name,
            field,
            is_flexible = self.de.is_flexible(),
            type_name = type_name::<T::Value>(),
            meta_field = self.de.meta.field.is_some(),
            is_records = self.de.is_records(),
        );

        self.de.path.push_front(field);
        let outcome = seed.deserialize(&mut *self.de).map(Some);
        _ = self.de.path.pop_front();
        outcome
    }
}

#[derive(Debug)]
struct Enum<'de, 'a> {
    de: &'a mut Decoder<'de>,
    name: &'static str,
}

impl<'de, 'a> Enum<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, name: &'static str) -> Self {
        Self { de, name }
    }
}

impl<'de> EnumAccess<'de> for Enum<'de, '_> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.de)?;
        Ok((val, self))
    }
}

impl<'de> VariantAccess<'de> for Enum<'de, '_> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Err(Error::Message(String::from("expecting string")))
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(name = self.name, seed = type_name_of_val(&seed));
        seed.deserialize(&mut *self.de)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            name = self.name,
            ?fields,
            visitor = type_name_of_val(&visitor)
        );
        Deserializer::deserialize_struct(self.de, self.name, fields, visitor)
    }
}
