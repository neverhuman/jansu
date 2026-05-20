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

//! AVRO from_json conversion tests

use super::*;
use crate::avro::from_json as avro_from_json;

#[test]
fn from_json() -> Result<()> {
    let _guard = init_tracing()?;

    assert_eq!(
        Value::Null,
        avro_from_json(&AvroSchema::parse(&json!({"type": "null"}))?, &json!(null))?
    );

    assert_eq!(
        Value::Boolean(true),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "boolean"}))?,
            &json!(true)
        )?
    );

    assert_eq!(
        Value::Boolean(false),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "boolean"}))?,
            &json!(false)
        )?
    );

    assert_eq!(
        Value::Int(i32::MIN),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "int"}))?,
            &json!(i32::MIN)
        )?
    );

    assert_eq!(
        Value::Int(i32::MAX),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "int"}))?,
            &json!(i32::MAX)
        )?
    );

    assert_eq!(
        Value::Long(i64::MIN),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "long"}))?,
            &json!(i64::MIN)
        )?
    );

    assert_eq!(
        Value::Long(i64::MAX),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "long"}))?,
            &json!(i64::MAX)
        )?
    );

    assert_eq!(
        Value::Float(f32::MIN),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "float"}))?,
            &json!(f32::MIN)
        )?
    );

    assert_eq!(
        Value::Float(f32::MAX),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "float"}))?,
            &json!(f32::MAX)
        )?
    );

    assert_eq!(
        Value::Double(f64::MIN),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "double"}))?,
            &json!(f64::MIN)
        )?
    );

    assert_eq!(
        Value::Double(f64::MAX),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "double"}))?,
            &json!(f64::MAX)
        )?
    );

    assert_eq!(
        Value::String("hello world!".into()),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "string"}))?,
            &json!("hello world!")
        )?
    );

    assert_eq!(
        Value::Array(vec![
            Value::String("abc".into()),
            Value::String("pqr".into()),
            Value::String("xyz".into()),
        ]),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "array", "items": "string"}))?,
            &json!(["abc", "pqr", "xyz"])
        )?
    );

    assert_eq!(
        Value::Enum(2, "DIAMONDS".into()),
        avro_from_json(
            &AvroSchema::parse(&json!({
                "type": "enum",
                "name": "Suit",
                "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }))?,
            &json!("DIAMONDS")
        )?
    );

    assert_eq!(
        Value::Bytes([97, 98, 99].into()),
        avro_from_json(
            &AvroSchema::parse(&json!({"type": "bytes"}))?,
            &json!("abc")
        )?
    );

    {
        let uuid = Uuid::new_v4();

        assert_eq!(
            Value::Uuid(uuid),
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "string", "logicalType": "uuid"}))?,
                &json!(uuid.to_string())
            )?
        );
    }

    {
        let value = Value::TimestampMillis(119_731_017_000);

        assert_eq!(
            value,
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-millis"}))?,
                &json!("1973-10-17T18:36:57")
            )?
        );

        let value = Value::TimestampMillis(119_731_017_123);

        assert_eq!(
            value,
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-millis"}))?,
                &json!("1973-10-17T18:36:57.123")
            )?
        );

        assert_eq!(
            value,
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-millis"}))?,
                &json!("1973-10-17T18:36:57.123456")
            )?
        );

        assert_eq!(
            value,
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-millis"}))?,
                &json!("1973-10-17T18:36:57.123456789")
            )?
        );
    }

    {
        assert_eq!(
            Value::TimestampMicros(119_731_017_000_000),
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-micros"}))?,
                &json!("1973-10-17T18:36:57")
            )?
        );

        assert_eq!(
            Value::TimestampMicros(119_731_017_123_000),
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-micros"}))?,
                &json!("1973-10-17T18:36:57.123")
            )?
        );

        assert_eq!(
            Value::TimestampMicros(119_731_017_123_456),
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-micros"}))?,
                &json!("1973-10-17T18:36:57.123456")
            )?
        );

        assert_eq!(
            Value::TimestampMicros(119_731_017_123_456),
            avro_from_json(
                &AvroSchema::parse(&json!({"type": "long", "logicalType": "timestamp-micros"}))?,
                &json!("1973-10-17T18:36:57.123456789")
            )?
        );
    }

    {
        let v = avro_from_json(
            &AvroSchema::parse(&json!({
                "type": "map",
                "values": "long"
            }))?,
            &json!({"a": 1, "b": 3, "c": 5}),
        )?;

        assert!(matches!(v, Value::Map(_)));

        let Value::Map(values) = v else {
            panic!("{v:?}")
        };

        assert_eq!(Some(&Value::Long(1)), values.get("a"));
        assert_eq!(Some(&Value::Long(3)), values.get("b"));
        assert_eq!(Some(&Value::Long(5)), values.get("c"));
    }

    {
        let v = avro_from_json(
            &AvroSchema::parse(&json!({
            "type": "array",
            "items": {
                "type": "record",
                "name": "people",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "lucky", "type": "array", "items": "int"}
                ]}
            }))?,
            &json!([
                {"id": 32123, "name": "alice", "lucky": [6]},
                {"id": 45654, "name": "bob", "lucky": [5, 9]}]),
        )?;

        assert!(matches!(v, Value::Array(_)));

        let Value::Array(values) = v else {
            panic!("{v:?}")
        };

        assert_eq!(2, values.len());

        let Some(Value::Record(r0)) = values.first() else {
            panic!("{:?}", values[0])
        };

        assert_eq!(
            Value::Int(32123),
            r0.iter().find(|(name, _)| name == "id").unwrap().1
        );

        assert_eq!(
            Value::String("alice".into()),
            r0.iter().find(|(name, _)| name == "name").unwrap().1
        );

        assert_eq!(
            Value::Array(vec![Value::Int(6)]),
            r0.iter().find(|(name, _)| name == "lucky").unwrap().1
        );

        let Some(Value::Record(r1)) = values.get(1) else {
            panic!("{:?}", values[0])
        };

        assert_eq!(
            Value::Int(45654),
            r1.iter().find(|(name, _)| name == "id").unwrap().1
        );

        assert_eq!(
            Value::String("bob".into()),
            r1.iter().find(|(name, _)| name == "name").unwrap().1
        );

        assert_eq!(
            Value::Array(vec![Value::Int(5), Value::Int(9)]),
            r1.iter().find(|(name, _)| name == "lucky").unwrap().1
        );
    }

    Ok(())
}
