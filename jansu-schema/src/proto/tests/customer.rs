// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Protobuf customer schema tests

use super::*;

#[test]
fn timestamp_well_known_type() -> Result<()> {
    let _guard = init_tracing()?;
    let proto = Bytes::from_static(
        br#"
        syntax = "proto3";

        import "google/protobuf/timestamp.proto";

        message Value {
            google.protobuf.Timestamp timestamp = 1;
        }
        "#,
    );

    let _file_descriptor = make_fd(proto).inspect(|fds| debug!(?fds))?;

    Ok(())
}

#[tokio::test]
async fn customer_002_user_id() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../tests/customer-002.proto"
    )))?;

    let message_generator = schema.message_generator().unwrap();

    let user_id = schema
        .message_by_package_relative_name(MessageKind::Value)
        .and_then(|message_descriptor| message_descriptor.field_by_name("user_id"))
        .unwrap();

    let configuration = FieldGeneratorConfiguration::with_field_generator(
        user_id.proto(),
        &message_generator.generator_descriptor,
    );

    assert!(configuration.skip());

    Ok(())
}

#[tokio::test]
async fn customer_002_email_address() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../tests/customer-002.proto"
    )))?;

    let message_generator = schema.message_generator().unwrap();

    let user_id = schema
        .message_by_package_relative_name(MessageKind::Value)
        .and_then(|message_descriptor| message_descriptor.field_by_name("email_address"))
        .unwrap();

    let configuration = FieldGeneratorConfiguration::with_field_generator(
        user_id.proto(),
        &message_generator.generator_descriptor,
    );

    assert!(!configuration.skip());
    assert_eq!(Some("\"lorem\""), configuration.script());

    Ok(())
}

#[tokio::test]
async fn customer_002_industry() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../tests/customer-002.proto"
    )))?;

    let message_generator = schema.message_generator().unwrap();

    let user_id = schema
        .message_by_package_relative_name(MessageKind::Value)
        .and_then(|message_descriptor| message_descriptor.field_by_name("industry"))
        .unwrap();

    let configuration = FieldGeneratorConfiguration::with_field_generator(
        user_id.proto(),
        &message_generator.generator_descriptor,
    );

    assert!(!configuration.skip());
    assert_eq!(Some(3), configuration.repeated_len());
    assert_eq!(Some("\"elit\""), configuration.repeated_script());

    Ok(())
}

#[tokio::test]
async fn customer_003_industry() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = Schema::try_from(Bytes::from_static(include_bytes!(
        "../../../tests/customer-003.proto"
    )))?;

    let message_generator = schema.message_generator().unwrap();

    let user_id = schema
        .message_by_package_relative_name(MessageKind::Value)
        .and_then(|message_descriptor| message_descriptor.field_by_name("industry"))
        .unwrap();

    let configuration = FieldGeneratorConfiguration::with_field_generator(
        user_id.proto(),
        &message_generator.generator_descriptor,
    );

    assert!(!configuration.skip());
    assert_eq!(Some(1..=3), configuration.repeated_range());
    assert_eq!(Some("\"elit\""), configuration.repeated_script());

    Ok(())
}
