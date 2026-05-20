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

//! JSON schema evaluation tests

use super::*;

#[test]
fn integer_type_can_be_float_dot_zero() -> Result<()> {
    let schema = json!({"type": "integer"});
    let validator = jsonschema::validator_for(&schema)?;

    assert!(validator.is_valid(&json!(42)));
    assert!(validator.is_valid(&json!(-1)));
    assert!(validator.is_valid(&json!(1.0)));

    Ok(())
}

#[test]
fn array_with_items_type_basic_output() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "value": {
                "type": "array",
                "items": {
                    "type": "number"
                }
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([1, 2, 3, 4, 5]))
            .flag()
            .valid
    );

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([-1, 2.3, 3, 4.0, 5]))
            .flag()
            .valid
    );

    assert!(
        !schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([3, "different", { "types": "of values" }]))
            .flag()
            .valid
    );

    assert!(
        !schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!({"Not": "an array"}))
            .flag()
            .valid,
    );

    Ok(())
}

#[test]
fn array_basic_output() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "value": {
                "type": "array",
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([1, 2, 3, 4, 5]))
            .flag()
            .valid
    );

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!([3, "different", { "types": "of values" }]))
            .flag()
            .valid
    );

    assert!(
        !schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!({"Not": "an array"}))
            .flag()
            .valid,
    );

    Ok(())
}

#[test]
fn schema_basic_output() -> Result<()> {
    let _guard = init_tracing()?;

    let schema = serde_json::to_vec(&json!({
        "type": "object",
        "properties": {
            "key": {
                "type": "number"
            },
            "value": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    }
                }
            }
        }
    }))
    .map_err(Into::into)
    .map(Bytes::from)
    .and_then(Schema::try_from)?;

    debug!(?schema);

    assert!(
        schema
            .key
            .as_ref()
            .unwrap()
            .evaluate(&json!(12321))
            .flag()
            .valid
    );

    assert!(
        schema
            .value
            .as_ref()
            .unwrap()
            .evaluate(&json!({"name": "alice", "email": "alice@example.com"}))
            .flag()
            .valid
    );

    Ok(())
}
