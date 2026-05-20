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

use serde_json::json;

use super::*;

#[test]
fn as_a_str() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
            "hello": "world"
        }
        "#,
    )?;

    let wv = Wv::from(&v);
    let s: &str = wv.as_a("hello")?;
    assert_eq!("world", s);

    Ok(())
}

#[test]
fn as_a_string() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
            "hello": "world"
        }
        "#,
    )?;

    let wv = Wv::from(&v);
    let s: String = wv.as_a("hello")?;
    assert_eq!("world", s);

    Ok(())
}

#[test]
fn as_a_kind() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
            "type": "request"
        }
        "#,
    )?;

    let wv = Wv::from(&v);
    let s: MessageKind = wv.as_a("type")?;
    assert_eq!(MessageKind::Request, s);

    Ok(())
}

#[test]
fn as_option() -> Result<()> {
    fn as_option<'a, 'b, T>(
        instance: &'a Wv<'b>,
        operation: impl Fn(&'a Wv<'b>, &'static str) -> Result<Option<T>>,
        tests: &[(&'static str, Option<T>)],
    ) -> Result<()>
    where
        T: 'a + PartialEq + std::fmt::Debug,
    {
        for (name, expected) in tests {
            assert_eq!(*expected, operation(instance, name)?, "name: {name}");
        }
        Ok(())
    }

    let v = serde_json::from_str::<Value>(
        r#"
        {
            "u64": 18446744073709551615,
            "u32": 4294967295,
            "u16": 65535,
            "u8": 255,

            "i64_max": 9223372036854775807,
            "i64_min": -9223372036854775808,
            "i32_max": 2147483647,
            "i32_min": -2147483648,
            "i16_max": 32767,
            "i16_min": -32768,
            "i8_max": 127,
            "i8_min": -128
        }
        "#,
    )?;

    let wv = Wv::from(&v);

    as_option(
        &wv,
        <Wv<'_> as AsOption<u64>>::as_option,
        &[
            ("u64", Some(u64::MAX)),
            ("u32", Some(u32::MAX.into())),
            ("u16", Some(u16::MAX.into())),
            ("u8", Some(u8::MAX.into())),
            ("i64_max", Some(i64::MAX.try_into()?)),
            ("i64_min", None),
            ("i32_max", Some(i32::MAX.try_into()?)),
            ("i32_min", None),
            ("i16_max", Some(i16::MAX.try_into()?)),
            ("i16_min", None),
            ("i8_max", Some(i8::MAX.try_into()?)),
            ("i8_min", None),
        ],
    )?;

    assert!(matches!(
        <Wv<'_> as AsOption<u32>>::as_option(&wv, "u64"),
        Err(Error::TryFromInt(_)),
    ));
    assert!(matches!(
        <Wv<'_> as AsOption<u32>>::as_option(&wv, "i64_max"),
        Err(Error::TryFromInt(_)),
    ));

    as_option(
        &wv,
        <Wv<'_> as AsOption<u32>>::as_option,
        &[
            // ("u64", None),
            ("u32", Some(u32::MAX)),
            ("u16", Some(u16::MAX.into())),
            ("u8", Some(u8::MAX.into())),
            // ("i64_max", Some(i64::MAX.try_into()?)),
            ("i64_min", None),
            ("i32_max", Some(i32::MAX.try_into()?)),
            ("i32_min", None),
            ("i16_max", Some(i16::MAX.try_into()?)),
            ("i16_min", None),
            ("i8_max", Some(i8::MAX.try_into()?)),
            ("i8_min", None),
        ],
    )?;

    Ok(())
}

#[test]
fn as_a_u64() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
            "value": 32123
        }
        "#,
    )?;

    let wv = Wv::from(&v);
    let s: u64 = wv.as_a("value")?;
    assert_eq!(32123, s);

    Ok(())
}

#[test]
fn as_a_version_range() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
            "flexibleVersions": "2+"
        }
        "#,
    )?;

    let wv = Wv::from(&v);
    let flexible: VersionRange = wv.as_a("flexibleVersions")?;

    assert_eq!(
        VersionRange {
            start: 2,
            end: i16::MAX
        },
        flexible
    );

    Ok(())
}

#[test]
fn as_array_value() -> Result<()> {
    let v = serde_json::from_str::<Value>(
        r#"
        {
            "x": [1,2,3]
        }
        "#,
    )?;

    let wv = Wv::from(&v);
    let array: &[Value] = wv.as_a("x")?;
    assert_eq!([json!(1), json!(2), json!(3)], array);

    Ok(())
}
