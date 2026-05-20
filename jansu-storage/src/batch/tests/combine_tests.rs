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

//! Tests for [`combine`](crate::batch::combine).

use bytes::Bytes;
use jansu_sans_io::{BatchAttribute, record::inflated};

use super::into_batches;
use crate::{Result, batch::combine};

#[test]
fn combine_empty() -> Result<()> {
    assert_eq!(None, combine(vec![])?);
    Ok(())
}

#[test]
fn combine_batches() -> Result<()> {
    let batches = [
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ],
        vec![Bytes::from_static(b"f"), Bytes::from_static(b"g")],
        vec![Bytes::from_static(b"i")],
        vec![Bytes::from_static(b"j")],
        vec![Bytes::from_static(b"k")],
        vec![
            Bytes::from_static(b"p"),
            Bytes::from_static(b"q"),
            Bytes::from_static(b"r"),
            Bytes::from_static(b"s"),
        ],
    ];

    let producer_id = 54345;
    let producer_epoch = 32123;
    let base_offset = 0;
    let attributes: i16 = BatchAttribute::default().into();
    let base_sequence: i32 = 0;

    let combined = inflated::Batch::try_from(
        into_batches(
            attributes,
            producer_id,
            producer_epoch,
            base_offset,
            &batches[..],
        )
        .and_then(combine)?
        .expect("a batch"),
    )?;

    assert_eq!(combined.producer_id, producer_id);
    assert_eq!(combined.producer_epoch, producer_epoch);
    assert_eq!(combined.base_sequence, base_sequence);
    assert_eq!(combined.base_offset, base_offset);
    assert_eq!(combined.attributes, attributes);

    let index = 0;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[0][0].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 1;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[0][1].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 2;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[0][2].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 3;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[1][0].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 4;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[1][1].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 5;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[2][0].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 6;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[3][0].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 7;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[4][0].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 8;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[5][0].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 9;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[5][1].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 10;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[5][2].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    let index = 11;
    assert_eq!(None, combined.records[index].key);
    assert_eq!(Some(batches[5][3].clone()), combined.records[index].value);
    assert_eq!(index, combined.records[index].offset_delta as usize);

    Ok(())
}
