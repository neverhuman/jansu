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

mod blocking_storage;

use std::{sync::Arc, time::Duration};

use jansu_sans_io::IsolationLevel;
use rama::{Context, Service};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{Request, RequestStorageService, Response};
use crate::{AbortedTransactionRange, Error, Topition};
use blocking_storage::BlockingFetchStorage;

#[tokio::test]
async fn fetch_request_is_cancelled() {
    let storage = BlockingFetchStorage::default();
    let service = RequestStorageService::new(storage);
    let mut ctx = Context::default();
    let cancellation = CancellationToken::new();
    _ = ctx.insert(cancellation.clone());

    let request = Request::Fetch {
        topition: Topition::new(String::from("topic"), 0),
        offset: 0,
        min_bytes: 1,
        max_bytes: 1024,
        isolation: IsolationLevel::ReadUncommitted,
    };

    let handle = tokio::spawn(async move { service.serve(ctx, request).await });

    tokio::task::yield_now().await;
    cancellation.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("fetch request should finish promptly")
        .expect("join should succeed");

    assert!(matches!(result, Err(Error::Cancelled)));
}

#[tokio::test]
async fn aborted_transaction_ranges_request_is_forwarded() {
    let storage = BlockingFetchStorage {
        gate: Arc::new(Notify::new()),
        aborted_transaction_ranges: vec![
            AbortedTransactionRange {
                producer_id: 11,
                offset_start: 7,
                offset_end: 13,
            },
            AbortedTransactionRange {
                producer_id: 12,
                offset_start: 21,
                offset_end: 29,
            },
        ],
    };
    let service = RequestStorageService::new(storage.clone());

    let response = service
        .serve(
            Context::default(),
            Request::AbortedTransactionRanges(Topition::new("topic", 0)),
        )
        .await
        .expect("request should succeed");

    let forwarded = match response {
        Response::AbortedTransactionRanges(result) => result.expect("storage result"),
        other => panic!("unexpected response: {other:?}"),
    };

    assert_eq!(storage.aborted_transaction_ranges, forwarded);
}
