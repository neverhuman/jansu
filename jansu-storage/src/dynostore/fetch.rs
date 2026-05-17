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

//! Fetch (read) path for DynoStore.

use super::*;

impl DynoStore {
    pub(super) async fn fetch_inner(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<deflated::Batch>> {
        let high_watermark = self
            .offset_stage_inner(topition)
            .await
            .map(|offset_stage| {
                if isolation_level == IsolationLevel::ReadCommitted {
                    offset_stage.last_stable
                } else {
                    offset_stage.high_watermark
                }
            })?;

        debug!(high_watermark);

        let mut offsets = BTreeSet::new();

        if offset < high_watermark {
            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/",
                self.cluster, topition.topic, topition.partition
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error, ?topition, ?offset, ?min_bytes, ?max_bytes))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                let Some(offset) = meta.location.parts().next_back() else {
                    continue;
                };

                let offset = i64::from_str(&offset.as_ref()[0..20])?;
                debug!(offset);

                if offset < high_watermark {
                    _ = offsets.insert(offset);
                }
            }
        }

        let mut batches = vec![];

        let mut bytes = max_bytes as u64;

        for offset in offsets.split_off(&offset) {
            debug!(?offset);

            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
                self.cluster, topition.topic, topition.partition, offset,
            ));

            let get_result = self
                .object_store
                .get(&location)
                .await
                .inspect_err(|error| error!(?error, ?topition, ?offset, ?min_bytes, ?max_bytes))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?;

            let size = get_result.meta.size;

            let mut batch = get_result
                .bytes()
                .await
                .inspect_err(|error| error!(?error, %location))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                .and_then(|encoded| self.decode(encoded))?;
            batch.base_offset = offset;
            batches.push(batch);

            if size > bytes {
                break;
            } else {
                bytes = bytes.saturating_sub(size);
            }
        }

        Ok(batches)
    }

    pub(super) async fn offset_stage_inner(&self, topition: &Topition) -> Result<OffsetStage> {
        let stable = self
            .meta
            .with(&self.object_store, |meta| {
                Ok(meta
                    .transactions
                    .values()
                    .flat_map(|txn| {
                        debug!(?txn);

                        txn.epochs
                            .values()
                            .filter(|detail| {
                                detail.state.is_some_and(|state| {
                                    state != TxnState::Committed && state != TxnState::Aborted
                                })
                            })
                            .map(BTreeMap::<Topition, Offset>::from)
                            .collect::<Vec<_>>()
                    })
                    .fold(BTreeMap::new(), |mut acc, e| {
                        debug!(?acc, ?e);

                        for (topition, offset_start) in e.iter() {
                            _ = acc
                                .entry(topition.to_owned())
                                .and_modify(|existing_offset_start| {
                                    if *existing_offset_start > *offset_start {
                                        *existing_offset_start = *offset_start
                                    }
                                })
                                .or_insert(*offset_start);
                        }

                        acc
                    }))
            })
            .await?;

        debug!(?stable);

        let watermark = self.watermarks.lock().map(|mut locked| {
            locked
                .entry(topition.to_owned())
                .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                .to_owned()
        })?;

        watermark
            .with(&self.object_store, |watermark| {
                debug!(?watermark);
                let high_watermark = watermark.high.map_or(0, |h| h);
                let log_start = watermark.low.map_or(0, |l| l);
                let last_stable = stable.get(topition).map_or(high_watermark, |v| *v);

                Ok(OffsetStage {
                    last_stable,
                    high_watermark,
                    log_start,
                })
            })
            .await
    }
}
