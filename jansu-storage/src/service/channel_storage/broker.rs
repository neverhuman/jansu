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

//! Broker lifecycle channel calls for [`RequestChannelService`].

use std::time::SystemTime;

use jansu_sans_io::describe_cluster_response::DescribeClusterBroker;
use rama::{Context, Service};
use url::Url;

use crate::service::{Request, RequestChannelService, Response};
use crate::{BrokerRegistrationRequest, Error, Result};
use tracing::instrument;

impl RequestChannelService {
    #[instrument(name = "register_broker", skip_all)]
    pub(super) async fn channel_register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        self.serve(
            Context::default(),
            Request::RegisterBroker(broker_registration),
        )
        .await
        .and_then(|response| {
            if let Response::RegisterBroker(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "brokers", skip_all)]
    pub(super) async fn channel_brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.serve(Context::default(), Request::Brokers)
            .await
            .and_then(|response| {
                if let Response::Brokers(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "maintain", skip_all)]
    pub(super) async fn channel_maintain(&self, now: SystemTime) -> Result<()> {
        self.serve(Context::default(), Request::Maintain(now))
            .await
            .and_then(|response| {
                if let Response::Maintain(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "cluster_id", skip_all)]
    pub(super) async fn channel_cluster_id(&self) -> Result<String> {
        self.serve(Context::default(), Request::ClusterId)
            .await
            .and_then(|response| {
                if let Response::ClusterId(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "node", skip_all)]
    pub(super) async fn channel_node(&self) -> Result<i32> {
        self.serve(Context::default(), Request::Node)
            .await
            .and_then(|response| {
                if let Response::Node(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "advertised_listener", skip_all)]
    pub(super) async fn channel_advertised_listener(&self) -> Result<Url> {
        self.serve(Context::default(), Request::AdvertisedListener)
            .await
            .and_then(|response| {
                if let Response::AdvertisedListener(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(name = "ping", skip_all)]
    pub(super) async fn channel_ping(&self) -> Result<()> {
        self.serve(Context::default(), Request::Ping)
            .await
            .and_then(|response| {
                if let Response::Ping(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }
}
