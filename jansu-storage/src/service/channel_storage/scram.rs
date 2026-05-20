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

//! SCRAM credential channel calls for [`RequestChannelService`].

use jansu_sans_io::ScramMechanism;
use rama::{Context, Service};

use crate::service::{Request, RequestChannelService, Response};
use crate::{Error, Result, ScramCredential};
use tracing::instrument;

impl RequestChannelService {
    #[instrument(name = "delete_user_scram_credential", skip_all)]
    pub(super) async fn channel_delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let user = user.to_string();

        self.serve(
            Context::default(),
            Request::DeleteUserScramCredential { user, mechanism },
        )
        .await
        .and_then(|response| {
            if let Response::DeleteUserScramCredential(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "upsert_user_scram_credential", skip_all)]
    pub(super) async fn channel_upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let user = user.to_string();

        self.serve(
            Context::default(),
            Request::UpsertUserScramCredential {
                user,
                mechanism,
                credential,
            },
        )
        .await
        .and_then(|response| {
            if let Response::UpsertUserScramCredential(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }

    #[instrument(name = "user_scram_credential", skip_all)]
    pub(super) async fn channel_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let user = user.to_string();

        self.serve(
            Context::default(),
            Request::UserScramCredential { user, mechanism },
        )
        .await
        .and_then(|response| {
            if let Response::UserScramCredential(inner) = response {
                inner.map_err(Into::into)
            } else {
                Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
            }
        })
        .map_err(Into::into)
    }
}
