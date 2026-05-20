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

//! Protobuf encode, decode and file descriptor helpers

use std::{io::Write, sync::LazyLock};

use bytes::{BufMut, Bytes, BytesMut};
use jansu_sans_io::ErrorCode;
use protobuf::{
    CodedInputStream, MessageDyn, descriptor,
    reflect::{FileDescriptor, MessageDescriptor},
    well_known_types,
};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

use crate::{Error, Result};

pub(super) fn message_to_bytes(message: Box<dyn MessageDyn>) -> Result<Bytes> {
    let mut w = BytesMut::new().writer();
    message
        .write_to_writer_dyn(&mut w)
        .and(Ok(Bytes::from(w.into_inner())))
        .map_err(Into::into)
}

pub(super) fn decode(
    message_descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
) -> Result<Option<Box<dyn MessageDyn>>> {
    debug!(?message_descriptor, ?encoded);

    message_descriptor.map_or(Ok(None), |message_descriptor| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            let mut message = message_descriptor.new_instance();

            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                .and(Ok(Some(message)))
                .inspect(|message| debug!(?message))
        })
    })
}

pub(super) fn validate(
    message_descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
) -> Result<()> {
    decode(message_descriptor, encoded).and(Ok(()))
}

pub(super) static WELL_KNOWN_TYPES: LazyLock<Vec<FileDescriptor>> = LazyLock::new(|| {
    vec![
        descriptor::file_descriptor().to_owned(),
        well_known_types::duration::file_descriptor().to_owned(),
        well_known_types::empty::file_descriptor().to_owned(),
        well_known_types::source_context::file_descriptor().to_owned(),
        well_known_types::timestamp::file_descriptor().to_owned(),
        well_known_types::wrappers::file_descriptor().to_owned(),
    ]
});

pub(super) static META_FILE_DESCRIPTOR: LazyLock<Option<Vec<FileDescriptor>>> =
    LazyLock::new(|| make_fd(Bytes::from_static(include_bytes!("../meta.proto"))).ok());

pub(super) fn make_fd(proto: Bytes) -> Result<Vec<FileDescriptor>> {
    tempdir().map_err(Into::into).and_then(|temp_dir| {
        NamedTempFile::new_in(&temp_dir)
            .inspect(|temp_dir| debug!(?temp_dir))
            .map_err(Into::into)
            .and_then(|mut temp_file| {
                temp_file.write_all(&proto).map_err(Into::into).and(
                    protobuf_parse::Parser::new()
                        .pure()
                        .input(&temp_file)
                        .include(&temp_dir)
                        .parse_and_typecheck()
                        .inspect_err(|err| debug!(?err))
                        .map_err(Into::into)
                        .and_then(|parsed| {
                            parsed
                                .file_descriptors
                                .into_iter()
                                .map(|file_descriptor_proto| {
                                    FileDescriptor::new_dynamic(
                                        file_descriptor_proto,
                                        &WELL_KNOWN_TYPES[..],
                                    )
                                    .inspect_err(|err| debug!(?err))
                                    .map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        }),
                )
            })
    })
}
