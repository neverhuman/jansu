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

//! Kafka-standard topic configuration defaults, types, and synonym mappings.
//!
//! This module centralizes the default topic config values and synonym
//! relationships so that all storage backends produce consistent
//! `DescribeConfigs` responses matching Kafka 4.2 behavior.

use std::collections::BTreeMap;

use jansu_sans_io::{
    ConfigSource, ConfigType,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsSynonym},
};

/// A single Kafka topic config definition.
struct TopicConfigDef {
    name: &'static str,
    default_value: &'static str,
    config_type: ConfigType,
    synonym: Option<&'static str>,
}

/// Ordered list of Kafka-standard topic config defaults.
///
/// Values and types match Kafka 4.2 `DescribeConfigs` for a newly-created
/// topic with no explicit overrides.
const TOPIC_CONFIG_DEFS: &[TopicConfigDef] = &[
    TopicConfigDef {
        name: "cleanup.policy",
        default_value: "delete",
        config_type: ConfigType::List,
        synonym: Some("log.cleanup.policy"),
    },
    TopicConfigDef {
        name: "compression.type",
        default_value: "producer",
        config_type: ConfigType::String,
        synonym: Some("compression.type"),
    },
    TopicConfigDef {
        name: "delete.retention.ms",
        default_value: "86400000",
        config_type: ConfigType::Long,
        synonym: Some("log.cleaner.delete.retention.ms"),
    },
    TopicConfigDef {
        name: "file.delete.delay.ms",
        default_value: "60000",
        config_type: ConfigType::Long,
        synonym: Some("log.segment.delete.delay.ms"),
    },
    TopicConfigDef {
        name: "flush.messages",
        default_value: "9223372036854775807",
        config_type: ConfigType::Long,
        synonym: Some("log.flush.interval.messages"),
    },
    TopicConfigDef {
        name: "flush.ms",
        default_value: "9223372036854775807",
        config_type: ConfigType::Long,
        synonym: Some("log.flush.interval.ms"),
    },
    TopicConfigDef {
        name: "index.interval.bytes",
        default_value: "4096",
        config_type: ConfigType::Int,
        synonym: Some("log.index.interval.bytes"),
    },
    TopicConfigDef {
        name: "max.compaction.lag.ms",
        default_value: "9223372036854775807",
        config_type: ConfigType::Long,
        synonym: Some("log.cleaner.max.compaction.lag.ms"),
    },
    TopicConfigDef {
        name: "max.message.bytes",
        default_value: "1048588",
        config_type: ConfigType::Int,
        synonym: Some("message.max.bytes"),
    },
    TopicConfigDef {
        name: "message.downconversion.enable",
        default_value: "true",
        config_type: ConfigType::Boolean,
        synonym: Some("log.message.downconversion.enable"),
    },
    TopicConfigDef {
        name: "message.timestamp.difference.max.ms",
        default_value: "9223372036854775807",
        config_type: ConfigType::Long,
        synonym: Some("log.message.timestamp.difference.max.ms"),
    },
    TopicConfigDef {
        name: "message.timestamp.type",
        default_value: "CreateTime",
        config_type: ConfigType::String,
        synonym: Some("log.message.timestamp.type"),
    },
    TopicConfigDef {
        name: "min.cleanable.dirty.ratio",
        default_value: "0.5",
        config_type: ConfigType::Double,
        synonym: Some("log.cleaner.min.cleanable.ratio"),
    },
    TopicConfigDef {
        name: "min.compaction.lag.ms",
        default_value: "0",
        config_type: ConfigType::Long,
        synonym: Some("log.cleaner.min.compaction.lag.ms"),
    },
    TopicConfigDef {
        name: "min.insync.replicas",
        default_value: "1",
        config_type: ConfigType::Int,
        synonym: Some("min.insync.replicas"),
    },
    TopicConfigDef {
        name: "retention.bytes",
        default_value: "-1",
        config_type: ConfigType::Long,
        synonym: Some("log.retention.bytes"),
    },
    TopicConfigDef {
        name: "retention.ms",
        default_value: "604800000",
        config_type: ConfigType::Long,
        synonym: Some("log.retention.ms"),
    },
    TopicConfigDef {
        name: "segment.bytes",
        default_value: "1073741824",
        config_type: ConfigType::Int,
        synonym: Some("log.segment.bytes"),
    },
    TopicConfigDef {
        name: "segment.index.bytes",
        default_value: "10485760",
        config_type: ConfigType::Int,
        synonym: Some("log.index.size.max.bytes"),
    },
    TopicConfigDef {
        name: "segment.jitter.ms",
        default_value: "0",
        config_type: ConfigType::Long,
        synonym: Some("log.roll.jitter.ms"),
    },
    TopicConfigDef {
        name: "segment.ms",
        default_value: "604800000",
        config_type: ConfigType::Long,
        synonym: Some("log.roll.ms"),
    },
    TopicConfigDef {
        name: "unclean.leader.election.enable",
        default_value: "false",
        config_type: ConfigType::Boolean,
        synonym: Some("unclean.leader.election.enable"),
    },
];

/// Build a `BTreeMap` of all default topic configs for a fresh topic.
///
/// Each entry has `is_default = Some(true)` and
/// `config_source = DefaultConfig`. Callers should overlay any
/// explicitly-stored configs on top (changing `is_default` to `None`).
pub(crate) fn build_default_configs() -> BTreeMap<String, DescribeConfigsResourceResult> {
    TOPIC_CONFIG_DEFS
        .iter()
        .map(|def| {
            (
                def.name.to_string(),
                DescribeConfigsResourceResult::default()
                    .name(def.name.into())
                    .value(Some(def.default_value.into()))
                    .read_only(false)
                    .is_default(Some(true))
                    .config_source(Some(ConfigSource::DefaultConfig.into()))
                    .is_sensitive(false)
                    .synonyms(Some([].into()))
                    .config_type(Some(def.config_type.into()))
                    .documentation(Some("".into())),
            )
        })
        .collect()
}

/// Return synonym entries for a given topic config name, if any.
///
/// The returned synonyms carry the provided `value` and source
/// `DefaultConfig`, matching Kafka's synonym response shape.
pub(crate) fn synonyms_for(name: &str, value: Option<&str>) -> Option<Vec<DescribeConfigsSynonym>> {
    TOPIC_CONFIG_DEFS
        .iter()
        .find(|def| def.name == name)
        .and_then(|def| def.synonym)
        .map(|synonym_name| {
            vec![
                DescribeConfigsSynonym::default()
                    .name(synonym_name.into())
                    .value(value.map(Into::into))
                    .source(ConfigSource::DefaultConfig.into()),
            ]
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_contain_cleanup_policy_and_retention_ms() {
        let defaults = build_default_configs();
        assert!(defaults.contains_key("cleanup.policy"));
        assert!(defaults.contains_key("retention.ms"));
        assert_eq!(defaults["cleanup.policy"].value.as_deref(), Some("delete"));
        assert_eq!(defaults["retention.ms"].value.as_deref(), Some("604800000"));
    }

    #[test]
    fn synonyms_for_known_configs() {
        let syns = synonyms_for("cleanup.policy", Some("delete")).unwrap();
        assert_eq!(1, syns.len());
        assert_eq!("log.cleanup.policy", syns[0].name);

        let syns = synonyms_for("retention.ms", Some("604800000")).unwrap();
        assert_eq!(1, syns.len());
        assert_eq!("log.retention.ms", syns[0].name);

        let syns = synonyms_for("segment.bytes", Some("1073741824")).unwrap();
        assert_eq!(1, syns.len());
        assert_eq!("log.segment.bytes", syns[0].name);
    }

    #[test]
    fn defaults_are_ordered_by_name() {
        let defaults = build_default_configs();
        let keys: Vec<_> = defaults.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
