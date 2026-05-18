use std::collections::VecDeque;

use jansu_model::{FieldMeta, MessageMeta};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum Kind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum Container {
    Struct {
        name: &'static str,
        len: usize,
    },

    StructVariant {
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    },
}

impl Container {
    pub(super) fn name(&self) -> String {
        match self {
            Self::Struct { name, .. } => (*name).to_string(),
            Self::StructVariant { name, variant, .. } => format!("{name}::{variant}"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct FieldLookup(&'static [(&'static str, &'static FieldMeta)]);

impl From<&'static [(&'static str, &'static FieldMeta)]> for FieldLookup {
    fn from(value: &'static [(&'static str, &'static FieldMeta)]) -> Self {
        Self(value)
    }
}

impl FieldLookup {
    #[must_use]
    pub(super) fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        self.0
            .iter()
            .find(|(found, _)| name == *found)
            .map(|(_, meta)| *meta)
    }
}

pub(super) const PARSE_DEPTH: usize = 6;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Meta {
    pub(super) message: Option<&'static MessageMeta>,
    pub(super) field: Option<&'static FieldMeta>,
    pub(super) parse: VecDeque<FieldLookup>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            message: Default::default(),
            field: Default::default(),
            parse: VecDeque::with_capacity(PARSE_DEPTH),
        }
    }
}
