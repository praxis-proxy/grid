//! Validated served-model names and sets.

use std::collections::BTreeSet;

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Maximum length of one model name, in bytes.
///
/// Hugging Face ids (`org/name`) stay well below this; vLLM also accepts
/// local paths as served names. Longer names are treated as garbage.
pub(crate) const MAX_MODEL_NAME_BYTES: usize = 256;

/// Maximum number of models one provider may report.
///
/// A vLLM or `KServe` endpoint serves one base model plus its `LoRA` adapters,
/// usually a handful to dozens. At [`MAX_MODEL_NAME_BYTES`] per name, the
/// worst case is 64 KiB of status, far below the etcd object limit.
pub(crate) const MAX_SERVED_MODELS: usize = 256;

// ---------------------------------------------------------------------------
// ServedModelsError
// ---------------------------------------------------------------------------

/// Why a list of names is not a valid served-model set.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ServedModelsError {
    /// A name is empty or whitespace only.
    #[error("blank model name")]
    Blank,

    /// A name exceeds [`MAX_MODEL_NAME_BYTES`].
    #[error("model name exceeds {MAX_MODEL_NAME_BYTES} bytes")]
    TooLong,

    /// A name contains a control character.
    #[error("model name {0:?} contains a control character")]
    ControlChar(String),

    /// The same name appears more than once.
    ///
    /// Treated as a malformed response rather than silently deduplicated.
    #[error("duplicate model name {0:?}")]
    Duplicate(String),

    /// More than [`MAX_SERVED_MODELS`] names.
    #[error("more than {MAX_SERVED_MODELS} models")]
    TooMany,
}

// ---------------------------------------------------------------------------
// ModelName
// ---------------------------------------------------------------------------

/// A served-model name that passed validation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ModelName(String);

impl TryFrom<String> for ModelName {
    type Error = ServedModelsError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.trim().is_empty() {
            return Err(ServedModelsError::Blank);
        }

        if name.len() > MAX_MODEL_NAME_BYTES {
            return Err(ServedModelsError::TooLong);
        }

        if name.chars().any(char::is_control) {
            return Err(ServedModelsError::ControlChar(name));
        }

        Ok(Self(name))
    }
}

// ---------------------------------------------------------------------------
// ServedModels
// ---------------------------------------------------------------------------

/// The set of models a provider serves: bounded, unique, sorted.
///
/// ```ignore
/// let models = ServedModels::try_from_names(["b".to_owned(), "a".to_owned()])?;
/// assert_eq!(models.into_names(), ["a", "b"]);
/// ```
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct ServedModels(BTreeSet<ModelName>);

impl ServedModels {
    /// Validate `names` as a whole; one bad name rejects the whole list.
    ///
    /// # Errors
    ///
    /// Returns [`ServedModelsError`] for an invalid or duplicate name, or when
    /// the list exceeds [`MAX_SERVED_MODELS`].
    pub(crate) fn try_from_names<I>(names: I) -> Result<Self, ServedModelsError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut set = BTreeSet::new();

        for name in names {
            if set.len() == MAX_SERVED_MODELS {
                return Err(ServedModelsError::TooMany);
            }

            let name = ModelName::try_from(name)?;
            if let Some(dup) = set.replace(name) {
                return Err(ServedModelsError::Duplicate(dup.0));
            }
        }

        Ok(Self(set))
    }

    /// Sorted model names.
    pub(crate) fn into_names(self) -> Vec<String> {
        self.0.into_iter().map(|name| name.0).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_sorted() {
        let models = ServedModels::try_from_names(names(&["b", "a", "c"]));

        assert_eq!(
            models.map(ServedModels::into_names),
            Ok(names(&["a", "b", "c"])),
            "names should be sorted"
        );
    }

    #[test]
    fn empty_list_is_valid() {
        let models = ServedModels::try_from_names(Vec::new());

        assert_eq!(models, Ok(ServedModels::default()), "empty list should be valid");
    }

    #[test]
    fn duplicate_is_rejected() {
        let models = ServedModels::try_from_names(names(&["a", "b", "a"]));

        assert_eq!(
            models,
            Err(ServedModelsError::Duplicate("a".to_owned())),
            "duplicate should be rejected"
        );
    }

    #[test]
    fn blank_is_rejected() {
        let models = ServedModels::try_from_names(names(&["a", "  "]));

        assert_eq!(models, Err(ServedModelsError::Blank), "blank should be rejected");
    }

    #[test]
    fn control_char_is_rejected() {
        let models = ServedModels::try_from_names(names(&["a\nb"]));

        assert_eq!(
            models,
            Err(ServedModelsError::ControlChar("a\nb".to_owned())),
            "control char should be rejected"
        );
    }

    #[test]
    fn name_length_is_bounded() {
        let at_limit = "x".repeat(MAX_MODEL_NAME_BYTES);
        let over_limit = "x".repeat(MAX_MODEL_NAME_BYTES + 1);

        assert!(
            ServedModels::try_from_names([at_limit]).is_ok(),
            "name at limit should be accepted"
        );
        assert_eq!(
            ServedModels::try_from_names([over_limit]),
            Err(ServedModelsError::TooLong),
            "name over limit should be rejected"
        );
    }

    #[test]
    fn model_count_is_bounded() {
        let at_limit: Vec<String> = (0..MAX_SERVED_MODELS).map(|i| format!("m{i}")).collect();
        let over_limit: Vec<String> = (0..=MAX_SERVED_MODELS).map(|i| format!("m{i}")).collect();

        assert!(
            ServedModels::try_from_names(at_limit).is_ok(),
            "count at limit should be accepted"
        );
        assert_eq!(
            ServedModels::try_from_names(over_limit),
            Err(ServedModelsError::TooMany),
            "count over limit should be rejected"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn names(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|&name| name.to_owned()).collect()
    }
}
