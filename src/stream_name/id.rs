use std::{fmt, str};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A stream ID or list of IDs.
///
/// # Examples
///
/// `account1`
///
/// A single stream ID.
///
/// `account1+account2`
///
/// A compound stream ID.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ID(Vec<String>);

impl ID {
    /// Compound ID separator.
    ///
    /// When multiple IDs are present, they are separated by a plus (`+`)
    /// character.
    ///
    /// # Example
    ///
    /// `account1+account2`
    pub const COMPOUND_ID_SEPARATOR: char = '+';

    /// Creates a new ID from a string.
    ///
    /// The provided `id` string is split by `+` characters if present to create
    /// a compound ID.
    ///
    /// Returns an error if the ID is empty.
    pub fn new(id: &str) -> Result<Self> {
        if id.is_empty() {
            return Err(Error::EmptyStreamName);
        }

        let ids: Vec<String> = id
            .split(Self::COMPOUND_ID_SEPARATOR)
            .map(|id| id.to_string())
            .collect();

        Self::new_compound(ids)
    }

    /// Creates a compound ID from a vec of IDs.
    ///
    /// Returns an error if the `ids` vec is empty, or any ID within the vec is
    /// an empty string.
    pub fn new_compound(ids: Vec<String>) -> Result<Self> {
        if ids.is_empty() || ids.iter().any(|id| id.is_empty()) {
            return Err(Error::EmptyStreamName);
        }

        Ok(ID(ids))
    }

    /// Returns a slice of the IDs.
    ///
    /// # Example
    ///
    /// ```
    /// # use message_db::stream_name::ID;
    /// #
    /// # fn main() -> message_db::Result<()> {
    /// let id = ID::new("account1+account2")?;
    /// assert_eq!(id.ids(), &["account1", "account2"]);
    /// # Ok(())
    /// # }
    pub fn ids(&self) -> &[String] {
        &self.0
    }

    /// Returns the cardinal ID.
    ///
    /// This is the first ID. If there is only one ID present, that is the
    /// cardinal ID.
    ///
    /// # Example
    ///
    /// ```
    /// # use message_db::stream_name::ID;
    /// #
    /// # fn main() -> message_db::Result<()> {
    /// let id = ID::new("account1+account2")?;
    /// assert_eq!(id.cardinal_id(), "account1");
    /// # Ok(())
    /// # }
    pub fn cardinal_id(&self) -> &str {
        self.0.first().as_ref().unwrap()
    }
}

impl str::FromStr for ID {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, id) in self.ids().iter().enumerate() {
            if i != 0 {
                write!(f, "{}", Self::COMPOUND_ID_SEPARATOR)?;
            }
            write!(f, "{id}")?;
        }

        Ok(())
    }
}
