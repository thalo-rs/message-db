use std::{fmt, str};

use heck::ToLowerCamelCase;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A stream category containing an entity name, and optionally category types.
///
/// # Examples
///
/// `account`
///
/// Account entity with no category types.
///
/// `account:command`
///
/// Commands for the account entity.
///
/// `account:command+snapshot`
///
/// Commands and snapshots for the account entity.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Category {
    /// Category entity name.
    pub entity_name: String,
    /// Category types.
    pub types: Vec<String>,
}

impl Category {
    /// Category type separator.
    ///
    /// When a stream category contains a category type, it is separated by a
    /// colon (`:`) character.
    ///
    /// # Example
    ///
    /// `category:command`
    pub const CATEGORY_TYPE_SEPARATOR: char = ':';
    /// Compound type separator.
    ///
    /// When one or more category types are present, they are separated by a
    /// plus (`+`) character.
    ///
    /// # Example
    ///
    /// `category:command+snapshot`
    pub const COMPOUNT_TYPE_SEPARATOR: char = '+';

    /// Creates a new [`Category`] containing an entity name, and optionally a
    /// list of category types.
    ///
    /// For a category with no types, an empty [`Vec`] should be used.
    ///
    /// An error is returned if the entity name is empty.
    ///
    /// # Example
    ///
    /// ```
    /// # use message_db::stream_name::Category;
    /// #
    /// # fn main() -> message_db::Result<()> {
    /// let category = Category::new("account", vec!["command".to_string()])?;
    /// assert_eq!(category.to_string(), "account:command");
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(entity_name: impl Into<String>, types: Vec<String>) -> Result<Self> {
        let entity_name = entity_name.into();
        if entity_name.is_empty() {
            return Err(Error::EmptyStreamName);
        }

        Ok(Category { entity_name, types })
    }

    /// Normalizes a category into camelCase.
    ///
    /// # Example
    ///
    /// ```
    /// # use message_db::stream_name::Category;
    /// #
    /// let category = Category::normalize("Bank_Account");
    /// assert_eq!(category, "bankAccount");
    /// ```
    pub fn normalize(category: &str) -> String {
        category.to_lower_camel_case()
    }
}

impl str::FromStr for Category {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(Self::CATEGORY_TYPE_SEPARATOR) {
            Some((entity_name, types)) => Ok(Category {
                entity_name: entity_name.to_string(),
                types: types
                    .split(Self::COMPOUNT_TYPE_SEPARATOR)
                    .map(|s| s.to_string())
                    .collect(),
            }),
            None => Ok(Category {
                entity_name: s.to_string(),
                types: vec![],
            }),
        }
    }
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.entity_name)?;
        if !self.types.is_empty() {
            write!(f, "{}", Self::CATEGORY_TYPE_SEPARATOR)?;
            for (i, t) in self.types.iter().enumerate() {
                if i != 0 {
                    write!(f, "{}", Self::COMPOUNT_TYPE_SEPARATOR)?;
                }
                write!(f, "{t}")?;
            }
        }

        Ok(())
    }
}
