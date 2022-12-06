use std::{fmt, str};

use heck::ToLowerCamelCase;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Category {
    pub entity_name: String,
    pub types: Vec<String>,
}

impl Category {
    pub const CATEGORY_TYPE_SEPARATOR: char = ':';
    pub const COMPOUNT_TYPE_SEPARATOR: char = '+';

    pub fn new(entity_name: impl Into<String>, types: Vec<String>) -> Result<Self> {
        let entity_name = entity_name.into();
        if entity_name.is_empty() {
            return Err(Error::EmptyStreamName);
        }

        Ok(Category { entity_name, types })
    }

    /// Normalizes a category into camelCase.
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
