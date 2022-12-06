use std::{fmt, str};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ID(Vec<String>);

impl ID {
    pub const COMPOUND_ID_SEPARATOR: char = '+';

    pub fn new(id: &str) -> Result<Self> {
        if id.is_empty() {
            return Err(Error::EmptyStreamID);
        }

        let ids: Vec<String> = id
            .split(Self::COMPOUND_ID_SEPARATOR)
            .map(|id| id.to_string())
            .collect();

        Self::new_compound(ids)
    }

    pub fn new_compound(ids: Vec<String>) -> Result<Self> {
        if ids.is_empty() || ids.iter().any(|id| id.is_empty()) {
            return Err(Error::EmptyStreamID);
        }

        Ok(ID(ids))
    }

    pub fn ids(&self) -> &[String] {
        &self.0
    }

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
