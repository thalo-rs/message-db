pub mod category;
pub mod id;

use std::{fmt, str};

use serde::{Deserialize, Serialize};

use self::category::Category;
use self::id::ID;
use crate::Error;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamName {
    pub category: Category,
    pub id: Option<ID>,
}

impl StreamName {
    pub const ID_SEPARATOR: char = '-';

    pub fn is_category(stream_name: &str) -> bool {
        stream_name.contains(Self::ID_SEPARATOR)
    }

    pub fn category(stream_name: &str) -> &str {
        stream_name
            .split_once(Self::ID_SEPARATOR)
            .map(|(category, _)| category)
            .unwrap_or(stream_name)
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.category)?;
        if let Some(id) = &self.id {
            write!(f, "{}", Self::ID_SEPARATOR)?;
            write!(f, "{id}")?;
        }

        Ok(())
    }
}

impl str::FromStr for StreamName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // category:type_a+type_b-id_a+id_b
        match s.split_once(StreamName::ID_SEPARATOR) {
            Some((category_part, id_part)) => {
                let category = category_part.parse()?;
                let id = id_part.parse()?;

                Ok(StreamName {
                    category,
                    id: Some(id),
                })
            }
            None => {
                let category = s.parse()?;

                Ok(StreamName { category, id: None })
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::StreamName;

    #[test]
    fn it_works() {
        let category = "category".parse().unwrap();
        let id = "id".parse().unwrap();
        let steam_name = StreamName {
            category,
            id: Some(id),
        };
        assert_eq!(steam_name.to_string(), "category-id");

        let category = "category:type_a".parse().unwrap();
        let id = "id".parse().unwrap();
        let steam_name = StreamName {
            category,
            id: Some(id),
        };
        assert_eq!(steam_name.to_string(), "category:type_a-id");

        let category = "category:type_a+type_b".parse().unwrap();
        let id = "id".parse().unwrap();
        let steam_name = StreamName {
            category,
            id: Some(id),
        };
        assert_eq!(steam_name.to_string(), "category:type_a+type_b-id");

        let category = "category".parse().unwrap();
        let id = "id_a+id_b".parse().unwrap();
        let steam_name = StreamName {
            category,
            id: Some(id),
        };
        assert_eq!(steam_name.to_string(), "category-id_a+id_b");

        let category = "category:type_a+type_b".parse().unwrap();
        let id = "id_a+id_b".parse().unwrap();
        let steam_name = StreamName {
            category,
            id: Some(id),
        };
        assert_eq!(steam_name.to_string(), "category:type_a+type_b-id_a+id_b");
    }
}
