use chrono::{NaiveDateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{ColumnIndex, Decode, FromRow, Row, Type};

use crate::message::{Message, Metadata};
use crate::stream_name::StreamName;

impl<'a, R: Row, T> FromRow<'a, R> for Message<T>
where
    &'a str: ColumnIndex<R>,
    StreamName: Decode<'a, R::Database>,
    StreamName: Type<R::Database>,
    String: Decode<'a, R::Database>,
    String: Type<R::Database>,
    i64: Decode<'a, R::Database>,
    i64: Type<R::Database>,
    Option<Value>: Decode<'a, R::Database>,
    Option<Value>: Type<R::Database>,
    NaiveDateTime: Decode<'a, R::Database>,
    NaiveDateTime: Type<R::Database>,
    T: for<'de> Deserialize<'de>,
{
    fn from_row(row: &'a R) -> sqlx::Result<Self> {
        let id =
            row.try_get::<String, _>("id")?
                .parse()
                .map_err(|err| sqlx::Error::ColumnDecode {
                    index: "id".to_string(),
                    source: Box::new(err),
                })?;
        let data = row.try_get("data").map(|data: Option<Value>| {
            serde_json::from_value(data.unwrap_or_default()).map_err(|err| {
                sqlx::Error::ColumnDecode {
                    index: "data".to_string(),
                    source: Box::new(err),
                }
            })
        })??;
        let metadata =
            row.try_get("metadata")
                .map(|metadata: Option<Value>| match metadata {
                    Some(metadata) => {
                        serde_json::from_value(metadata).map_err(|err| sqlx::Error::ColumnDecode {
                            index: "metadata".to_string(),
                            source: Box::new(err),
                        })
                    }
                    None => Ok(Metadata::default()),
                })??;
        let time = Utc.from_utc_datetime(&row.try_get("time")?);
        Ok(Message {
            id,
            stream_name: row.try_get("stream_name")?,
            msg_type: row.try_get("type")?,
            position: row.try_get("position")?,
            global_position: row.try_get("global_position")?,
            data,
            metadata,
            time,
        })
    }
}
