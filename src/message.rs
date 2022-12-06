mod metadata;

use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub use self::metadata::Metadata;
use crate::stream_name::StreamName;
use crate::{Error, Result};

pub type MessageData = Value;
pub type GenericMessage = Message<MessageData>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message<T> {
    pub id: Uuid,
    pub stream_name: StreamName,
    pub msg_type: String,
    pub position: i64,
    pub global_position: i64,
    pub data: T,
    pub metadata: Metadata,
    #[serde(with = "ts_milliseconds")]
    pub time: DateTime<Utc>,
}

impl<T> Message<T> {
    pub fn map_data<U, F>(self, f: F) -> Message<U>
    where
        F: FnOnce(T) -> U,
    {
        let new_data = f(self.data);
        Message {
            id: self.id,
            stream_name: self.stream_name,
            msg_type: self.msg_type,
            position: self.position,
            global_position: self.global_position,
            data: new_data,
            metadata: self.metadata,
            time: self.time,
        }
    }
}

impl GenericMessage {
    pub fn deserialize_data<T>(self) -> Result<Message<T>, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let data = serde_json::from_value(self.data)?;
        Ok(Message {
            id: self.id,
            stream_name: self.stream_name,
            msg_type: self.msg_type,
            position: self.position,
            global_position: self.global_position,
            data,
            metadata: self.metadata,
            time: self.time,
        })
    }
}

pub(crate) trait DeserializeMessage<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Output;

    fn deserialize_messages(self) -> Result<Self::Output>;
}

impl<T> DeserializeMessage<T> for Option<GenericMessage>
where
    T: for<'de> Deserialize<'de>,
{
    type Output = Option<Message<T>>;

    fn deserialize_messages(self) -> Result<Self::Output> {
        self.map(|message| message.deserialize_data())
            .transpose()
            .map_err(Error::DeserializeData)
    }
}

impl<T> DeserializeMessage<T> for Vec<GenericMessage>
where
    T: for<'de> Deserialize<'de>,
{
    type Output = Vec<Message<T>>;

    fn deserialize_messages(self) -> Result<Self::Output> {
        self.into_iter()
            .map(|message| message.deserialize_data())
            .collect::<Result<Vec<_>, _>>()
            .map_err(Error::DeserializeData)
    }
}
