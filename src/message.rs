use std::collections::HashMap;

use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::stream_name::category::Category;
use crate::stream_name::StreamName;
use crate::{Error, Result};

pub type MessageData = Option<Value>;
pub type GenericMessage = Message<MessageData>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "database", derive(sqlx::FromRow))]
pub struct Message<T> {
    pub id: Uuid,
    pub stream_name: StreamName,
    pub msg_type: String,
    pub position: i64,
    pub global_position: i64,
    pub data: T,
    #[cfg_attr(feature = "database", sqlx(try_from = "MessageData"))]
    pub metadata: Metadata,
    #[serde(with = "ts_milliseconds")]
    #[cfg_attr(feature = "database", sqlx(try_from = "DbTimestampTz"))]
    pub time: DateTime<Utc>,
}
#[cfg(feature = "database")]
#[cfg_attr(feature = "database", derive(sqlx::Type))]
#[cfg_attr(feature = "database", sqlx(transparent))]
struct DbTimestampTz(chrono::NaiveDateTime);

#[cfg(feature = "database")]
impl TryFrom<DbTimestampTz> for DateTime<Utc> {
    type Error = std::convert::Infallible;

    fn try_from(value: DbTimestampTz) -> Result<Self, Self::Error> {
        use chrono::TimeZone;

        Ok(Utc.from_utc_datetime(&value.0))
    }
}

/// A message's metadata object contains information about the stream where the
/// message resides, the previous message in a series of messages that make up a
/// messaging workflow, the originating process to which the message belongs, as
/// well as other data that are pertinent to understanding the provenance and
/// disposition of the message.
///
/// Where as a message's data represents information pertinent to the business
/// process that the message is involved with, a message's metadata contains
/// information that is mechanical and infrastructural. Message metadata is data
/// about messaging machinery, like message schema version, source stream,
/// positions, provenance, reply address, and the like.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Metadata {
    /// The name of the stream where the message resides.
    pub stream_name: Option<String>,
    /// The sequential position of the message in its stream.
    pub position: Option<i64>,
    /// The sequential position of the message in the entire message store.
    pub global_position: Option<i64>,
    /// The stream name of the message that precedes the message in a sequential
    /// [message flow](http://docs.eventide-project.org/user-guide/messages-and-message-data/messages.html#message-workflows).
    pub causation_message_stream_name: Option<String>,
    /// The sequential position of the causation message in its stream.
    pub causation_message_position: Option<i64>,
    /// The sequential position of the message in the entire message store.
    pub causation_message_global_position: Option<i64>,
    /// Name of the stream that represents an encompassing business process that
    /// coordinates the sub-process that the message is a part of.
    pub correlation_stream_name: Option<String>,
    /// Name of a stream where a reply should be sent as a result of processing
    /// the message.
    pub reply_stream_name: Option<String>,
    // /// Timestamp that the message was written to the message store.
    // #[serde(with = "ts_milliseconds")]
    // pub time: Option<DateTime<Utc>>,
    /// Version identifier of the message schema itself.
    pub schema_version: Option<String>,
    /// Additional properties.
    pub properties: HashMap<String, Value>,
    /// Additional local properties.
    pub local_properties: HashMap<String, Value>,
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
        let data = serde_json::from_value(self.data.unwrap_or_default())?;
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

impl Metadata {
    /// The de facto unique identifier for a message is a combination of the
    /// message's stream name and the message's position number within that
    /// stream.
    ///
    /// Returns the identifier is formatted as a URI fragment of the form
    /// stream_name/position.
    pub fn identifier(&self) -> Option<String> {
        Option::zip(self.stream_name.as_ref(), self.position)
            .map(|(stream_name, position)| format!("{stream_name}/{position}"))
    }

    /// The unique identifier for a message's causation message is a combination
    /// of the causation message's stream name and the causation message's
    /// position number within that stream.
    ///
    /// Returns the identifier is formatted as a URI fragment of the form
    /// `causation_message_stream_name/causation_message_position`.
    pub fn causation_message_identifier(&self) -> Option<String> {
        Option::zip(
            self.causation_message_stream_name.as_ref(),
            self.causation_message_position,
        )
        .map(|(stream_name, position)| format!("{stream_name}/{position}"))
    }

    /// When messages represent subsequent steps in a workflow, a subsequent
    /// message's metadata records elements of the preceding message's metadata.
    /// Each message in a workflow carries provenance data of the message that
    /// precedes it.
    ///
    /// The message's implementation of `follow` specifically manages the
    /// transfer of message data from the preceding message to the
    /// subsequent method, and then delegates to the metadata object to
    /// manage the transfer of message flow and provenance data between the
    /// two metadata objects.
    ///
    /// There are three metadata attributes that comprise the identifying
    /// information of a message's preceding message. They are collectively
    /// referred to as causation data.
    ///
    /// - `causation_message_stream_name`
    /// - `causation_message_position`
    /// - `causation_message_global_position`
    ///
    /// Each message's metadata in a workflow may also carry identifying
    /// information about the overall or coordinating workflow that the messages
    /// participates in. That identifying information is referred to as
    /// correlation data.
    ///
    /// - `correlation_stream_name`
    ///
    /// Additionally, a message's metadata may carry a *reply address*:
    ///
    /// - `reply_stream_name`
    pub fn follow(&mut self, preceding_metadata: Metadata) {
        self.causation_message_stream_name = preceding_metadata.stream_name;
        self.causation_message_position = preceding_metadata.position;
        self.causation_message_global_position = preceding_metadata.global_position;

        self.correlation_stream_name = preceding_metadata.correlation_stream_name;

        self.reply_stream_name = preceding_metadata.reply_stream_name;

        self.properties.extend(preceding_metadata.properties);
    }

    /// Metadata objects can be determined to follow each other using the
    /// metadata's follows? predicate method.
    ///
    /// Returns `true` when the metadata's causation and provenance attributes
    /// match the metadata argument's message source attributes.
    pub fn follows(&self, preceding_metadata: &Metadata) -> bool {
        if self.causation_message_stream_name.is_none() && preceding_metadata.stream_name.is_none()
        {
            return false;
        }

        if self.causation_message_stream_name != preceding_metadata.stream_name {
            return false;
        }

        if self.causation_message_position.is_none() && preceding_metadata.position.is_none() {
            return false;
        }

        if self.causation_message_position != preceding_metadata.position {
            return false;
        }

        if self.causation_message_global_position.is_none()
            && preceding_metadata.global_position.is_none()
        {
            return false;
        }

        if self.causation_message_global_position != preceding_metadata.global_position {
            return false;
        }

        if preceding_metadata.correlation_stream_name.is_some()
            && self.correlation_stream_name != preceding_metadata.correlation_stream_name
        {
            return false;
        }

        if preceding_metadata.reply_stream_name.is_some()
            && self.reply_stream_name != preceding_metadata.reply_stream_name
        {
            return false;
        }

        true
    }

    /// Clears the reply stream name, setting it to None.
    pub fn clear_reply_stream_name(&mut self) {
        self.reply_stream_name = None;
    }

    /// Is a reply.
    pub fn is_reply(&self) -> bool {
        self.reply_stream_name.is_some()
    }

    /// Is correlated with another stream name.
    pub fn is_correlated(&self, stream_name: &str) -> bool {
        let Some(correlation_stream_name) = &self.correlation_stream_name else {
            return false;
        };

        let stream_name = Category::normalize(stream_name);

        if StreamName::is_category(&stream_name) {
            StreamName::category(correlation_stream_name) == stream_name
        } else {
            correlation_stream_name == &stream_name
        }
    }
}

impl TryFrom<Option<Value>> for Metadata {
    type Error = serde_json::Error;

    fn try_from(value: Option<Value>) -> Result<Self, Self::Error> {
        match value {
            Some(value) => serde_json::from_value(value),
            None => Ok(Metadata::default()),
        }
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
