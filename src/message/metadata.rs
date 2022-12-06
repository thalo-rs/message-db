use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::stream_name::category::Category;
use crate::stream_name::StreamName;

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
