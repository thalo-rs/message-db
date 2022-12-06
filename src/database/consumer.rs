use std::collections::VecDeque;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::SelectAll;
use futures::{ready, FutureExt, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use tokio::time::Instant;
use tokio_util::sync::ReusableBoxFuture;
use tracing::{error, info};
use typed_builder::TypedBuilder;

use crate::database::client::{GetCategoryMessagesOpts, MessageDb, WriteMessageOpts};
use crate::message::{DeserializeMessage, GenericMessage, Message};
use crate::stream_name::{Category, StreamName, ID};
use crate::{Error, Result};

/// Options for [`MessageDb::subscribe_to_category`].
#[derive(Clone, Debug, Default, PartialEq, Eq, TypedBuilder)]
pub struct SubscribeToCategoryOpts<'a> {
    #[builder(default = Duration::from_millis(100))]
    poll_interval: Duration,
    #[builder(default, setter(strip_option))]
    batch_size: Option<i64>,
    #[builder(default = 100)]
    position_update_interval: usize,
    #[builder(default, setter(into, strip_option))]
    identifier: Option<&'a str>,
    #[builder(default, setter(strip_option))]
    correlation: Option<&'a str>,
    #[builder(default, setter(strip_option))]
    group_member: Option<i64>,
    #[builder(default, setter(strip_option))]
    group_size: Option<i64>,
    #[builder(default, setter(strip_option))]
    condition: Option<&'a str>,
}

#[derive(
    Clone, Copy, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct Recorded {
    position: i64,
}

impl MessageDb {
    /// Returns a new consumer position stream name for the `category`.
    pub fn position_stream_name(
        mut category: Category,
        consumer_identifier: Option<&str>,
    ) -> Result<StreamName> {
        const POSITION_TYPE: &str = "position";

        if !category.types.iter().any(|t| t == POSITION_TYPE) {
            category.types.push(POSITION_TYPE.to_string());
        }

        let id = consumer_identifier.map(ID::from_str).transpose()?;

        Ok(StreamName { category, id })
    }

    /// Subscribes to multiple categories into a combined stream.
    ///
    /// See [`MessageDb::subscribe_to_category`].
    pub async fn subscribe_to_categories<'a, 'b, 'e, 'c: 'a + 'e, T, E>(
        executor: E,
        category_names: &[&'a str],
        opts: &'b SubscribeToCategoryOpts<'a>,
    ) -> Result<SelectAll<CategoryStream<'a, E, T>>>
    where
        T: for<'de> Deserialize<'de> + 'a,
        E: 'a + 'c + 'e + sqlx::Executor<'c, Database = Postgres> + Clone + Send + Sync,
    {
        let streams = futures::future::join_all(category_names.iter().map(|category_name| {
            Self::subscribe_to_category::<T, E>(executor.clone(), category_name, opts).boxed()
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        Ok(futures::stream::select_all(streams))
    }

    /// Subscribes to a category, consuming messages as a stream.
    ///
    /// The consumer position is saved every
    /// `SubscribeToCategoryOpts::position_update_interval` messages consumed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use futures::StreamExt;
    /// use message_db::database::{MessageDb, SubscribeToCategoryOpts};
    /// use message_db::message::MessageData;
    ///
    /// let mut stream = MessageDb::subscribe_to_category::<MessageData, _>(
    ///     &message_db,
    ///     "account",
    ///     &SubscribeToCategoryOpts::builder()
    ///         .identifier("my_app")
    ///         .build(),
    /// )
    /// .await?;
    ///
    /// while let Some(command) = stream.next().await {
    ///     /* ... */
    /// }
    /// ```
    pub async fn subscribe_to_category<'a, 'b, 'e, 'c: 'a + 'e, T, E>(
        executor: E,
        category_name: &'a str,
        opts: &'b SubscribeToCategoryOpts<'a>,
    ) -> Result<CategoryStream<'a, E, T>>
    where
        T: for<'de> Deserialize<'de> + 'a,
        E: 'a + 'c + 'e + sqlx::Executor<'c, Database = Postgres> + Clone,
    {
        let stream_name =
            Self::position_stream_name(category_name.parse()?, opts.identifier)?.to_string();
        let last_message =
            Self::get_last_stream_message(executor.clone(), &stream_name, Some("position")).await?;
        let expected_version = last_message
            .as_ref()
            .map(|last| last.position)
            .unwrap_or(-1);
        let last_position = last_message
            .map(|last| {
                last.deserialize_data::<Recorded>()
                    .map(|recorded| recorded.position + 1)
            })
            .transpose()
            .map_err(|_| Error::Decode {
                expected: "recorded position",
            })?
            .unwrap_or(-1);

        let fut = ReusableBoxFuture::new(make_future(
            executor.clone(),
            category_name,
            GetCategoryMessagesOpts {
                position: Some(last_position),
                batch_size: opts.batch_size,
                correlation: opts.correlation,
                consumer_group_member: opts.group_member,
                consumer_group_size: opts.group_size,
                condition: opts.condition,
            },
            Duration::ZERO,
        ));

        Ok(CategoryStream {
            category_name,
            fut,
            message_db: executor,
            messages: VecDeque::new(),
            poll_interval: opts.poll_interval,
            position_update_interval: opts.position_update_interval,
            messages_since_last_position_update: 0,
            // position store
            update_position_future: None,
            consumer_stream_name: stream_name,
            expected_position_version: expected_version,
        })
    }

    /// Saves a consumer position.
    ///
    /// Consumer positions are automatically saved when using
    /// [`MessageDb::subscribe_to_category`].
    pub async fn write_consumer_position<'e, 'c: 'e, E>(
        executor: E,
        category_name: &str,
        identifier: Option<&str>,
        position: i64,
        opts: &WriteMessageOpts<'_>,
    ) -> Result<i64>
    where
        E: 'e + sqlx::Executor<'c, Database = Postgres>,
    {
        let stream_name =
            Self::position_stream_name(category_name.parse()?, identifier)?.to_string();
        Self::write_consumer_position_to_stream(executor, &stream_name, position, opts).await
    }

    /// Saves a consumer position to a stream name.
    pub async fn write_consumer_position_to_stream<'e, 'c: 'e, E>(
        executor: E,
        stream_name: &str,
        position: i64,
        opts: &WriteMessageOpts<'_>,
    ) -> Result<i64>
    where
        E: 'e + sqlx::Executor<'c, Database = Postgres>,
    {
        let data = Recorded { position };
        Self::write_message(
            executor,
            stream_name,
            "position",
            &serde_json::to_value(data).unwrap(),
            opts,
        )
        .await
    }
}

/// A category stream for consuming messages and storing the position.
///
/// This is returned by [`MessageDb::subscribe_to_category`].
#[pin_project]
pub struct CategoryStream<'a, E, T> {
    category_name: &'a str,
    fut: ReusableBoxFuture<
        'a,
        (
            Result<Vec<GenericMessage>>,
            GetCategoryMessagesOpts<'a>,
            Instant,
        ),
    >,
    message_db: E,
    messages: VecDeque<Message<T>>,
    poll_interval: Duration,
    position_update_interval: usize,
    messages_since_last_position_update: usize,
    // position store
    update_position_future: Option<BoxFuture<'a, Result<i64>>>,
    consumer_stream_name: String,
    expected_position_version: i64,
}

impl<'a, 'e, 'c: 'a + 'e, E, T> Stream for CategoryStream<'a, E, T>
where
    E: 'c + 'e + sqlx::Executor<'c, Database = Postgres> + Clone,
    T: for<'de> Deserialize<'de> + 'a,
{
    type Item = Result<Message<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let first_message = this.messages.pop_front();
        let fut_poll = this.fut.poll(cx);
        let pos_fut_poll = this
            .update_position_future
            .as_mut()
            .map(|pos_fut| pos_fut.poll_unpin(cx));

        if let Some(message) = first_message {
            return Poll::Ready(Some(Ok(message)));
        }

        if let Some(pos_fut_poll) = pos_fut_poll {
            match pos_fut_poll {
                Poll::Ready(Ok(pos)) => {
                    info!(position = pos, "saved consumer position");
                    *this.update_position_future = None;
                }
                Poll::Ready(Err(err)) => {
                    error!("failed to save consumer position: {err}");
                    *this.update_position_future = None;
                }
                Poll::Pending => {}
            }
        }
        let (result, mut opts, poll_time) = ready!(fut_poll);
        if let Ok(Some(last)) = result.as_ref().map(|messages| messages.last()) {
            opts.position = Some(last.global_position + 1);
        }

        let sleep_duration = this.poll_interval.saturating_sub(poll_time.elapsed());
        let next_fut = make_future(
            this.message_db.clone(),
            this.category_name,
            opts,
            sleep_duration,
        );
        this.fut.set(next_fut);

        match result {
            Ok(messages) if messages.is_empty() => Poll::Pending,
            Ok(messages) => {
                *this.messages_since_last_position_update += messages.len();
                if this.messages_since_last_position_update >= this.position_update_interval {
                    let pos = messages.first().unwrap().global_position;
                    *this.update_position_future = Some(
                        make_update_position_future(
                            this.message_db.clone(),
                            this.consumer_stream_name.clone(),
                            pos,
                            *this.expected_position_version,
                        )
                        .boxed(),
                    );
                    *this.expected_position_version += 1;
                    *this.messages_since_last_position_update = 0;
                }

                let messages: Result<Vec<_>, _> = messages.deserialize_messages();
                match messages {
                    Ok(messages) => {
                        *this.messages = messages.into();
                        Poll::Ready(Some(Ok(this.messages.pop_front().unwrap())))
                    }
                    Err(err) => Poll::Ready(Some(Err(err))),
                }
            }
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }
}

async fn make_future<'a, 'b, 'c, 'e, 'f: 'e, T, E>(
    executor: E,
    category_name: &'b str,
    opts: GetCategoryMessagesOpts<'c>,
    sleep: Duration,
) -> (
    Result<Vec<Message<T>>>,
    GetCategoryMessagesOpts<'c>,
    Instant,
)
where
    T: for<'de> Deserialize<'de> + 'a,
    E: 'f + sqlx::Executor<'f, Database = Postgres>,
{
    if !sleep.is_zero() {
        tokio::time::sleep(sleep).await;
    }
    let poll_time = Instant::now();
    let result = MessageDb::get_category_messages::<T, E>(executor, category_name, &opts).await;
    (result, opts, poll_time)
}

async fn make_update_position_future<'e, 'c: 'e, E>(
    executor: E,
    stream_name: String,
    pos: i64,
    expected_version: i64,
) -> Result<i64>
where
    E: 'e + sqlx::Executor<'c, Database = Postgres>,
{
    MessageDb::write_consumer_position_to_stream(
        executor,
        &stream_name,
        pos,
        &WriteMessageOpts::builder()
            .expected_version(expected_version)
            .build(),
    )
    .await?;
    Ok(pos)
}
