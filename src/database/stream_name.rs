use sqlx::database::{HasArguments, HasValueRef};
use sqlx::encode::IsNull;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};
use sqlx::{Database, Decode, Encode, Type};

use crate::stream_name::StreamName;

impl<'q, DB: Database> Encode<'q, DB> for StreamName
where
    String: Encode<'q, DB>,
{
    fn encode_by_ref(&self, buf: &mut <DB as HasArguments<'q>>::ArgumentBuffer) -> IsNull {
        <String as Encode<'q, DB>>::encode_by_ref(&self.to_string(), buf)
    }
}

impl<'r, DB: Database> Decode<'r, DB> for StreamName
where
    String: Decode<'r, DB>,
{
    fn decode(
        value: <DB as HasValueRef<'r>>::ValueRef,
    ) -> Result<
        Self,
        Box<dyn std::error::Error + 'static + ::std::marker::Send + ::std::marker::Sync>,
    > {
        let s = <String as Decode<'r, DB>>::decode(value)?;
        let stream_name = s.parse()?;
        Ok(stream_name)
    }
}

impl<DB: Database> Type<DB> for StreamName
where
    String: Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <String as Type<DB>>::type_info()
    }

    fn compatible(ty: &DB::TypeInfo) -> ::std::primitive::bool {
        <String as Type<DB>>::compatible(ty)
    }
}

impl PgHasArrayType for StreamName
where
    String: PgHasArrayType,
{
    fn array_type_info() -> PgTypeInfo {
        <String as PgHasArrayType>::array_type_info()
    }
}
