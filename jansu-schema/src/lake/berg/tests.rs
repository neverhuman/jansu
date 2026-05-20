// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Iceberg lake house tests

use super::*;
use dotenvy::dotenv;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use rand::{distr::Alphanumeric, prelude::*, rng};
use std::{env::var, fs::File, marker::PhantomData, str::FromStr as _, sync::Arc, thread};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

pub(crate) fn alphanumeric_string(length: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn init_tracing() -> Result<DefaultGuard> {
    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[tokio::test]
async fn create_namespace() -> Result<()> {
    _ = dotenv().ok();
    let _guard = init_tracing()?;

    let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
    let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
    let warehouse = var("ICEBERG_WAREHOUSE").ok();
    let namespace = alphanumeric_string(5);
    debug!(catalog_uri, location_uri, ?warehouse, namespace);

    let schema_registry = Registry::from_str("memory://")?;

    let lake = Iceberg::new(
        Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
            .location(Url::parse(location_uri)?)
            .catalog(Url::parse(catalog_uri)?)
            .warehouse(warehouse.clone())
            .schema_registry(schema_registry)
            .namespace(Some(namespace.clone())),
    )
    .await?;

    let ident = lake.create_namespace().await?;
    assert_eq!(namespace, ident.to_url_string());

    Ok(())
}

#[tokio::test]
async fn create_duplicate_namespace() -> Result<()> {
    _ = dotenv().ok();
    let _guard = init_tracing()?;

    let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
    let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
    let warehouse = var("ICEBERG_WAREHOUSE").ok();
    let namespace = alphanumeric_string(5);
    debug!(catalog_uri, location_uri, ?warehouse, namespace);

    let schema_registry = Registry::from_str("memory://")?;

    {
        let lake = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse.clone())
                .schema_registry(schema_registry.clone())
                .namespace(Some(namespace.clone())),
        )
        .await?;

        let ident = lake.create_namespace().await?;
        assert_eq!(namespace, ident.to_url_string());
    }

    {
        let lake = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse)
                .schema_registry(schema_registry)
                .namespace(Some(namespace.clone())),
        )
        .await?;

        let ident = lake.create_namespace().await?;
        assert_eq!(namespace, ident.to_url_string());
    }

    Ok(())
}

#[tokio::test]
async fn create_table() -> Result<()> {
    _ = dotenv().ok();
    let _guard = init_tracing()?;

    let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
    let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
    let warehouse = var("ICEBERG_WAREHOUSE").ok();
    let namespace = alphanumeric_string(5);

    debug!(catalog_uri, location_uri, ?warehouse, namespace);

    let schema_registry = Registry::from_str("memory://")?;

    let lake_house = Iceberg::new(
        Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
            .location(Url::parse(location_uri)?)
            .catalog(Url::parse(catalog_uri)?)
            .namespace(Some(namespace.clone()))
            .schema_registry(schema_registry)
            .warehouse(warehouse.clone()),
    )
    .await?;

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()?;

    let table_name = alphanumeric_string(5);

    let table = lake_house.load_or_create_table(&table_name, schema).await?;
    assert_eq!(table_name, table.identifier().name());
    assert_eq!(namespace, table.identifier().namespace().to_url_string());

    Ok(())
}

#[tokio::test]
async fn create_duplicate_table() -> Result<()> {
    _ = dotenv().ok();
    let _guard = init_tracing()?;

    let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
    let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
    let warehouse = var("ICEBERG_WAREHOUSE").ok();
    let namespace = alphanumeric_string(5);
    let table_name = alphanumeric_string(5);

    debug!(catalog_uri, location_uri, ?warehouse, namespace, table_name);

    let schema_registry = Registry::from_str("memory://")?;

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()?;

    {
        let lake_house = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse.clone())
                .schema_registry(schema_registry.clone())
                .namespace(Some(namespace.clone())),
        )
        .await?;

        let table = lake_house
            .load_or_create_table(&table_name, schema.clone())
            .await?;
        assert_eq!(table_name, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());
    }

    {
        let lake_house = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone()))
                .schema_registry(schema_registry)
                .warehouse(warehouse),
        )
        .await?;

        let table = lake_house.load_or_create_table(&table_name, schema).await?;
        assert_eq!(table_name, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());
    }

    Ok(())
}

#[test]
fn url_parse() -> Result<()> {
    let uri = Url::parse("http://localhost:8181")?;
    assert_eq!("http://localhost:8181/", uri.as_str());
    assert_eq!("http", uri.scheme());
    assert!(uri.has_host());
    assert_eq!(Some("localhost"), uri.host_str());
    assert_eq!(Some(8181), uri.port());
    assert_eq!("/", uri.path());

    let uri = Url::parse("http://localhost:8181/catalog")?;
    assert_eq!("http://localhost:8181/catalog", uri.as_str());
    assert_eq!("http", uri.scheme());
    assert!(uri.has_host());
    assert_eq!(Some("localhost"), uri.host_str());
    assert_eq!(Some(8181), uri.port());
    assert_eq!("/catalog", uri.path());

    Ok(())
}
