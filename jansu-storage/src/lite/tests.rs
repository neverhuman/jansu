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

use tempfile::tempdir;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use super::*;

fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(EnvFilter::from_default_env().add_directive(
                format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
            ))
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
async fn insert_with_returning() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");
    let db = libsql::Builder::new_local(file_path).build().await?;
    let connection = db.connect()?;

    let sql = "create table xyz (
            id integer primary key autoincrement,
            name text not null
            )";

    _ = connection.execute(sql, ()).await?;

    let sql = "insert into xyz (name) values (?1) returning xyz.name";

    let statement = connection.prepare(sql).await?;
    let mut rows = statement.query(&["abc"]).await?;
    let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
    let name = row.get_str(0).inspect(|name| debug!(name))?;
    assert_eq!("abc", name);

    Ok(())
}

#[tokio::test]
async fn insert_select_with_returning() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");
    let db = libsql::Builder::new_local(file_path).build().await?;
    let connection = db.connect()?;

    let sql = "create table pqr (
            id integer primary key autoincrement,
            name text not null
            )";

    _ = connection.execute(sql, ()).await?;

    let sql = "insert into pqr (name) values (?1)";
    let statement = connection.prepare(sql).await?;
    assert_eq!(1, statement.execute(&["fgh"]).await?);

    let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer references pqr (id) not null,
            name text not null
            )";

    _ = connection.execute(sql, ()).await?;

    let sql = "insert into xyz (pqr, name)
            select pqr.id, ?2
            from pqr
            where pqr.name = ?1
            returning xyz.name";

    let expected = "abc";
    let mut rows = connection.query(sql, &["fgh", expected]).await?;
    let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
    let actual = row.get_str(0).inspect(|name| debug!(name))?;
    assert_eq!(expected, actual);

    Ok(())
}

#[tokio::test]
async fn cte_insert_with_returning() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");
    let db = libsql::Builder::new_local(file_path).build().await?;
    let connection = db.connect()?;

    let sql = "create table pqr (
            id integer primary key autoincrement,
            name text not null
            )";

    _ = connection.execute(sql, ()).await?;

    let sql = "insert into pqr (name) values (?1)";
    let statement = connection.prepare(sql).await?;
    assert_eq!(1, statement.execute(&["fgh"]).await?);

    let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer references pqr (id) not null,
            name text not null
            )";

    _ = connection.execute(sql, ()).await?;

    let sql = "with qwe as (
                select pqr.id, ?2
                from pqr
                where pqr.name = ?1
            )
            insert into xyz (pqr, name)
            select * from qwe
            returning xyz.name";

    let expected = "abc";
    let mut rows = connection.query(sql, &["fgh", expected]).await?;
    let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
    let actual = row.get_str(0).inspect(|name| debug!(name))?;
    assert_eq!(expected, actual);

    Ok(())
}

#[tokio::test]
async fn simpler_insert_select_with_returning() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");
    let db = libsql::Builder::new_local(file_path).build().await?;
    let connection = db.connect()?;

    let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer not null,
            name text not null
            )";

    _ = connection.execute(sql, ()).await?;

    let expected = "abc";

    let sql = "insert into xyz (pqr, name)
            select 41 + 1, ?1
            returning xyz.pqr, xyz.name";

    let mut rows = connection.query(sql, &[expected]).await?;
    let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();

    assert_eq!(42, row.get::<i32>(0)?);

    let actual = row.get_str(1).inspect(|name| debug!(name))?;
    assert_eq!(expected, actual);

    Ok(())
}

#[tokio::test]
async fn create_topic() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");
    let db = libsql::Builder::new_local(file_path).build().await?;
    let connection = db.connect()?;

    assert_eq!(
        0,
        connection
            .execute(&include_sql!("../ddl/010-cluster.sql"), ())
            .await?
    );

    assert_eq!(
        0,
        connection
            .execute(&include_sql!("../ddl/020-topic.sql"), ())
            .await?
    );

    let cluster = "jansu";

    assert_eq!(
        1,
        connection
            .execute(
                &fix_parameters(&include_sql!("../sql/register_broker.sql"))?,
                &[cluster]
            )
            .await?
    );

    let name = "test";
    let uuid = Uuid::new_v4();
    let partitions = 3;
    let replication_factor = 3;

    let mut rows = connection
        .query(
            &fix_parameters(&include_sql!("../sql/topic_insert.sql"))?,
            (
                cluster,
                name,
                uuid.to_string(),
                partitions,
                replication_factor,
            ),
        )
        .await?;

    let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
    assert_eq!(uuid.to_string().as_str(), row.get_str(0)?);
    Ok(())
}

#[tokio::test]
async fn create_topic_in_tx() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");

    let db = libsql::Builder::new_local(file_path).build().await?;
    let tx = db.connect()?.transaction().await?;

    assert_eq!(
        0,
        tx.execute(&include_sql!("../ddl/010-cluster.sql"), ()).await?
    );

    assert_eq!(0, tx.execute(&include_sql!("../ddl/020-topic.sql"), ()).await?);

    let cluster = "jansu";

    assert_eq!(
        1,
        tx.execute(
            &fix_parameters(&include_sql!("../sql/register_broker.sql"))?,
            &[cluster]
        )
        .await?
    );

    let name = "test";
    let uuid = Uuid::new_v4();
    let partitions = 3;
    let replication_factor = 3;

    let mut rows = tx
        .query(
            &fix_parameters(&include_sql!("../sql/topic_insert.sql"))?,
            (
                cluster,
                name,
                uuid.to_string(),
                partitions,
                replication_factor,
            ),
        )
        .await?;

    let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
    assert_eq!(uuid.to_string().as_str(), row.get_str(0)?);
    Ok(())
}

#[tokio::test]
async fn lite_system_time() -> Result<()> {
    let _guard = init_tracing()?;

    let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
    let file_path = temp_dir.path().join("jansu.db");

    let db = libsql::Builder::new_local(file_path).build().await?;
    let connection = db.connect()?;

    assert_eq!(
        0,
        connection
            .execute(&include_sql!("../ddl/010-cluster.sql"), ())
            .await?
    );

    let name = "lite";

    _ = connection
        .execute(&include_sql!("../sql/register_broker.sql"), &[name])
        .await?;

    let mut rows = connection
        .query("select last_updated from cluster where name = ?1", &[name])
        .await?;
    let row = rows.next().await?.unwrap();
    let _timestamp = LiteTimestamp::try_from(row.get_value(0)?)?;

    Ok(())
}
