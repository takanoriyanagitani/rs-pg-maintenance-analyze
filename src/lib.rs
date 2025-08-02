use std::io;

use sqlx::PgPool;

use async_graphql::futures_util;

use futures_util::StreamExt;
use futures_util::TryStreamExt;

use async_graphql::EmptySubscription;
use async_graphql::Object;
use async_graphql::Schema;

pub struct UncheckedTableName(pub String);
pub struct CheckedTableName(String);

impl CheckedTableName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[async_trait::async_trait]
pub trait TableNameChecker: Sync + Send + 'static {
    async fn check_table_name(
        &self,
        schema: &str,
        unchecked: UncheckedTableName,
    ) -> Result<CheckedTableName, io::Error>;
}

#[async_trait::async_trait]
pub trait TableChecker: Sync + Send + 'static {
    async fn table_exists(&self, schema: &str, name: &str) -> Result<bool, io::Error>;
}

#[async_trait::async_trait]
impl<C> TableNameChecker for C
where
    C: TableChecker,
{
    async fn check_table_name(
        &self,
        schema: &str,
        unchecked: UncheckedTableName,
    ) -> Result<CheckedTableName, io::Error> {
        let raw_name: &str = &unchecked.0;
        let found: bool = self.table_exists(schema, raw_name).await?;
        if !found {
            return Err(io::Error::other(format!("the table {raw_name} not found")));
        }
        Ok(CheckedTableName(unchecked.0))
    }
}

pub struct PgTabChk {
    pub pool: PgPool,
}

#[async_trait::async_trait]
impl TableChecker for PgTabChk {
    async fn table_exists(&self, schema: &str, name: &str) -> Result<bool, io::Error> {
        let p: &PgPool = &self.pool;

        let oi: Option<i32> = sqlx::query_scalar!(
            r#"(
                SELECT 1::INTEGER AS one
                FROM information_schema.tables
                WHERE
                    table_schema = $1::TEXT
                    AND table_name = $2::TEXT
            )"#,
            schema,
            name,
        )
        .fetch_optional(p)
        .await
        .map(|o| o.flatten())
        .map_err(io::Error::other)?;

        match oi {
            Some(1) => Ok(true),
            Some(i) => Err(io::Error::other(format!("unexpected value got: {i}"))),
            None => Ok(false),
        }
    }
}

pub struct PgAnalyze {
    pub pool: PgPool,
}

impl PgAnalyze {
    pub async fn analyze(&self, table: &CheckedTableName) -> Result<(), io::Error> {
        let p: &PgPool = &self.pool;
        sqlx::query(&format!("ANALYZE {}", table.0))
            .execute(p)
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
}

pub struct MutationRoot {
    pub checker: Box<dyn TableNameChecker>,
    pub az: PgAnalyze,
}

impl MutationRoot {
    pub fn new_default(p: &PgPool) -> Self {
        let chk = PgTabChk { pool: p.clone() };
        Self {
            checker: Box::new(chk),
            az: PgAnalyze { pool: p.clone() },
        }
    }
}

#[Object]
impl MutationRoot {
    async fn analyze_by_table_name(&self, schema: String, name: String) -> Result<bool, io::Error> {
        // TableNameChecker should reject unknown table "name"s
        let unchecked = UncheckedTableName(name);
        let checked: CheckedTableName = self.checker.check_table_name(&schema, unchecked).await?;
        self.az.analyze(&checked).await?;
        Ok(true)
    }

    async fn analyze_tables(&self, schema: String, names: Vec<String>) -> Result<bool, io::Error> {
        for name in names {
            // TableNameChecker should reject unknown table "name"s
            let unchecked = UncheckedTableName(name);
            let checked: CheckedTableName =
                self.checker.check_table_name(&schema, unchecked).await?;
            self.az.analyze(&checked).await?;
        }
        Ok(true)
    }
}

pub struct PgQuery {
    pub pool: PgPool,
}

#[Object]
impl PgQuery {
    pub async fn get_table_names(
        &self,
        schema: Option<String>,
        table_name_pattern: Option<String>,
    ) -> Result<Vec<String>, io::Error> {
        sqlx::query_scalar!(
            r#"(
                SELECT table_name
                FROM information_schema.tables
                WHERE
                    table_schema = $1::TEXT
                    AND table_name LIKE $2::TEXT
            )"#,
            schema.unwrap_or_else(|| "public".into()),
            table_name_pattern.unwrap_or_else(|| "%".into()),
        )
        .fetch(&self.pool)
        .map(|r| {
            r.map_err(io::Error::other)
                .and_then(|o| o.ok_or(io::Error::other("non empty table name expected")))
        })
        .try_collect()
        .await
    }
}

pub type PgSchema = Schema<PgQuery, MutationRoot, EmptySubscription>;

pub fn schema_new(q: PgQuery, m: MutationRoot) -> PgSchema {
    Schema::build(q, m, EmptySubscription).finish()
}

pub fn schema_new_default(p: &PgPool) -> PgSchema {
    let pg_query = PgQuery { pool: p.clone() };
    let mutation_root = MutationRoot::new_default(p);
    schema_new(pg_query, mutation_root)
}

pub async fn conn2pool(conn_str: &str) -> Result<PgPool, io::Error> {
    PgPool::connect(conn_str).await.map_err(io::Error::other)
}

pub async fn conn2schema(conn_str: &str) -> Result<PgSchema, io::Error> {
    let pool = conn2pool(conn_str).await?;
    Ok(schema_new_default(&pool))
}
