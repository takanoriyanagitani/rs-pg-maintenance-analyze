use std::env;
use std::io;

use std::process::ExitCode;

use tokio::net::TcpListener;

use async_graphql_axum::GraphQLRequest;
use async_graphql_axum::GraphQLResponse;

use rs_pg_maintenance_analyze::PgSchema;

async fn conn2schema(conn_str: &str) -> Result<PgSchema, io::Error> {
    rs_pg_maintenance_analyze::conn2schema(conn_str).await
}

async fn req2res(s: &PgSchema, req: GraphQLRequest) -> GraphQLResponse {
    s.execute(req.into_inner()).await.into()
}

async fn sub() -> Result<(), io::Error> {
    let conn_str = env::var("DB_CONNECTION_STRING")
        .expect("Environment variable DB_CONNECTION_STRING not set");

    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    let s: PgSchema = conn2schema(&conn_str).await?;
    let sdl: String = s.sdl();
    std::fs::write("./pg-maintenance-analyze.graphql", sdl.as_bytes())?;

    let listener = TcpListener::bind(listen_addr).await?;

    let app = axum::Router::new().route(
        "/",
        axum::routing::post(|req: GraphQLRequest| async move { req2res(&s, req).await }),
    );

    axum::serve(listener, app).await
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
