#[allow(unused_imports)]
use std::{
    ascii, io,
    net::SocketAddr,
    path::{self, Path, PathBuf},
    str,
    sync::Arc,
};

extern crate anyhow;
use anyhow::{anyhow, Result};
use futures::{StreamExt, TryFutureExt};
use structopt::{self, StructOpt};
use tracing::{error, info, info_span};
use tracing_futures::Instrument as _;
#[allow(unused_imports)]
use tokio::prelude::*;

mod security;

extern crate common;

#[derive(StructOpt, Debug)]
#[structopt(name = "server")]
struct Opt {

    #[structopt(long = "keylog")]
    keylog: bool,

    #[structopt(parse(from_os_str), short = "k", long = "key", requires = "cert")]
    key: Option<PathBuf>,

    #[structopt(parse(from_os_str), short = "c", long = "cert", requires = "key")]
    cert: Option<PathBuf>,

    #[structopt(long = "stateless-retry")]
    stateless_retry: bool,

    #[structopt(long = "listen", default_value = "0.0.0.0:8000")]
    listen: SocketAddr,
}


fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    ).unwrap();

    let mut rt = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap();
    let opt = Opt::from_args();
    let code = {
        if let Err(e) =  rt.block_on(run(opt)) {
            eprintln!("ERROR: {}", e);
            1
        } else {
            0
        }
    };

    std::process::exit(code);
}

//#[tokio::main]
async fn run(options: Opt) -> Result<()> {
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.stream_window_uni(0);
    let mut server_config = quinn::ServerConfig::default();
    server_config.transport = Arc::new(transport_config);
    let mut server_config = quinn::ServerConfigBuilder::new(server_config);
    server_config.protocols(common::ALPN_QUIC_HTTP);

    if options.keylog {
        server_config.enable_keylog();
    }

    if options.stateless_retry {
        server_config.use_stateless_retry(true);
    }
   
    let (key, cert_chain) = security::init_security(&options.key, &options.cert).await?;
    server_config.certificate(cert_chain, key)?;


    let mut endpoint = quinn::Endpoint::builder();
    endpoint.listen(server_config.build());

    let mut incoming = {
        let (endpoint, incoming) = endpoint.bind(&options.listen)?;
        info!("Listening on {}.", endpoint.local_addr()?);
        incoming
    };

    while let Some(conn) = incoming.next().await {
        info!("Connection incoming.");
        tokio::spawn(
            handle_connection(conn).unwrap_or_else(move |e| {
                error!("Connection failed: {reason}", reason = e.to_string())
            }),
        );
    }
    
    Ok(())
}

async fn handle_connection(conn: quinn::Connecting) -> Result<()> {
    let quinn::NewConnection {
        connection,
        mut bi_streams,
        ..
    } = conn.await?;

    let span = info_span!(
        "connection",
        remote = %connection.remote_address(),
        protocol = %connection
            .authentication_data()
            .protocol
            .map_or_else(|| "<none>".into(), |x| String::from_utf8_lossy(&x).into_owned())
    );

    async {
        info!("Established");

        while let Some(stream) = bi_streams.next().await {
            let stream = match stream {
                Err(quinn::ConnectionError::ApplicationClosed {..}) => {
                    info!("Connection closed.");
                    return Ok(());
                },
                Err(e) => {
                    return Err(e);
                },
                Ok(s) => s,
            };

            tokio::spawn(
                handle_request(stream)
                    .unwrap_or_else(move |e| error!("Failed: {reason}.", reason = e.to_string()))
                    .instrument(info_span!("Request")),
            );
        }

        Ok(())
    }
    .instrument(span)
    .await?;

    Ok(())
}

async fn handle_request((mut send, recv): (quinn::SendStream, quinn::RecvStream)) -> Result<()> {
    let req = recv
        .read_to_end(64 * 1024)
        .await
        .map_err(|e| anyhow!("Failed reading request: {}", e))?;

    use std::convert::TryFrom;
    let test = broker_proto::Protocol::try_from(&req[..]).unwrap();
    let escaped = format!("{:#?}", test);

    info!(content = %escaped);

    send.write_all(&req)
        .await
        .map_err(|e| anyhow!("Failed to send response: {}", e))?;
    
    send.finish()
        .await
        .map_err(|e| anyhow!("Failed to shutdown stream: {}", e))?;
    info!("Complete.");
    Ok(())
}

