use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic_health::{ServingStatus, server::health_reporter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let cert = std::fs::read("certs/server.pem")?;
    let key = std::fs::read("certs/server-key.pem")?;
    let tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));

    let (reporter, health_service) = health_reporter();
    reporter.set_service_status("", ServingStatus::Serving).await;

    Server::builder()
        .tls_config(tls)?
        .add_service(health_service)
        .serve(addr)
        .await?;

    Ok(())
}
