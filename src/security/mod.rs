use std::{
    path::PathBuf,
    io,
};

use tokio::fs;

extern crate anyhow;
use anyhow::{bail, Context, Result};
use tracing::info;

pub async fn init_security(key: &Option<PathBuf>, cert: &Option<PathBuf>) -> Result<(quinn::PrivateKey, quinn::CertificateChain)> {
    if let (Some(key_path), Some(cert_path)) = (&key, &cert) {
        let key = fs::read(key_path).await.context("Failed to read private key.")?;
        let key = if key_path.extension().map_or(false, |x| x == "der") {
            quinn::PrivateKey::from_der(&key)?
        } else {
            quinn::PrivateKey::from_pem(&key)?
        };
        let cert_chain = fs::read(cert_path).await.context("Failed to read certificate chain.")?;
        let cert_chain = if cert_path.extension().map_or(false, |x| x == "der") {
            quinn::CertificateChain::from_certs(quinn::Certificate::from_der(&cert_chain))
        } else {
            quinn::CertificateChain::from_pem(&cert_chain)?
        };

        Ok((key, cert_chain))
    } else {
        let dirs = directories::ProjectDirs::from("org", "quinn", "quinn-examples").unwrap();
        let path = dirs.data_local_dir();
        let cert_path = path.join("cert.der");
        let key_path = path.join("key.der");

        let (cert, key) = match fs::read(&cert_path).await.and_then( |x| Ok((x, std::fs::read(&key_path)?))) {
            Ok(x) => x,
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                info!("Generating self-signed certificate.");
                let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                let key = cert.serialize_private_key_der();
                let cert = cert.serialize_der().unwrap();

                fs::create_dir_all(&path).await.context("Failed to create certificate directory.")?;
                fs::write(&cert_path, &cert).await.context("Failed to write certificate.")?;
                fs::write(&key_path, &key).await.context("Failed to write private key")?;
                (cert, key)
            }
            Err(e) => {
                bail!("Failed to read certificate: {}.", e)
            }
        };

        let key = quinn::PrivateKey::from_der(&key)?;
        let cert = quinn::Certificate::from_der(&cert)?;

        Ok((key, quinn::CertificateChain::from_certs(vec![cert])))
    }
}