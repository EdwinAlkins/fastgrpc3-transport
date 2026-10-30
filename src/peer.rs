//! What `grpc` tells a handler about an mTLS client, from its certificate: the identities
//! (`peer_identities`) and the authentication context (`auth_context`), with `grpc`'s keys.

use std::collections::HashMap;
use std::net::IpAddr;

use base64::Engine;
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

/// `grpc`'s authentication context of a TLS connection, with the client certificate's
/// properties when there is one (`der`, the leaf). The session details `grpc` adds
/// (`ssl_tls_version`, `ssl_session_reused`...) are not known here.
pub fn auth_context(der: Option<&[u8]>) -> HashMap<&'static str, Vec<Vec<u8>>> {
    let mut context = HashMap::from([
        ("transport_security_type", vec![b"ssl".to_vec()]),
        (
            "security_level",
            vec![b"TSI_PRIVACY_AND_INTEGRITY".to_vec()],
        ),
    ]);
    let Some((der, certificate)) = der.and_then(|der| Some((der, parse(der)?))) else {
        return context;
    };
    let names = alternative_names(&certificate);
    let mut add = |key, values: Vec<String>| {
        if !values.is_empty() {
            context.insert(key, values.into_iter().map(String::into_bytes).collect());
        }
    };
    add("x509_common_name", common_names(&certificate));
    add("x509_subject", vec![certificate.subject().to_string()]);
    add("peer_dns", names.dns.clone());
    add("peer_uri", names.uris.clone());
    let spiffe = names.uris.iter().filter(|uri| uri.starts_with("spiffe://"));
    add("peer_spiffe_id", spiffe.cloned().collect());
    add("x509_subject_alternative_name", names.all);
    add("x509_pem_cert", vec![pem(der)]);
    context
}

/// `grpc`'s peer identities: the subject alternative names, or the common name if none.
pub fn identities(der: &[u8]) -> Vec<Vec<u8>> {
    let Some(certificate) = parse(der) else {
        return Vec::new();
    };
    let names = alternative_names(&certificate).all;
    let names = if names.is_empty() {
        common_names(&certificate)
    } else {
        names
    };
    names.into_iter().map(String::into_bytes).collect()
}

fn parse(der: &[u8]) -> Option<X509Certificate<'_>> {
    X509Certificate::from_der(der)
        .ok()
        .map(|(_, certificate)| certificate)
}

fn common_names(certificate: &X509Certificate<'_>) -> Vec<String> {
    let names = certificate.subject().iter_common_name();
    names
        .filter_map(|name| name.as_str().ok().map(str::to_owned))
        .collect()
}

#[derive(Default)]
struct AlternativeNames {
    all: Vec<String>,
    dns: Vec<String>,
    uris: Vec<String>,
}

fn alternative_names(certificate: &X509Certificate<'_>) -> AlternativeNames {
    let mut names = AlternativeNames::default();
    let Ok(Some(extension)) = certificate.subject_alternative_name() else {
        return names;
    };
    for name in &extension.value.general_names {
        let text = match name {
            GeneralName::DNSName(dns) => {
                names.dns.push((*dns).to_owned());
                (*dns).to_owned()
            }
            GeneralName::URI(uri) => {
                names.uris.push((*uri).to_owned());
                (*uri).to_owned()
            }
            GeneralName::RFC822Name(email) => (*email).to_owned(),
            GeneralName::IPAddress(bytes) => match <[u8; 4]>::try_from(*bytes) {
                Ok(v4) => IpAddr::from(v4).to_string(),
                Err(_) => match <[u8; 16]>::try_from(*bytes) {
                    Ok(v6) => IpAddr::from(v6).to_string(),
                    Err(_) => continue,
                },
            },
            _ => continue,
        };
        names.all.push(text);
    }
    names
}

fn pem(der: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(der);
    let lines: Vec<&str> = encoded
        .as_bytes()
        .chunks(64)
        .map(|line| std::str::from_utf8(line).expect("base64 is ASCII"))
        .collect();
    format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        lines.join("\n")
    )
}
