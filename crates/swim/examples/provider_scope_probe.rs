//! Test-only encrypted SWIM injector for the hub/site provider-scope qualification.
//!
//! Reads public probe configuration, a site PKCS8 PEM key, and a raw 32-byte mesh
//! key from three file arguments. Only public delivery metadata is printed.

use std::{
    error::Error,
    fs,
    io::{self, Write as _},
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crdt::{GridStateSnapshot, ProviderMetricsSnapshot, ProviderPhase, ProviderState};
use serde::{Deserialize, Serialize};
use swim::{AccumulatedOutput, NodeId, StateBroadcast, SwimNode, crypto, node::MAX_SWIM_PACKET_BYTES, signing};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum time to establish the probe's temporary membership.
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Time spent retransmitting queued custom broadcasts after joining.
const DELIVERY_WINDOW: Duration = Duration::from_secs(1);
/// Receive polling interval, also bounding the delivery loop's shutdown delay.
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(50);

/// Public fixture inputs; signing and mesh keys are supplied separately.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Site whose enrolled key signs the adversarial provider record.
    origin_site: String,
    /// Authenticated grid identity, distinct from the rejected network name.
    grid_id: String,
    /// Deliberately unauthorized provider network.
    network_id: String,
    /// Provider to impersonate in the authoritative update.
    provider_id: String,
    /// Model carried by the provider.
    model: String,
    /// Hub SWIM endpoint reachable from this process.
    target: SocketAddr,
}

/// Non-secret evidence of the signed update and its transport delivery.
#[derive(Serialize)]
struct Evidence<'config> {
    /// Site whose signature the receiver must verify.
    origin_site: &'config str,
    /// Grid whose provider policy is exercised.
    grid_id: &'config str,
    /// Network deliberately rejected by the receiver.
    network_id: &'config str,
    /// Future but bounded revision used to expose poisoning on a vulnerable receiver.
    revision: u64,
    /// Custom-broadcast datagrams actually sent to the hub.
    broadcast_datagrams: usize,
}

/// Load fixture keys without placing their contents in logs or process arguments.
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let config_path = args
        .next()
        .ok_or("expected configuration, signing-key, and mesh-key files")?;
    let signing_key_path = args.next().ok_or("missing signing-key file")?;
    let mesh_key_path = args.next().ok_or("missing mesh-key file")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let config: Config = serde_json::from_slice(&fs::read(config_path)?)?;
    let private_key = rcgen::KeyPair::from_pem(&fs::read_to_string(signing_key_path)?)?.serialize_der();
    let mesh_key: crypto::SwimKey = fs::read(mesh_key_path)?
        .try_into()
        .map_err(|_bytes| "mesh key must be 32 bytes")?;
    let evidence = inject(&config, &private_key, &mesh_key)?;
    writeln!(io::stdout().lock(), "{}", serde_json::to_string(&evidence)?)?;
    Ok(())
}

/// Construct a valid signature over an authoritative update for the wrong network.
fn signed_update(config: &Config, private_key: &[u8]) -> Result<StateBroadcast, Box<dyn Error>> {
    let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let revision = swim::state_broadcast::max_leased_revision(now_ms).saturating_sub(1_000);
    let mut snapshot = GridStateSnapshot::new(config.origin_site.clone());
    snapshot.upsert_provider(ProviderState {
        network_id: config.network_id.clone(),
        site_id: config.origin_site.clone(),
        provider_id: config.provider_id.clone(),
        routing_cluster: config.provider_id.clone(),
        models: vec![config.model.clone()],
        tools: Vec::new(),
        backend_kind: "local".to_owned(),
        capacity_weight: 1,
        phase: ProviderPhase::Available,
        metrics: ProviderMetricsSnapshot::default(),
        access_policy: crdt::ProviderAccessPolicy::default(),
        revision,
        writer_id: config.origin_site.clone(),
    });
    let mut broadcast = StateBroadcast::new(config.origin_site.clone(), revision, snapshot, None)
        .with_grid_id(Some(config.grid_id.clone()))
        .with_authoritative_provider_state()
        .with_signed_at(Some(now_ms));
    broadcast.signature = Some(signing::sign_ecdsa_p256(private_key, &broadcast.signable_bytes()?)?);
    Ok(broadcast)
}

/// Join as a separate relay, send the signed update, and leave without replacing the real site.
fn inject<'config>(
    config: &'config Config,
    private_key: &[u8],
    mesh_key: &crypto::SwimKey,
) -> Result<Evidence<'config>, Box<dyn Error>> {
    let route = UdpSocket::bind(if config.target.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" })?;
    route.connect(config.target)?;
    let socket = UdpSocket::bind(SocketAddr::new(route.local_addr()?.ip(), 0))?;
    socket.set_read_timeout(Some(RECEIVE_TIMEOUT))?;
    let mut node = SwimNode::new(NodeId::new("scope-probe".to_owned(), socket.local_addr()?));
    transmit(
        &socket,
        node.announce(NodeId::seed(config.target)),
        mesh_key,
        config.target,
    )?;
    let joining = Instant::now();
    while !node.live_identities().any(|peer| peer.socket_addr() == config.target) {
        if joining.elapsed() >= JOIN_TIMEOUT {
            return Err("probe could not join the hub SWIM endpoint".into());
        }
        receive(&socket, &mut node, mesh_key, config.target)?;
    }
    let update = signed_update(config, private_key)?;
    let revision = update.revision;
    node.publish_state_broadcast(&update)?;
    let mut broadcast_datagrams: usize = 0;
    let delivering = Instant::now();
    while delivering.elapsed() < DELIVERY_WINDOW {
        broadcast_datagrams =
            broadcast_datagrams.saturating_add(transmit(&socket, node.broadcast(), mesh_key, config.target)?);
        receive(&socket, &mut node, mesh_key, config.target)?;
    }
    transmit(&socket, node.leave(), mesh_key, config.target)?;
    if broadcast_datagrams == 0 {
        return Err("no custom-broadcast datagrams sent to the hub".into());
    }
    Ok(Evidence {
        origin_site: &config.origin_site,
        grid_id: &config.grid_id,
        network_id: &config.network_id,
        revision,
        broadcast_datagrams,
    })
}

/// Encrypt and send only traffic destined for the qualification's hub.
fn transmit(
    socket: &UdpSocket,
    output: AccumulatedOutput,
    mesh_key: &crypto::SwimKey,
    target: SocketAddr,
) -> Result<usize, Box<dyn Error>> {
    let mut sent: usize = 0;
    for message in output.messages.into_iter().filter(|message| message.addr == target) {
        let encrypted = crypto::encrypt(mesh_key, &message.data)?;
        if encrypted.len() > MAX_SWIM_PACKET_BYTES {
            return Err("encrypted probe packet exceeds the SWIM transport budget".into());
        }
        socket.send_to(&encrypted, target)?;
        sent = sent.saturating_add(1);
    }
    Ok(sent)
}

/// Service a bounded receive step while Foca establishes and maintains the relay's membership.
fn receive(
    socket: &UdpSocket,
    node: &mut SwimNode,
    mesh_key: &crypto::SwimKey,
    target: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    let mut buffer = [0_u8; MAX_SWIM_PACKET_BYTES];
    match socket.recv_from(&mut buffer) {
        Ok((length, _)) => {
            let bytes = buffer.get(..length).ok_or("invalid UDP receive length")?;
            let plaintext = crypto::decrypt(mesh_key, bytes)?;
            transmit(socket, node.handle_data(&plaintext), mesh_key, target)?;
        },
        Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {},
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[test]
    fn fixture_signs_the_foreign_network_with_a_valid_origin_key() -> Result<(), Box<dyn Error>> {
        let private_key = rcgen::KeyPair::generate()?.serialize_der();
        let config = config("127.0.0.1:7946".parse()?, "denied");
        let update = signed_update(&config, &private_key)?;
        let public_key = signing::public_key_ecdsa_p256(&private_key)?;
        let signature = update.signature.as_deref().ok_or("missing signature")?;
        signing::verify_ecdsa_p256(&public_key, &update.signable_bytes()?, signature)?;
        assert_eq!(
            update.grid_id.as_deref(),
            Some("authenticated-grid"),
            "the grid remains authentic"
        );
        assert!(
            update
                .snapshot
                .providers
                .values()
                .all(|provider| provider.network_id == "denied"),
            "only provider scope is forged"
        );
        assert!(
            update.revision > update.signed_at_ms.unwrap_or(0).saturating_add(60_000),
            "a vulnerable receiver cannot recover by accepting a normal immediate update"
        );
        Ok(())
    }

    #[test]
    fn encrypted_udp_probe_delivers_valid_state_and_rejects_cross_network_state() -> Result<(), Box<dyn Error>> {
        for network in ["grid", "denied"] {
            let private_key = rcgen::KeyPair::generate()?.serialize_der();
            let public_key = signing::public_key_ecdsa_p256(&private_key)?;
            let socket = UdpSocket::bind("127.0.0.1:0")?;
            let target = socket.local_addr()?;
            let config = config(target, network);
            let mesh_key = [7_u8; 32];
            let stop = Arc::new(AtomicBool::new(false));
            let server_stop = Arc::clone(&stop);
            let server = std::thread::spawn(move || serve(socket, public_key, mesh_key, &server_stop));
            let delivered = inject(&config, &private_key, &mesh_key);
            stop.store(true, Ordering::Relaxed);
            let state = server.join().map_err(|_panic| "receiver thread panicked")??;
            let evidence = delivered?;
            assert!(
                evidence.broadcast_datagrams > 0,
                "the encrypted custom broadcast was sent"
            );
            assert_eq!(
                state.providers.is_empty(),
                network == "denied",
                "valid transport and signature alone cannot authorize {network}"
            );
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Public configuration for the loopback receiver fixture.
    fn config(target: SocketAddr, network: &str) -> Config {
        Config {
            origin_site: "site-a".to_owned(),
            grid_id: "authenticated-grid".to_owned(),
            network_id: network.to_owned(),
            provider_id: "vcr".to_owned(),
            model: "model".to_owned(),
            target,
        }
    }

    /// Run the actual receiver with signature and network policy on an encrypted UDP socket.
    fn serve(
        socket: UdpSocket,
        public_key: Vec<u8>,
        mesh_key: crypto::SwimKey,
        stop: &AtomicBool,
    ) -> Result<GridStateSnapshot, String> {
        socket
            .set_read_timeout(Some(RECEIVE_TIMEOUT))
            .map_err(|error| error.to_string())?;
        let address = socket.local_addr().map_err(|error| error.to_string())?;
        let mut node = SwimNode::new(NodeId::new("hub".to_owned(), address));
        node.enforce_signed_grid("authenticated-grid", "grid");
        node.pin_origin_for_grid("authenticated-grid", "site-a".to_owned(), vec![public_key])
            .map_err(|error| error.to_string())?;
        let mut buffer = [0_u8; MAX_SWIM_PACKET_BYTES];
        while !stop.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buffer) {
                Ok((length, _)) => {
                    let bytes = buffer.get(..length).ok_or("invalid UDP length")?;
                    let plaintext = crypto::decrypt(&mesh_key, bytes).map_err(|error| error.to_string())?;
                    for message in node.handle_data(&plaintext).messages {
                        let encrypted = crypto::encrypt(&mesh_key, &message.data).map_err(|error| error.to_string())?;
                        socket
                            .send_to(&encrypted, message.addr)
                            .map_err(|error| error.to_string())?;
                    }
                },
                Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {},
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(node.state_snapshot())
    }
}
