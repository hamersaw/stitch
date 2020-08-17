#[macro_use]
extern crate log;

use comm::Server as CommServer;
use protobuf::StitchServer;
use structopt::StructOpt;
use swarm::prelude::{DhtBuilder, SwarmConfigBuilder};
use tonic::transport::Server;

mod rpc;
use rpc::StitchImpl;

use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::Arc;

fn main() {
    // initilaize logger
    env_logger::init();

    // parse arguments
    let opt = Opt::from_args();
 
    /*// build swarm config
    let swarm_config = SwarmConfigBuilder::new()
        .addr(SocketAddr::new(opt.ip_addr, opt.gossip_port))
        .gossip_interval_ms(2000)
        .build().expect("build swarm config");

    // build dht
    let dht_builder = DhtBuilder::new()
        .id(opt.node_id)
        .rpc_addr(SocketAddr::new(opt.ip_addr, opt.rpc_port))
        .swarm_config(swarm_config)
        .tokens(opt.tokens)
        .xfer_addr(SocketAddr::new(opt.ip_addr, opt.xfer_port));

    let dht_builder = if let Some(ip_addr) = opt.seed_ip_addr {
        dht_builder.seed_addr(SocketAddr::new(ip_addr, opt.seed_port))
    } else {
        dht_builder
    };

    let (mut swarm, dht) = dht_builder.build().expect("build dht");

    // start swarm
    swarm.start().expect("swarm start");*/

    /*// start transfer server
    let listener = TcpListener::bind(format!("{}:{}",
        opt.ip_addr, opt.xfer_port)).expect("xfer service bind");
    let transfer_stream_handler =
        Arc::new(TransferStreamHandler::new(album_manager.clone()));
    let mut server = CommServer::new(listener,
        50, transfer_stream_handler);

    server.start().expect("transfer server start");*/

    // start GRPC server
    let addr = SocketAddr::new(opt.ip_addr, opt.rpc_port);

    let stitch = StitchImpl::new();
    if let Err(e) = start_rpc_server(addr, stitch) {
        panic!("failed to start rpc server: {}", e);
    }

    // wait indefinitely
    //thread::park();
}

#[tokio::main]
async fn start_rpc_server(addr: SocketAddr, stitch: StitchImpl)
        -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(StitchServer::new(stitch))
        .serve(addr).await?;

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "stitchd", about="Node in the stitch framework.")]
struct Opt {
    #[structopt(name="NODE_ID", help="Integer node identifier.")]
    node_id: u16,

    #[structopt(short="i", long="ip-address",
        help="gossip ip address.", default_value="127.0.0.1")]
    ip_addr: IpAddr,

    #[structopt(short="p", long="port",
        help="gossip port.", default_value="15705")]
    gossip_port: u16,

    #[structopt(short="r", long="rpc-port",
        help="rpc port.", default_value="15706")]
    rpc_port: u16,

    #[structopt(short="s", long="seed-ip-address", help="seed ip address.")]
    seed_ip_addr: Option<IpAddr>,

    #[structopt(short="e", long="seed-port",
        help="seed port.", default_value="15705")]
    seed_port: u16,

    #[structopt(short="x", long="xfer-port",
        help="data transfer port.", default_value="15707")]
    xfer_port: u16,
}
