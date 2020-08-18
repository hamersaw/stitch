use protobuf::{ImageBroadcastRequest, ImageBroadcastType, ImageListRequest, ImageManagementClient, Node, NodeLocateRequest, NodeManagementClient};
use structopt::StructOpt;
use st_image::prelude::Geocode;
use tonic::Request;

use std::error::Error;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(name="stitch")]
struct Opt {
    #[structopt(short, long,
        help="stip node ip address", default_value="127.0.0.1")]
    ip_address: IpAddr,

    #[structopt(name="MIN_LATITUDE", help="minimum bounding latitude")]
    min_latitude: f64,

    #[structopt(name="MAX_LATITUDE", help="maximum bounding latitude")]
    max_latitude: f64,

    #[structopt(name="MIN_LONGITUDE",
        help="minimum bounding longitude")]
    min_longitude: f64,

    #[structopt(name="MAX_LONGITUDE",
        help="maximum bounding longtude")]
    max_longitude: f64,

    #[structopt(short, long,
        help="stip node rpc port", default_value="15606")]
    port: u16,

    #[structopt(name="TIMESTAMP", help="image timestamp")]
    timestamp: u64,

    #[structopt(name="OUTPUT_FILE", help="image output file")]
    output_file: PathBuf,
}

fn main() {
    // parse command line options
    let opt = Opt::from_args();

    // identify geohash windows in bounding box
    let geocode = Geocode::Geohash;
    let (longitude_interval, latitude_interval) =
        geocode.get_intervals(5);
    let windows = st_image::prelude::get_window_bounds(
        opt.min_longitude, opt.max_longitude,
        opt.min_latitude, opt.max_latitude,
        longitude_interval, latitude_interval);

    // process windows
    for (min_long, max_long, min_lat, max_lat) in windows.iter() {
        // compute window geohash
        let geohash = match geocode.get_code(
                (min_long + max_long) / 2.0,
                (min_lat + max_lat) / 2.0, 5) {
            Ok(geohash) => geohash,
            Err(e) => panic!("failed to compute geohash: {}", e),
        };

        println!("processing '{}'", geohash);

        // find node responsible for this geohash
        let node = match locate_node(&opt.ip_address,
                opt.port, &geohash) {
            Ok(node) => node,
            Err(e) => panic!("failed to locate node: {}", e),
        };

        println!("  [+] found on node {}", node.id);

        // TODO - check for Sentinel-2 image

        // TODO - check for preceeding Sentinel-2 and MODIS images
    }

    // TODO - locate available geohash images
    /*for node in node_list_reply.nodes.iter() {
        // initialize ImageManagement grpc client
        let mut client = ImageManagementClient::connect(
            format!("http://{}", node.rpc_addr)).await?;

        // iterate over image stream
        let mut stream = client.list(Request::new(request.clone()))
            .await?.into_inner();
        while let Some(image) = stream.message().await? {
            for file in image.files.iter() {
                println!("{:<8}{:<12}{:<10}{:<8}{:<12}{:<16.5}{:<16.5}{:<12}{:<80}",
                    node.id, image.platform, image.geocode,
                    image.source, image.timestamp, file.pixel_coverage,
                    image.cloud_coverage.unwrap_or(-1.0),
                    file.subdataset, file.path);
            }
        }
    }*/

    /*// initialize Filter
    let filter = Filter {
        end_timestamp: opt.timestamp,
        geocode: geohash,
        max_cloud_coverage: crate::f64_opt(
            list_matches.value_of("max_cloud_coverage"))?,
        min_pixel_coverage: crate::f64_opt(
            list_matches.value_of("min_pixel_coverage"))?,
        platform: crate::string_opt(list_matches.value_of("platform")),
        recurse: list_matches.is_present("recurse"),
        source: crate::string_opt(list_matches.value_of("source")),
        start_timestamp: crate::i64_opt(
            list_matches.value_of("start_timestamp"))?,
    };

    // initialize ImageListRequest
    let request = ImageListRequest {
        album: list_matches.value_of("ALBUM").unwrap().to_string(),
        filter: filter,
    };*/

    // TODO - stitch together image
    //  - download images
    //  - generate missing images
}

#[tokio::main]
async fn locate_node(ip_address: &IpAddr, port: u16,
        geohash: &str) -> Result<Node, Box<dyn Error>> {
    // initialize NodeManagement grpc client
    let mut client = NodeManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize NodeLocateRequest
    let request = Request::new(NodeLocateRequest {
        geocode: geohash.to_string(),
    });

    // retrieve NodeLocateReply
    let reply = client.locate(request).await?;
    let reply = reply.get_ref();

    // process node
    match &reply.node {
        Some(node) => Ok(node.clone()),
        None => Err(format!("failed to locate geocode '{}'",
            geohash).into()),
    }
}

/*    // retrieve stip cluster nodes
    let nodes = match get_nodes(&opt.ip_address, opt.port) {
        Ok(nodes) => nodes,
        Err(e) => panic!("failed to retrieve nodes: {}", e),
    };

#[tokio::main]
async fn get_nodes(ip_address: &IpAddr, port: u16)
        -> Result<HashMap<u32, String>, Box<dyn Error>> {
    // initialize NodeManagement grpc client
    let mut client = NodeManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize NodeListRequest
    let node_list_request = Request::new(NodeListRequest {});

    // retrieve NodeListReply
    let node_list_reply = client.list(node_list_request).await?;
    let node_list_reply = node_list_reply.get_ref();

    // populate nodes map
    let mut nodes = HashMap::new();
    for node in node_list_reply.nodes.iter() {
        nodes.insert(node.id, node.xfer_addr.clone());
    }

    Ok(nodes)
}*/
