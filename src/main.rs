use protobuf::{Filter, Image, ImageListRequest, ImageManagementClient, Node, NodeLocateRequest, NodeManagementClient};
use structopt::StructOpt;
use st_image::prelude::Geocode;
use tonic::Request;

use std::error::Error;
use std::net::IpAddr;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(name="stitch")]
struct Opt {
    #[structopt(short, long, help="stip album", default_value="test")]
    album: String,

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
    timestamp: i64,

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

        println!("[+] processing '{}'", geohash);

        // find node responsible for this geohash
        let node = match locate_node(&opt.ip_address,
                opt.port, &geohash) {
            Ok(node) => node,
            Err(e) => panic!("failed to locate node: {}", e),
        };

        println!("  [+] found geohash on node {}", node.id);

        // check for Sentinel-2 image
        let image = match get_sentinel_image(node.rpc_addr,
                &opt.album, &geohash, opt.timestamp) {
            Ok(image) => image,
            Err(e) => panic!("failed to check Sentinel-2 image: {}", e),
        };

        if let Some(_) = image {
            // TODO - add to processing list
            println!("  [+] found Sentinel-2 image");
            continue;
        } else {
            println!("  [|] unable to find Sentinel-2 image");
        }

        // TODO - check for preceeding Sentinel-2 and MODIS images
    }

    // TODO - process images
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

#[tokio::main]
async fn get_sentinel_image(rpc_address: String,
        album: &str, geohash: &str, timestamp: i64)
        -> Result<Option<Image>, Box<dyn Error>> {
    // initialize ImageManagement grpc client
    let mut client = ImageManagementClient::connect(
        format!("http://{}", rpc_address)).await?;

    // initialize Filter
    let filter = Filter {
        end_timestamp: Some(timestamp + 86400),
        geocode: Some(geohash.to_string()),
        max_cloud_coverage: None,
        min_pixel_coverage: Some(0.95),
        platform: Some("Sentinel-2".to_string()),
        recurse: false,
        source: None,
        start_timestamp: Some(timestamp - 86400),
    };

    // initialize ImageListRequest
    let request = ImageListRequest {
        album: album.to_string(),
        filter: filter,
    };

    // iterate over image stream
    let mut stream = client.list(Request::new(request.clone()))
        .await?.into_inner();

    let mut image = None;
    while let Some(image_proto) = stream.message().await? {
        image = Some(image_proto);
    }

    Ok(image)
}
