use byteorder::{ReadBytesExt, WriteBytesExt};
use gdal::raster::{Dataset, Driver};
use failure::ResultExt;
use protobuf::{Filter, Image, ImageListRequest, ImageManagementClient, Node, NodeLocateRequest, NodeManagementClient};
use structopt::StructOpt;
use st_image::coordinate::Geocode;
use tonic::Request;

use std::error::Error;
use std::ffi::CString;
use std::io::{Read, Write};
use std::net::{IpAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, StructOpt)]
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

    #[structopt(name="MIN_LONGITUDE", help="minimum bounding longitude")]
    min_longitude: f64,

    #[structopt(name="MAX_LONGITUDE", help="maximum bounding longtude")]
    max_longitude: f64,

    #[structopt(short, long,
        help="stip node rpc port", default_value="15606")]
    port: u16,

    #[structopt(short, long, help="thread count", default_value="4")]
    thread_count: u8,

    #[structopt(name="TIMESTAMP", help="image timestamp")]
    timestamp: i64,

    #[structopt(name="OUTPUT_FILE", help="image output file")]
    output_file: PathBuf,
}

enum ImageDownload {
    SATnet,
    Stip(Node, Image),
}

impl ImageDownload {
    fn execute(&self) -> Result<Dataset, Box<dyn Error>> {
        match self {
            ImageDownload::SATnet => unimplemented!(),
            ImageDownload::Stip(node, image) => {
                // connect to stip transfer service
                let mut stream = TcpStream::connect(&node.xfer_addr)?;

                // send readop
                stream.write_u8(0)?;

                // send path
                let path = &image.files[3].path;
                stream.write_u8(path.len() as u8)?;
                stream.write(path.as_bytes())?;

                // send subgeocode indicator
                stream.write_u8(0)?;

                // check for failure
                if stream.read_u8()? != 0 {
                    let len = stream.read_u8()?;
                    let mut buf = vec![0u8; len as usize];
                    stream.read_exact(&mut buf)?;
                    return Err(String::from_utf8(buf)?.into())
                }
                
                // read dataset
                let dataset = st_image::serialize::read(&mut stream)?;
                return Ok(dataset);
            },
        }
    }
}

fn main() {
    // parse command line options
    let opt = Opt::from_args();

    // identify geohash windows in bounding box
    let geocode = Geocode::Geohash;
    let (longitude_interval, latitude_interval) =
        geocode.get_intervals(5);
    let windows = st_image::coordinate::get_windows(
        opt.min_longitude, opt.max_longitude,
        opt.min_latitude, opt.max_latitude,
        longitude_interval, latitude_interval);

    // open channels
    let (geohash_tx, geohash_rx) = crossbeam_channel::unbounded();
    let images = Arc::new(RwLock::new(Vec::new()));

    // start worker threads
    let mut join_handles = Vec::new();
    for _ in 0..opt.thread_count {
        let geohash_rx = geohash_rx.clone();
        let images = images.clone();
        let opt = opt.clone();

        let join_handle = std::thread::spawn(move || {
            for geohash in geohash_rx.iter() {
                let geohash: &String = &geohash;

                // find node responsible for this geohash
                let node = match locate_node(&opt.ip_address,
                        opt.port, geohash) {
                    Ok(node) => node,
                    Err(e) => panic!("failed to locate node: {}", e),
                };

                // check for Sentinel-2 image
                let image = match get_sentinel_image(&node.rpc_addr,
                        &opt.album, geohash, opt.timestamp) {
                    Ok(image) => image,
                    Err(e) => panic!("failed to get sentinel: {}", e),
                };

                // if found -> retreive Sentinel-2 image
                if let Some(image) = image {
                    let mut images = images.write().unwrap();
                    images.push(ImageDownload::Stip(node, image));

                    continue;
                }

                // TODO - generate using SATnet

                println!("image for geohash {} unavailable", geohash);
            }
        });

        join_handles.push(join_handle);
    }

    // add geohashes to sender channel
    for (min_long, max_long, min_lat, max_lat) in windows.iter() {
        // compute window geohash
        let geohash = match geocode.encode(
                (min_long + max_long) / 2.0,
                (min_lat + max_lat) / 2.0, 5) {
            Ok(geohash) => geohash,
            Err(e) => panic!("failed to compute geohash: {}", e),
        };

        // send geohash down channel
        if let Err(e) = geohash_tx.send(geohash) {
            panic!("failed to send geohash: {}", e);
        }
    }

    // join worker threads
    drop(geohash_tx);
    for join_handle in join_handles {
        if let Err(e) = join_handle.join() {
            panic!("failed to join worker: {:?}", e);
        }
    }

    // download all images
    let mut datasets = Vec::new();
    let images = images.read().unwrap();
    for image in images.iter() {
        let dataset = match image.execute() {
            Ok(dataset) => dataset,
            Err(e) => panic!("failed to download image: {}", e),
        };

        datasets.push(dataset);
    }

    // merge datasets
    let dataset = match st_image::transform::merge(&datasets) {
        Ok(dataset) => dataset,
        Err(e) => panic!("failed to merge datasets: {}", e),
    };

    // split image on provided bounds
    let dataset = match st_image::transform::split(&dataset,
            opt.min_longitude, opt.max_longitude,
            opt.min_latitude, opt.max_latitude, 4326) {
        Ok(dataset) => dataset,
        Err(e) => panic!("failed to trim dataset: {}", e),
    };

    // open GeoTiff driver
    let driver = match Driver::get("GTiff").compat() {
        Ok(driver) => driver,
        Err(e) => panic!("failed to get GTiff driver: {}", e),
    };

    // initialize copy options
    let c_string = match CString::new("COMPRESS=LZW") {
        Ok(c_string) => c_string.into_raw(),
        Err(e) => panic!("failed to initialize c_options: {}", e),
    };

    let mut c_options = vec![c_string, std::ptr::null_mut()];

    // write image using GeoTiff format
    let path_str = opt.output_file.to_string_lossy();
    if let Err(e) = dataset.create_copy(&driver, &path_str,
            Some(c_options.as_mut_ptr())).compat() {
        panic!("failed to copy dataset: {}", e);
    }

    // clean up potential memory leaks
    unsafe {
        for ptr in c_options {
            if !ptr.is_null() {
                let _ = CString::from_raw(ptr);
            }
        }
    }
}

#[tokio::main]
async fn get_sentinel_image(rpc_address: &str,
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
        min_pixel_coverage: Some(1.0),
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
        if image_proto.files.len() == 4 {
            image = Some(image_proto);
        }
    }

    Ok(image)
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
