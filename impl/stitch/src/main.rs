use gdal::raster::Driver;
use failure::ResultExt;
use protobuf::{Filter, Image, ImageListRequest, ImageManagementClient, Node, NodeLocateRequest, NodeManagementClient};
use structopt::StructOpt;
use st_image::coordinate::Geocode;
use tonic::Request;

mod tile;
use tile::Tile;

use std::error::Error;
use std::ffi::CString;
use std::net::IpAddr;
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
    let tiles = Arc::new(RwLock::new(Vec::new()));

    // start worker threads
    let mut join_handles = Vec::new();
    for _ in 0..opt.thread_count {
        let geohash_rx = geohash_rx.clone();
        let tiles = tiles.clone();
        let opt = opt.clone();

        let join_handle = std::thread::spawn(move || {
            'geohash: for geohash in geohash_rx.iter() {
                let geohash: &String = &geohash;
                println!("{}", geohash);

                // find node responsible for this geohash
                let node = match locate_node(&opt.ip_address,
                        opt.port, geohash) {
                    Ok(node) => node,
                    Err(e) => panic!("failed to locate node: {}", e),
                };

                let end_timestamp = opt.timestamp 
                    + (86400 - (opt.timestamp % 86400));

                // retrieve sentinel-2 images
                let sentinel2_filter = Filter {
                    end_timestamp: Some(end_timestamp),
                    geocode: Some(geohash.to_string()),
                    max_cloud_coverage: None,
                    min_pixel_coverage: Some(1.0),
                    platform: Some("Sentinel-2".to_string()),
                    recurse: false,
                    source: None,
                    start_timestamp:
                        Some(end_timestamp - (15 * 86400) + 1),
                };

                let sentinel2_images = match get_images(&opt.album,
                        sentinel2_filter, &node.rpc_addr) {
                    Ok(images) => images,
                    Err(e) => panic!("failed to get sentinel-2: {}", e),
                };

                let sentinel2_images: Vec<Image> = sentinel2_images
                    .into_iter().filter(|x| x.files.len() == 4).collect();

                // if sentinel-2 image on timestamp -> use stip
                println!("  found {} sentinel-2 image(s)",
                    sentinel2_images.len());
                for image in sentinel2_images.iter() {
                    if (image.timestamp - opt.timestamp).abs() <= 86400 {
                        println!("    using {}", image.timestamp);
                        let mut tiles = tiles.write().unwrap();
                        tiles.push(
                            Tile::Stip(node.clone(), image.clone()));

                        continue 'geohash;
                    }
                }

                // retrieve modis images
                let modis_filter = Filter {
                    end_timestamp: Some(end_timestamp),
                    geocode: Some(geohash.to_string()),
                    max_cloud_coverage: None,
                    min_pixel_coverage: None,
                    platform: Some("MODIS".to_string()),
                    recurse: false,
                    source: None,
                    start_timestamp:
                        Some(end_timestamp - (10 * 86400) + 1),
                };

                let modis_images = match get_images(&opt.album,
                        modis_filter, &node.rpc_addr) {
                    Ok(images) => images,
                    Err(e) => panic!("failed to get modis: {}", e),
                };

                let modis_images: Vec<Image> = modis_images.into_iter()
                    .filter(|x| x.files.len() == 2).collect();

                // if two sentinel-2 images and one modis -> use SATnet
                println!("  found {} modis image(s)",
                    modis_images.len());
                if sentinel2_images.len() >= 2
                        && modis_images.len() >= 1 {
                    let mut tiles = tiles.write().unwrap();
                    tiles.push(Tile::SATnet(node.clone(),
                        sentinel2_images[..2].to_vec(),
                        modis_images[0].clone()));

                    continue 'geohash;
                }

                println!("  image unavailable");
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
    let tiles = tiles.read().unwrap();
    for tile in tiles.iter() {
        let dataset = match tile.download() {
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
async fn get_images(album: &str, filter: Filter, rpc_address: &str)
        -> Result<Vec<Image>, Box<dyn Error>> {
    // initialize ImageManagement grpc client
    let mut client = ImageManagementClient::connect(
        format!("http://{}", rpc_address)).await?;

    // initialize ImageListRequest
    let request = ImageListRequest {
        album: album.to_string(),
        filter: filter,
    };

    // iterate over image stream
    let mut stream = client.list(Request::new(request.clone()))
        .await?.into_inner();

    let mut images = Vec::new();
    while let Some(image) = stream.message().await? {
        images.push(image);
    }

    // sort in descending order by timstamp
    images.sort_by(|a, b| b.timestamp.partial_cmp(&a.timestamp).unwrap());
    Ok(images)
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
