use protobuf::{Filter, Image, ImageListRequest, ImageManagementClient};
use structopt::StructOpt;
use tonic::Request;

use std::cmp::Ordering;
use std::error::Error;
use std::net::IpAddr;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name="yogi")]
struct Opt {
    #[structopt(short, long, help="stip album", default_value="test")]
    album: String,

    #[structopt(short, long,
        help="stip node ip address", default_value="127.0.0.1")]
    ip_address: IpAddr,

    #[structopt(short, long,
        help="stip node rpc port", default_value="15606")]
    port: u16,

    #[structopt(short, long, help="thread count", default_value="4")]
    thread_count: u8,
}

fn main() {
    // parse command line options
    let opt = Opt::from_args();

    // get all Sentinel-2 images
    let sentinel2_filter = Filter {
        end_timestamp: None,
        geocode: None,
        max_cloud_coverage: None,
        min_pixel_coverage: Some(1.0),
        platform: Some("Sentinel-2".to_string()),
        recurse: false,
        source: None,
        start_timestamp: None,
    };

    let sentinel2_images = match get_images(&opt.album, sentinel2_filter,
            &format!("{}:{}", &opt.ip_address, opt.port)) {
        Ok(images) => images,
        Err(e) => panic!("failed to get sentinel-2: {}", e),
    };

    let sentinel2_images: Vec<Image> = sentinel2_images
        .into_iter().filter(|x| x.files.len() == 4).collect();

    // get all MODIS images
    let modis_filter = Filter {
        end_timestamp: None,
        geocode: None,
        max_cloud_coverage: None,
        min_pixel_coverage: None,
        platform: Some("MODIS".to_string()),
        recurse: false,
        source: None,
        start_timestamp: None,
    };

    let modis_images = match get_images(&opt.album, modis_filter,
            &format!("{}:{}", &opt.ip_address, opt.port)) {
        Ok(images) => images,
        Err(e) => panic!("failed to get modis: {}", e),
    };

    let modis_images: Vec<Image> = modis_images
        .into_iter().filter(|x| x.files.len() == 2).collect();

    // process SATnet images
    let (mut sentinel2_start_index, mut sentinel2_end_index) = (0, 0);
    let mut modis_index = 0;
    while modis_index < modis_images.len() {
        // adjust sentinel2 window
        while sentinel2_start_index + 1 < sentinel2_images.len()
                && sentinel2_images[sentinel2_start_index].geocode <
                modis_images[modis_index].geocode {
            sentinel2_start_index += 1;
        }

        while sentinel2_start_index + 1 < sentinel2_images.len()
                && sentinel2_images[sentinel2_start_index+1].geocode == 
                    modis_images[modis_index].geocode
                && sentinel2_images[sentinel2_start_index+1].timestamp <
                    modis_images[modis_index].timestamp {
            sentinel2_start_index += 1;
        }

        while sentinel2_images[sentinel2_end_index].geocode <
                sentinel2_images[sentinel2_start_index].geocode {
            //println!("{} {}", sentinel2_start_index, sentinel2_end_index);
            sentinel2_end_index += 1;
        }

        while sentinel2_end_index + 1 < sentinel2_images.len()
                && sentinel2_images[sentinel2_end_index+1].timestamp <
                modis_images[modis_index].timestamp - (15 * 86400){
            sentinel2_end_index += 1;
        }

        // not enough sentinel2 images -> move to next MODIS image
        if sentinel2_start_index - sentinel2_end_index < 3 {
            modis_index += 1;
            continue;
        }

        // TODO - process
        println!("TODO - process {} {} {}", sentinel2_end_index,
            sentinel2_start_index, modis_index);

        // get images
        let sentinel2_start_image =
            &sentinel2_images[sentinel2_start_index];
        let sentinel2_end_image = &sentinel2_images[sentinel2_end_index];
        let modis_image = &modis_images[modis_index];

        /*println!("  {} {}", sentinel2_end_image.geocode,
            sentinel2_end_image.timestamp);
        println!("  {} {}", sentinel2_start_image.geocode,
            sentinel2_start_image.timestamp);
        println!("  {} {}", modis_image.geocode,
            modis_image.timestamp);*/

        modis_index += 1;
    }

    // TODO - open channels
    //let (geohash_tx, geohash_rx) = crossbeam_channel::unbounded();
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

    // sort images by geohash and timestamp (ascending)
    images.sort_by(|a, b| {
        let geocode_cmp = a.geocode.partial_cmp(&b.geocode).unwrap();
        if geocode_cmp != Ordering::Equal{
            return geocode_cmp
        }
        a.timestamp.partial_cmp(&b.timestamp).unwrap()
    });
    Ok(images)
}
