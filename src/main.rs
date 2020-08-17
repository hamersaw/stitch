use structopt::StructOpt;
use st_image::prelude::Geocode;

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

    #[structopt(name="OUTPUT_FILE", help="image output file")]
    output_file: PathBuf,
}

fn main() {
    // parse command line options
    let opt = Opt::from_args();

    // determine geohashes in bounding box
    let geocode = Geocode::Geohash;
    let (longitude_interval, latitude_interval) =
        geocode.get_intervals(5);
    let windows = st_image::prelude::get_window_bounds(
        opt.min_longitude, opt.max_longitude,
        opt.min_latitude, opt.max_latitude,
        longitude_interval, latitude_interval);

    // TODO - tmp
    for (min_long, max_long, min_lat, max_lat) in windows.iter() {
        let geohash = geocode.get_code((min_long + max_long) / 2.0,
            (min_lat + max_lat) / 2.0, 5);

        println!("{:?}", geohash);
    }

    // TODO - locate available geohash images

    // TODO - stitch together image
    //  - download images
    //  - generate missing images
}
