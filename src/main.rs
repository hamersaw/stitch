use structopt::StructOpt;

use std::net::IpAddr;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
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
    let opt = Opt::from_args();
    println!("{:?}", opt);
}
