use byteorder::{ReadBytesExt, WriteBytesExt};
use crossbeam_channel::{Receiver, Sender};
use protobuf::{Filter, Image};
use structopt::StructOpt;

use std::error::Error;
use std::io::{Read, Write};
use std::net::{IpAddr, TcpStream};
use std::time::Instant;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name="stip")]
struct Opt {
    #[structopt(short, long, help="stip album", default_value="test")]
    album: String,

    #[structopt(short, long,
        help="stip node ip address", default_value="127.0.0.1")]
    ip_address: IpAddr,

    #[structopt(short, long,
        help="stip node rpc port", default_value="15606")]
    port: u16,

    #[structopt(short="x", long,
        help="stip node xfer port", default_value="15616")]
    xfer_port: u16,

    #[structopt(short, long, help="thread count", default_value="4")]
    thread_count: u8,

    #[structopt(short="s", long, help="beginning timestamp")]
    timestamp_start: Option<i64>,

    #[structopt(short="e", long, help="ending timestamp")]
    timestamp_end: Option<i64>,
}

fn main() {
    // parse command line options
    let opt = Opt::from_args();

    // get all Sentinel-2 images
    let sentinel2_filter = Filter {
        end_timestamp: opt.timestamp_end,
        geocode: None,
        max_cloud_coverage: None,
        min_pixel_coverage: Some(1.0),
        platform: Some("Sentinel-2".to_string()),
        recurse: false,
        source: None,
        start_timestamp: opt.timestamp_start,
    };

    let sentinel2_images = match yogi::get_images(
            &opt.album, sentinel2_filter, 
            &format!("{}:{}", &opt.ip_address, opt.port)) {
        Ok(images) => images,
        Err(e) => panic!("failed to get sentinel-2: {}", e),
    };

    let sentinel2_images: Vec<Image> = sentinel2_images
        .into_iter().filter(|x| x.files.len() == 4).collect();

    // open channels
    let (tx, rx): (Sender<Image>, Receiver<Image>) = 
        crossbeam_channel::unbounded();

    // start worker threads
    let mut join_handles = Vec::new();
    for _ in 0..opt.thread_count {
        let rx = rx.clone();
        let opt = opt.clone();

        let join_handle = std::thread::spawn(move || {
            for image in rx.iter() {
                if let Err(e) = process(&image, &opt) {
                    println!("image process failed: {}", e);
                }
            }
        });

        join_handles.push(join_handle);
    }

    // process stip images
    let instant = Instant::now();
    let mut count = 0;
    for image in sentinel2_images {
        // send images down channel
        if let Err(e) = tx.send(image.clone()) {
            panic!("failed to send geohash: {}", e);
        }

        count += 1;
    }

    // join worker threads
    drop(tx);
    for join_handle in join_handles {
        if let Err(e) = join_handle.join() {
            panic!("failed to join worker: {:?}", e);
        }
    }

    let duration = instant.elapsed();
    println!("imputed {} image(s) in {}.{}", count,
        duration.as_secs(), duration.subsec_nanos());
}

fn process(image: &Image, opt: &Opt) -> Result<(), Box<dyn Error>> {
    // connect to stitchd service
    let addr = format!("{}:{}", opt.ip_address, opt.xfer_port);
    let mut stream = TcpStream::connect(&addr)?;

    let instant = Instant::now();

    // send readop
    stream.write_u8(0)?;

    // send path
    write_string(&image.files[3].path, &mut stream)?;

    // send subgeocode indicator
    stream.write_u8(0)?;

    // check for failure
    if stream.read_u8()? != 0 {
        let error_message = read_string(&mut stream)?;
        return Err(error_message.into())
    }
    
    // read dataset
    let _ = st_image::serialize::read(&mut stream)?;

    let duration = instant.elapsed();
    println!("processed image in {}.{}",
        duration.as_secs(), duration.subsec_nanos());

    Ok(())
}

fn read_string<T: Read>(reader: &mut T)
        -> Result<String, Box<dyn Error>> {
    let len = reader.read_u8()?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn write_string<T: Write>(value: &str, writer: &mut T)
        -> Result<(), Box<dyn Error>> {
    writer.write_u8(value.len() as u8)?;
    writer.write(value.as_bytes())?;
    Ok(())
}
