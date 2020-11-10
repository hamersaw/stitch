use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crossbeam_channel::{Receiver, Sender};
use protobuf::{Filter, Image, ImageListRequest, ImageManagementClient};
use structopt::StructOpt;
use tonic::Request;

use std::cmp::Ordering;
use std::error::Error;
use std::io::{Read, Write};
use std::net::{IpAddr, TcpStream};
use std::time::Instant;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name="yogi")]
struct Opt {
    #[structopt(short, long, help="stip album", default_value="test")]
    album: String,

    #[structopt(short, long, help="size of batches", default_value="1")]
    batch_size: usize,

    #[structopt(short, long,
        help="stip node ip address", default_value="127.0.0.1")]
    ip_address: IpAddr,

    #[structopt(short, long,
        help="stip node rpc port", default_value="15606")]
    port: u16,

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

    let sentinel2_images = match get_images(&opt.album, sentinel2_filter,
            &format!("{}:{}", &opt.ip_address, opt.port)) {
        Ok(images) => images,
        Err(e) => panic!("failed to get sentinel-2: {}", e),
    };

    let sentinel2_images: Vec<Image> = sentinel2_images
        .into_iter().filter(|x| x.files.len() == 4).collect();

    // get all MODIS images
    let modis_filter = Filter {
        end_timestamp: opt.timestamp_end,
        geocode: None,
        max_cloud_coverage: None,
        min_pixel_coverage: None,
        platform: Some("MODIS".to_string()),
        recurse: false,
        source: None,
        start_timestamp: opt.timestamp_start,
    };

    let modis_images = match get_images(&opt.album, modis_filter,
            &format!("{}:{}", &opt.ip_address, opt.port)) {
        Ok(images) => images,
        Err(e) => panic!("failed to get modis: {}", e),
    };

    let modis_images: Vec<Image> = modis_images
        .into_iter().filter(|x| x.files.len() == 2
            && x.geocode.len() == 5).collect();

    // open channels
    let (tx, rx): (Sender<(Vec<Image>, Image)>, Receiver<(Vec<Image>, Image)>) = 
        crossbeam_channel::unbounded();

    // start worker threads
    let mut join_handles = Vec::new();
    for _ in 0..opt.thread_count {
        let rx = rx.clone();
        let opt = opt.clone();

        let join_handle = std::thread::spawn(move || {
            let mut batch = Vec::new();
            for datum in rx.iter() {
                batch.push(datum);

                if batch.len() == opt.batch_size {
                    if let Err(e) = process(&batch, &opt) {
                        println!("batch process failed: {}", e);
                    }

                    batch.clear();
                }
            }

            if batch.len() != 0 {
                if let Err(e) = process(&batch, &opt) {
                    println!("batch process failed: {}", e);
                }
            }
        });

        join_handles.push(join_handle);
    }

    // process SATnet images
    let (mut sentinel2_start_index, mut sentinel2_end_index) = (0, 0);
    let mut modis_index = 0;

    let instant = Instant::now();
    let mut count = 0;
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
            sentinel2_end_index += 1;
        }
        while sentinel2_end_index < sentinel2_start_index
                && sentinel2_end_index + 1 < sentinel2_images.len()
                && sentinel2_images[sentinel2_end_index+1].timestamp <
                    modis_images[modis_index].timestamp - (15 * 86400){
            sentinel2_end_index += 1;
        }
        
        // check if geocodes are equal
        if sentinel2_images[sentinel2_start_index].geocode
                    != modis_images[modis_index].geocode
                || sentinel2_images[sentinel2_start_index].timestamp
                    > modis_images[modis_index].timestamp {
            modis_index += 1;
            continue;
        }

        // not enough sentinel2 images -> move to next MODIS image
        if sentinel2_start_index - sentinel2_end_index < 3 {
            modis_index += 1;
            continue;
        }

        // get images
        let mut sentinel2_vec = Vec::new();
        while sentinel2_vec.len() < 3 {
            let image = &sentinel2_images[sentinel2_start_index
                - sentinel2_vec.len()];
            sentinel2_vec.push(image.clone());
        }

        let modis_image = &modis_images[modis_index];

        // send images down channel
        if let Err(e) = tx.send((sentinel2_vec, modis_image.clone())) {
            panic!("failed to send geohash: {}", e);
        }

        count += 1;
        modis_index += 1;
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

fn process(batch: &Vec<(Vec<Image>, Image)>,
        opt: &Opt) -> Result<(), Box<dyn Error>> {
    // connect to stitchd service
    let addr = format!("{}:12289", opt.ip_address);
    let mut stream = TcpStream::connect(&addr)?;

    let instant = Instant::now();

    // write batch metadata
    stream.write_u8(batch.len() as u8)?;
    for (sentinel2_images, modis_image) in batch.iter() {
        // write geohash and timestamp
        write_string(&modis_image.geocode, &mut stream)?;
        stream.write_i64::<BigEndian>(modis_image.timestamp)?;

        // write paths
        stream.write_u8(sentinel2_images.len() as u8)?;
        for image in sentinel2_images.iter() {
            write_string(&image.files[3].path, &mut stream)?;
        }

        write_string(&modis_image.files[1].path, &mut stream)?;
    }

    // check for failure
    if stream.read_u8()? != 0 {
        let error_message = read_string(&mut stream)?;
        return Err(error_message.into())
    }

    // read datasets
    for _ in 0..batch.len() {
        let _dataset = st_image::serialize::read(&mut stream)?;
    }

    let duration = instant.elapsed();
    println!("processed batch (size {}) in {}.{}", batch.len(), 
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
