use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::raster::Dataset;
use protobuf::{Image, Node};

use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;

pub enum Tile {
    Stip(Node, Image),
    Stitch(Node, Vec<Image>, Image),
}

impl Tile {
    pub fn download(&self) -> Result<Dataset, Box<dyn Error>> {
        match self {
            Tile::Stip(node, image) => {
                // connect to stip transfer service
                let mut stream = TcpStream::connect(&node.xfer_addr)?;

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
                let dataset = st_image::serialize::read(&mut stream)?;
                return Ok(dataset);
            },
            Tile::Stitch(node, sentinel2_images, modis_image) => {
                // connect to stitchd service
                let addr_fields: Vec<&str> =
                    node.xfer_addr.split(":").collect();
                let addr = format!("{}:12289", addr_fields[0]);

                let mut stream = TcpStream::connect(&addr)?;

                // write geohash and timestamp
                write_string(&modis_image.geocode, &mut stream)?;
                stream.write_i64::<BigEndian>(modis_image.timestamp)?;

                // write paths
                stream.write_u8(sentinel2_images.len() as u8)?;
                for image in sentinel2_images.iter() {
                    write_string(&image.files[3].path, &mut stream)?;
                }

                write_string(&modis_image.files[1].path, &mut stream)?;

                // check for failure
                if stream.read_u8()? != 0 {
                    let error_message = read_string(&mut stream)?;
                    return Err(error_message.into())
                }

                // read dataset
                let dataset = st_image::serialize::read(&mut stream)?;
                return Ok(dataset);
            },
        }
    }
}

pub fn read_string<T: Read>(reader: &mut T)
        -> Result<String, Box<dyn Error>> {
    let len = reader.read_u8()?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

pub fn write_string<T: Write>(value: &str, writer: &mut T)
        -> Result<(), Box<dyn Error>> {
    writer.write_u8(value.len() as u8)?;
    writer.write(value.as_bytes())?;
    Ok(())
}
