use byteorder::{ReadBytesExt, WriteBytesExt};
use gdal::raster::Dataset;
use protobuf::{Image, Node};

use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;

pub enum Tile {
    SATnet(Node, Vec<Image>, Image),
    Stip(Node, Image),
}

impl Tile {
    pub fn download(&self) -> Result<Dataset, Box<dyn Error>> {
        match self {
            Tile::SATnet(node, sentinel2_images, modis_image) => {
                unimplemented!();
            },
            Tile::Stip(node, image) => {
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
