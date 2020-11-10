use protobuf::{Filter, Image, ImageListRequest, ImageManagementClient};
use tonic::Request;

use std::cmp::Ordering;
use std::error::Error;

#[tokio::main]
pub async fn get_images(album: &str, filter: Filter, rpc_address: &str)
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
