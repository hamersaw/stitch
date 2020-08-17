use protobuf::Stitch;

pub struct StitchImpl {
}

impl StitchImpl {
    pub fn new() -> StitchImpl {
        StitchImpl {}
    }
}

#[tonic::async_trait]
impl Stitch for StitchImpl {
    /*async fn broadcast(&self, request: Request<AlbumBroadcastRequest>)
            -> Result<Response<AlbumBroadcastReply>, Status> {*/
}
