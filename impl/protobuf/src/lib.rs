mod stitch {
    tonic::include_proto!("stitch");
}

pub use stitch::*;
pub use stitch::stitch_client::StitchClient;
pub use stitch::stitch_server::{Stitch, StitchServer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
