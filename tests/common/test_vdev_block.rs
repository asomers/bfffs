use arkfs::common::vdev::Vdev;
use arkfs::common::vdev_block::*;
use arkfs::sys::vdev_file::*;
use std::fs;
use tempdir::TempDir;
use tokio_file::File;
use tokio_core::reactor::Core;

macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

#[test]
fn test_open() {
    let len = 1 << 26;  // 64MB
    let mut l = t!(Core::new());
    let tempdir = t!(TempDir::new("test_open"));
    let filename = tempdir.path().join("vdev");
    let file = t!(fs::File::create(&filename));
    t!(file.set_len(len));
    let leaf = Box::new(VdevFile::open(filename, l.handle()));
    VdevBlock::open(leaf, l.handle());
}

#[test]
fn test_size() {
    let len = 1 << 26;  // 64MB
    let mut l = t!(Core::new());
    let tempdir = t!(TempDir::new("test_size"));
    let filename = tempdir.path().join("vdev");
    let file = t!(fs::File::create(&filename));
    t!(file.set_len(len));
    let leaf = Box::new(VdevFile::open(filename, l.handle()));
    let vdev = VdevBlock::open(leaf, l.handle());
    assert_eq!(vdev.size(), 16384);
}


