// vim: tw=80

use futures::Future;
use nix;
use std::io;
use std::path::Path;
use tokio::reactor::Handle;
use tokio_file::File;

use common::*;
use common::vdev::*;
use common::vdev_leaf::*;


/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of ArkFS.  It works with both
/// regular files and device files
///
#[derive(Debug)]
pub struct VdevFile {
    file:   File,
    handle: Handle,
    size:   LbaT
}

impl SGVdev for VdevFile {
    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<SGListFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.readv_at(buf, off).unwrap().map(|r| {
            let mut v = 0;
            let mut sglist = SGList::new();
            for ar in r {
                v += ar.value.unwrap();
                sglist.push(ar.buf.into_bytes_mut().unwrap().freeze());
            }
            SGListResult{buf: sglist, value: v}
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }})
        )

    }

    fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<SGListFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.writev_at(&buf[..], off).unwrap().map(|r| {
            let mut v = 0;
            let mut sglist = SGList::new();
            for ar in r {
                v += ar.value.unwrap();
                sglist.push(ar.buf.into_bytes().unwrap());
            }
            SGListResult{buf: sglist, value: v}
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }})
        )
    }
}

impl Vdev for VdevFile {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        (lba / (VdevFile::LBAS_PER_ZONE as u64)) as ZoneT
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.read_at(buf, off).unwrap().map(|aio_result| {
            let value = aio_result.value.unwrap();
            let buf_ref = aio_result.into_buf_ref();
            IoVecResult {
                buf: buf_ref.into_bytes_mut().unwrap().freeze(),
                value: value
            }
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }
        }))
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        zone as u64 * VdevFile::LBAS_PER_ZONE
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<IoVecFut> {
        let off = lba as i64 * (dva::BYTES_PER_LBA as i64);
        Box::new(self.file.write_at(buf, off).unwrap().map(|aio_result| {
            let value = aio_result.value.unwrap();
            let buf_ref = aio_result.into_buf_ref();
            IoVecResult {
                buf: buf_ref.into_bytes().unwrap(),
                value: value
            }
        }).map_err(|e| {
            match e {
                nix::Error::Sys(x) => io::Error::from(x),
                _ => panic!("Unhandled error type")
            }})
        )
    }
}

impl VdevLeaf for VdevFile {
}

impl VdevFile {
    /// Size of a simulated zone
    const LBAS_PER_ZONE: LbaT = 1 << 19;  // 256 MB

    /// Open a file for use as a Vdev
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    /// * `h`       Handle to the Tokio reactor that will be used to service
    ///             this vdev.  
    pub fn open<P: AsRef<Path>>(path: P, h: Handle) -> Self {
        let f = File::open(path, h.clone()).unwrap();
        let size = f.metadata().unwrap().len() / dva::BYTES_PER_LBA as u64;
        VdevFile{file: f, handle: h, size: size}
    }
}
