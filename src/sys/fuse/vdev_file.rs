// vim: tw=80

use std::io;
use std::path::Path;
use tokio_core::reactor::Handle;
use tokio_file::{AioFut, File};

use common::*;
use common::zoned_device::*;
use common::vdev_leaf::*;

/// VdevFile: File-backed implementation of VdevBlock
///
/// This is used by the FUSE implementation of ArkFS.  It works with both
/// regular files and device files
///
pub struct VdevFile {
    file:   File,
    handle: Handle,
    size:   LbaT
}

impl ZonedDevice for VdevFile {
    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        (lba / (VdevFile::LBAS_PER_ZONE as u64)) as ZoneT
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        zone as u64 * VdevFile::LBAS_PER_ZONE
    }

    fn size(&self) -> LbaT {
        self.size
    }
}

impl VdevLeaf for VdevFile {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn read_at(&self, buf: IoVec, lba: LbaT) -> io::Result<AioFut<isize>> {
        self.file.read_at(buf, lba as i64 * (dva::BYTES_PER_LBA as i64))
    }

    fn readv_at(&self, buf: SGList, lba: LbaT) -> io::Result<AioFut<isize>> {
        self.file.readv_at(&buf, lba as i64 * (dva::BYTES_PER_LBA as i64))
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> io::Result<AioFut<isize>> {
        self.file.write_at(buf, lba as i64 * (dva::BYTES_PER_LBA as i64))
    }

    fn writev_at(&self, buf: SGList, lba: LbaT) -> io::Result<AioFut<isize>> {
        self.file.writev_at(&buf, lba as i64 * (dva::BYTES_PER_LBA as i64))
    }
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

