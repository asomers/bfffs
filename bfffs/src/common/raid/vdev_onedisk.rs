// vim: tw=80

use async_trait::async_trait;
use crate::{
    common::{
        *,
        label::*,
        vdev::*,
    }
};
use futures::future;
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    path::Path
};
use super::{
    vdev_raid_api::*,
};

#[cfg(test)]
use crate::common::vdev_block::MockVdevBlock as VdevBlock;
#[cfg(not(test))]
use crate::common::vdev_block::VdevBlock;

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    pub uuid:   Uuid,
    /// UUID of the wrapped `VdevFile`
    pub child:  Uuid
}

/// `VdevOneDisk`: RAID-level Virtual Device for single-disk clusters
///
/// This Vdev adapts a Cluster to a single disk, without providing any
/// redundancy.
pub struct VdevOneDisk {
    /// Underlying block device.
    blockdev: VdevBlock,

    uuid: Uuid,
}

impl VdevOneDisk {
    /// Create a new VdevOneDisk from an unused file or device
    ///
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `path`:               Pathnames of file or device
    // Hide from docs.  The public API should just be raid::create, but this
    // function technically needs to be public for testing purposes.
    #[doc(hidden)]
    pub fn create<P>(lbas_per_zone: Option<NonZeroU64>, path: P) -> Self
        where P: AsRef<Path> + 'static
    {
        let uuid = Uuid::new_v4();
        let blockdev = VdevBlock::create(path, lbas_per_zone).unwrap();
        VdevOneDisk{uuid, blockdev}
    }

    /// Open an existing `VdevOneDisk`
    ///
    /// # Parameters
    ///
    /// * `label`:      The `VdevOneDisk`'s label
    /// * `blockdevs`:  A map containing a single `VdevBlock`, indexed by UUID
    pub(super) fn open(label: Label, blockdevs: BTreeMap<Uuid, VdevBlock>)
        -> Self
    {
        assert_eq!(blockdevs.len(), 1);
        let blockdev = blockdevs.into_iter().next().unwrap().1;
        VdevOneDisk{uuid: label.uuid, blockdev}
    }

}

impl Vdev for VdevOneDisk {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.blockdev.lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> u32 {
        self.blockdev.optimum_queue_depth()
    }

    fn size(&self) -> LbaT {
        self.blockdev.size()
    }

    fn sync_all(&self) -> BoxVdevFut {
        self.blockdev.sync_all()
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.blockdev.zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.blockdev.zones()
    }
}

#[async_trait]
impl VdevRaidApi for VdevOneDisk {
    async fn erase_zone(&self, zone: ZoneT) -> Result<(), Error> {
        let limits = self.blockdev.zone_limits(zone);
        self.blockdev.erase_zone(limits.0, limits.1 - 1).await
    }

    async fn finish_zone(&self, zone: ZoneT) -> Result<(), Error> {
        let limits = self.blockdev.zone_limits(zone);
        self.blockdev.finish_zone(limits.0, limits.1 - 1).await
    }

    fn flush_zone(&self, _zone: ZoneT) -> (LbaT, BoxVdevFut) {
        (0, Box::pin(future::ok(())))
    }

    async fn open_zone(&self, zone: ZoneT) -> Result<(), Error> {
        let limits = self.blockdev.zone_limits(zone);
        self.blockdev.open_zone(limits.0).await
    }

    async fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Result<(), Error> {
        self.blockdev.read_at(buf, lba).await
    }

    async fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Result<(), Error>
    {
        self.blockdev.read_spacemap(buf, idx).await
    }

    async fn reopen_zone(&self, _zone: ZoneT, _allocated: LbaT)
        -> Result<(), Error>
    {
        Ok(())
    }

    async fn write_at(&self, buf: IoVec, _zone: ZoneT, lba: LbaT)
        -> Result<(), Error>
    {
        // Pad up to a whole number of LBAs.  Upper layers don't do this because
        // VdevRaidApi doesn't have a writev_at method.  But VdevBlock does, so
        // the raid layer is the most efficient place to pad.
        let partial = buf.len() % BYTES_PER_LBA;
        if partial == 0 {
            self.blockdev.write_at(buf, lba).await
        } else {
            let remainder = BYTES_PER_LBA - partial;
            let zbuf = ZERO_REGION.try_const().unwrap().slice_to(remainder);
            let sglist = vec![buf, zbuf];
            self.blockdev.writev_at(sglist, lba).await
        }
    }

    async fn write_label(&self, mut labeller: LabelWriter) -> Result<(), Error>
    {
        let onedisk_label = Label {
            uuid: self.uuid,
            child: self.blockdev.uuid()
        };
        let label = super::Label::OneDisk(onedisk_label);
        labeller.serialize(&label).unwrap();
        self.blockdev.write_label(labeller).await
    }

    // Allow &Vec arguments so we can clone them.
    // TODO: pass by value instead of reference, to eliminate the clone
    #[allow(clippy::ptr_arg)]
    async fn write_spacemap(&self, sglist: &SGList, idx: u32, block: LbaT)
        -> Result<(), Error>
        //-> BoxVdevFut
    {
        self.blockdev.write_spacemap(sglist.clone(), idx, block).await
    }
}

#[cfg(test)]
mod t {

use super::*;
// pet kcov
#[test]
fn debug() {
    let label = Label {
        uuid: Uuid::new_v4(),
        child: Uuid::new_v4()
    };
    format!("{:?}", label);
}

}
