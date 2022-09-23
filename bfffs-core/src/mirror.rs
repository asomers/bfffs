// vim: tw=80
//! BFFFS Mirror layer
//!
//! This provides vdevs which slot between `raid` and `VdevBlock` and
//! provide mirror functionality.  That includes both permanent mirrors as well
//! as temporary mirrors, used for spares and replacements.

use std::{
    io,
    num::NonZeroU64,
    path::Path,
    sync::atomic::{AtomicU32, Ordering}
};

use futures::{
    TryFutureExt,
    TryStreamExt,
    stream::FuturesUnordered
};
use serde_derive::{Deserialize, Serialize};

use crate::{
    label::*,
    types::*,
    vdev::*,
};
#[cfg(test)] use mockall::mock;
#[cfg(test)]
use crate::vdev_block::MockVdevBlock as VdevBlock;
#[cfg(not(test))]
use crate::vdev_block::VdevBlock;


#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    pub uuid:           Uuid,
    pub children:       Vec<Uuid>
}

/// `Mirror`: Device mirroring, both permanent and temporary
///
/// This Vdev mirrors two or more children.  It is used for both permanent
/// mirrors and for children which are spared or being replaced.
pub struct Mirror {
    /// Underlying block devices.
    blockdevs: Box<[VdevBlock]>,

    /// Wrapping index of the next child to read from during read operations
    // To eliminate the need for atomic divisions, the index is allowed to wrap
    // at 2**32.
    next_read_idx: AtomicU32,

    /// Best number of queued commands for the whole `Mirror`
    // NB it might be different for reads than for writes
    optimum_queue_depth: u32,

    /// Size of the vdev in bytes.  It's the minimum of the childrens' sizes.
    size: LbaT,

    uuid: Uuid,
}

impl Mirror {
    /// Create a new Mirror from unused files or devices
    ///
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `paths`:              Slice of pathnames of files and/or devices
    pub fn create<P>(
        paths: &[P],
        lbas_per_zone: Option<NonZeroU64>
    ) -> io::Result<Self>
        where P: AsRef<Path>
    {
        let uuid = Uuid::new_v4();
        let mut blockdevs = Vec::with_capacity(paths.len());
        for path in paths {
            blockdevs.push(VdevBlock::create(path, lbas_per_zone)?);
        }
        Ok(Mirror::new(uuid, blockdevs.into_boxed_slice()))
    }

    /// Asynchronously erase a zone on a mirror
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let fut = self.blockdevs.iter().map(|blockdev| {
            blockdev.erase_zone(start, end)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    /// Asynchronously finish a zone on a mirror
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let fut = self.blockdevs.iter().map(|blockdev| {
            blockdev.finish_zone(start, end)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn new(uuid: Uuid, blockdevs: Box<[VdevBlock]>) -> Self
    {
        assert!(blockdevs.len() > 0, "Need at least one disk");

        let next_read_idx = AtomicU32::new(0);

        // NB: the optimum queue depth should actually be greater for healthy
        // reads than for writes.  This calculation computes it for writes.
        let optimum_queue_depth = blockdevs.iter()
        .map(VdevBlock::optimum_queue_depth)
        .min()
        .unwrap();

        let size = blockdevs.iter()
        .map(VdevBlock::size)
        .min()
        .unwrap();

        Self {
            uuid,
            next_read_idx,
            optimum_queue_depth,
            size,
            blockdevs
        }
    }

    /// Open an existing `VdevMirror` from its component devices
    ///
    /// # Parameters
    ///
    /// * `uuid`:       Uuid of the desired `Mirror`, if present.  If `None`,
    ///                 then it will not be verified.
    /// * `combined`:   All the children `VdevBlock`s with their label readers
    pub fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlock, LabelReader)>)
        -> (Self, LabelReader)
    {
        let mut label_pair = None;
        let children = combined.into_iter()
            .map(|(vdev_block, mut label_reader)| {
                let label: Label = label_reader.deserialize().unwrap();
                if let Some(u) = uuid {
                    assert_eq!(u, label.uuid, "Opening disk from wrong mirror");
                }
                if label_pair.is_none() {
                    label_pair = Some((label, label_reader));
                }
                vdev_block
            }).collect::<Vec<VdevBlock>>()
            .into_boxed_slice();
        let (label, reader) = label_pair.unwrap();
        (Mirror::new(label.uuid, children), reader)
    }

    pub fn open_zone(&self, start: LbaT) -> BoxVdevFut {
        let fut = self.blockdevs.iter().map(|blockdev| {
            blockdev.open_zone(start)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut
    {
        let idx = self.read_idx();
        let fut = self.blockdevs[idx].read_at(buf, lba)
        .map_ok(drop);
        Box::pin(fut)
    }

    /// Return the index of the next child to read from
    fn read_idx(&self) -> usize {
        self.next_read_idx.fetch_add(1, Ordering::Relaxed) as usize %
            self.blockdevs.len()
    }

    pub fn read_spacemap(&self, buf: IoVecMut, smidx: u32) -> BoxVdevFut
    {
        let ridx = self.read_idx();
        let fut = self.blockdevs[ridx].read_spacemap(buf, smidx)
        .map_ok(drop);
        Box::pin(fut)
    }

    #[tracing::instrument(skip(self, bufs))]
    pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut
    {
        let idx = self.read_idx();
        let fut = self.blockdevs[idx].readv_at(bufs, lba)
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.blockdevs.iter().map(|blockdev| {
            blockdev.write_at(buf.clone(), lba)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_label(&self, mut labeller: LabelWriter) -> BoxVdevFut
    {
        let children_uuids = self.blockdevs.iter().map(|bd| bd.uuid())
            .collect::<Vec<_>>();
        let label = Label {
            uuid: self.uuid,
            children: children_uuids
        };
        labeller.serialize(&label).unwrap();
        let fut = self.blockdevs.iter().map(|bd| {
           bd.write_label(labeller.clone())
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        ->  BoxVdevFut
    {
        let fut = self.blockdevs.iter().map(|blockdev| {
            blockdev.write_spacemap(sglist.clone(), idx, block)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.blockdevs.iter().map(|blockdev| {
            blockdev.writev_at(bufs.clone(), lba)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }
}

impl Vdev for Mirror {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.blockdevs[0].lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> u32 {
        self.optimum_queue_depth
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn sync_all(&self) -> BoxVdevFut {
        // TODO: handle errors on some devices
        let fut = self.blockdevs.iter()
        .map(VdevBlock::sync_all)
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.blockdevs[0].zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.blockdevs[0].zones()
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock! {
    pub Mirror {
        #[mockall::concretize]
        pub fn create<P>(paths: &[P], lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path>;
        pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlock, LabelReader)>)
            -> (Self, LabelReader);
        pub fn open_zone(&self, start: LbaT) -> BoxVdevFut;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
        pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut;
        pub fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;
        pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            ->  BoxVdevFut;
        pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut;
    }
    impl Vdev for Mirror {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> BoxVdevFut;
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {
    use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
    use divbuf::DivBufShared;
    use futures::{FutureExt, future};
    use mockall::predicate::*;
    use super::*;

    fn mock_vdev_block() -> VdevBlock {
        const ZL0: (LbaT, LbaT) = (3, 32);
        //const ZL1 = (32, 64);

        let mut bd = VdevBlock::default();
        bd.expect_uuid()
            .return_const(Uuid::new_v4());
        bd.expect_optimum_queue_depth()
            .return_const(10u32);
        bd.expect_size()
            .return_const(262_144u64);
        bd.expect_zone_limits()
            .with(eq(0))
            .return_const(ZL0);
        bd
    }

    mod erase_zone {
        use super::*;

        #[test]
        fn basic() {
            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_erase_zone()
                    .once()
                    .with(eq(3), eq(31))
                    .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.erase_zone(3, 31).now_or_never().unwrap().unwrap();
        }
    }
    mod finish_zone {
        use super::*;

        #[test]
        fn basic() {
            fn mock() -> VdevBlock {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_finish_zone()
                    .once()
                    .with(eq(3), eq(31))
                    .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                bd
            }
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.finish_zone(3, 31).now_or_never().unwrap().unwrap();
        }
    }

    mod open_zone {
        use super::*;

        #[test]
        fn basic() {
            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
        }
    }

    mod read_at {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let mock = || {
                let total_reads2 = total_reads.clone();
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_read_at()
                    .withf(|buf, lba|
                        buf.len() == 4096
                        && *lba == 3
                ).returning(move |_, _| {
                    total_reads2.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ok::<(), Error>(()))
                });
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.read_at(buf, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }

        /// Multiple reads should be distributed across all children
        #[test]
        fn distributes() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_read_at()
                    .times(3)
                    .withf(|buf, _lba| buf.len() == 4096)
                    .returning(|_, _| {
                        Box::pin(future::ok::<(), Error>(()))
                    });
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let bd2 = mock();
            let uuid = Uuid::new_v4();
            let mirror = Mirror::new(uuid, vec![bd0, bd1, bd2].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            for i in 3..12 {
                let buf = dbs.try_mut().unwrap();
                mirror.read_at(buf, i).now_or_never().unwrap().unwrap();
            }
        }
    }

    mod read_spacemap {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let mock = || {
                let total_reads2 = total_reads.clone();
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_read_spacemap()
                    .withf(|buf, idx|
                        buf.len() == 4096
                        && *idx == 1
                ).returning(move |_, _| {
                    total_reads2.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ok::<(), Error>(()))
                });
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.read_spacemap(buf, 1).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }
    }

    mod readv_at {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let sglist = vec![buf];
            let total_reads = Arc::new(AtomicU32::new(0));

            let mock = || {
                let total_reads2 = total_reads.clone();
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_readv_at()
                    .withf(|sglist, lba|
                        sglist.len() == 1
                        && sglist[0].len() == 4096
                        && *lba == 3
                ).returning(move |_, _| {
                    total_reads2.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ok::<(), Error>(()))
                });
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.readv_at(sglist, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }
    }

    mod write_at {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![1u8; 4096]);
            let buf = dbs.try_const().unwrap();

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_write_at()
                    .once()
                    .withf(|buf, lba|
                        buf.len() == 4096
                        && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.write_at(buf, 3).now_or_never().unwrap().unwrap();
        }
    }

    mod writev_at {
        use super::*;

        #[test]
        fn basic() {
            let dbs0 = DivBufShared::from(vec![1u8; 4096]);
            let dbs1 = DivBufShared::from(vec![2u8; 8192]);
            let buf0 = dbs0.try_const().unwrap();
            let buf1 = dbs1.try_const().unwrap();
            let sglist = vec![buf0, buf1];

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_writev_at()
                    .once()
                    .withf(|sglist, lba|
                        sglist.len() == 2
                        && sglist[0].len() == 4096
                        && sglist[1].len() == 8192
                        && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.writev_at(sglist, 3).now_or_never().unwrap().unwrap();
        }
    }

    mod write_label {
        use super::*;

        #[test]
        fn basic() {
            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_write_label()
                .once()
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            let labeller = LabelWriter::new(0);
            mirror.write_label(labeller).now_or_never().unwrap().unwrap();
        }
    }

    mod write_spacemap {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![1u8; 4096]);
            let buf = dbs.try_const().unwrap();
            let sgl = vec![buf];

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_write_spacemap()
                    .once()
                    .withf(|sglist, idx, lba|
                        sglist.len() == 1
                        && sglist[0].len() == 4096
                        && *idx == 1
                        && *lba == 2
                ).return_once(|_, _, _| Box::pin(future::ok::<(), Error>(())));
                bd
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
            mirror.write_spacemap(sgl, 1, 2).now_or_never().unwrap().unwrap();
        }
    }
}
// LCOV_EXCL_STOP
