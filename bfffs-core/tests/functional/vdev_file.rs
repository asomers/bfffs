// vim: tw=80

mod basic {
    use bfffs_core::{
        BYTES_PER_LBA,
        vdev::*,
        vdev_file::*
    };
    use divbuf::DivBufShared;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use std::{
        fs,
        io::{Read, Seek, SeekFrom, Write},
        mem,
        ops::Deref,
        path::PathBuf,
    };
    use tempfile::{Builder, TempDir};

    struct Harness {
        vdev: VdevFile<'static>,
        file: fs::File,
        path: PathBuf,
        _tempdir: TempDir
    }

    #[fixture]
    fn harness() -> Harness {
        let len = 1 << 26;  // 64MB
        let tempdir = Builder::new()
            .prefix("test_vdev_file_basic")
            .tempdir()
            .unwrap();
        let filename = tempdir.path().join("vdev");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename)
            .unwrap();
        file.set_len(len).unwrap();
        let pb = filename.to_path_buf();
        let vdev = VdevFile::new(&file).unwrap();
        // Safe because vdev will drop before _file
        let vdev = unsafe{ mem::transmute::<VdevFile, VdevFile<'static>>(vdev)};
        Harness{file, vdev, path: pb, _tempdir: tempdir}
    }

    /// erase_zone on a plain file should succeed.  If fspacectl is supported,
    /// that region of the file should be zeroed.  Otherwise, nothing should
    /// happen.
    #[rstest]
    #[tokio::test]
    async fn erase_zone(harness: Harness) {
        let mut f = fs::File::open(harness.path).unwrap();
        let mut rbuf = vec![0u8; 4096];

        // First, write a record
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            harness.vdev.write_at(wbuf.clone(), 10).await
            .unwrap();
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, wbuf.deref());
        }

        let zl = harness.vdev.zone_limits(0);
        harness.vdev.erase_zone(0, zl.1 - 1).await.unwrap();

        // verify that it got erased, if fspacectl is supported here
        #[cfg(have_fspacectl)]
        {
            let expected = vec![0u8; 4096];
            f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, expected);
        }
    }

    /// Erasing a zone twice in the life of a vdev_file takes a different code
    /// path.  Exercise it, too.
    #[cfg(have_fspacectl)]
    #[rstest]
    #[tokio::test]
    async fn erase_zone_twice(harness: Harness) {
        let mut f = fs::File::open(harness.path).unwrap();
        let mut rbuf = vec![0u8; 4096];

        // First, write some data to two zones.
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            for zone in 0..2 {
                let zl = harness.vdev.zone_limits(zone);
                let wbuf = dbs.try_const().unwrap();
                harness.vdev.write_at(wbuf.clone(), zl.0).await
                .unwrap();
            }
        }

        // Now erase both zones.
        let zl0 = harness.vdev.zone_limits(0);
        let zl1 = harness.vdev.zone_limits(1);
        harness.vdev.erase_zone(0, zl0.1 - 1).await.unwrap();
        harness.vdev.erase_zone(1, zl1.1 - 1).await.unwrap();

        // verify that they got erased.
        let expected = vec![0u8; 4096];
        for zone in 0..2 {
            let zl = harness.vdev.zone_limits(zone);
            f.seek(SeekFrom::Start(zl.0 * BYTES_PER_LBA as u64)).unwrap();
            f.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, expected);
        }
    }



    #[rstest]
    fn lba2zone(harness: Harness) {
        assert_eq!(harness.vdev.lba2zone(0), None);
        assert_eq!(harness.vdev.lba2zone(9), None);
        assert_eq!(harness.vdev.lba2zone(10), Some(0));
        assert_eq!(harness.vdev.lba2zone((1 << 16) - 1), Some(0));
        assert_eq!(harness.vdev.lba2zone(1 << 16), Some(1));
    }

    #[rstest]
    fn size(harness: Harness) {
        assert_eq!(harness.vdev.size(), 16_384);
    }

    #[rstest]
    fn zone_limits(harness: Harness) {
        assert_eq!(harness.vdev.zone_limits(0), (10, 1 << 16));
        assert_eq!(harness.vdev.zone_limits(1), (1 << 16, 2 << 16));
    }

    #[rstest]
    fn zones(harness: Harness) {
        assert_eq!(harness.vdev.zones(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn read_at(mut harness: Harness) {
        let wbuf = vec![42u8; 4096];
        // Write some test data, but skip the labels
        harness.file.seek(SeekFrom::Start(10 * 4096)).unwrap();
        harness.file.write_all(wbuf.as_slice()).unwrap();

        // Run the test
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbs.try_mut().unwrap();
        //let vdev = VdevFile::create(path, None).unwrap();
        harness.vdev.read_at(rbuf, 10).await.unwrap();
        assert_eq!(&dbs.try_const().unwrap()[..], &wbuf[..]);
    }

    #[rstest]
    #[tokio::test]
    async fn readv_at(mut harness: Harness) {
        // Create the initial file
        let wbuf = (0..8192)
            .map(|i| (i / 16) as u8)
            .collect::<Vec<_>>();
        // Write some test data, but skip the labels
        harness.file.seek(SeekFrom::Start(10 * BYTES_PER_LBA as u64)).unwrap();
        harness.file.write_all(wbuf.as_slice()).unwrap();

        // Run the test
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let rbufs = vec![rbuf0, rbuf1];
        
        harness.vdev.readv_at(rbufs, 10).await.unwrap();
        assert_eq!(&dbs0.try_const().unwrap()[..], &wbuf[..4096]);
        assert_eq!(&dbs1.try_const().unwrap()[..], &wbuf[4096..]);
    }

    #[rstest]
    #[tokio::test]
    async fn write_at(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rbuf = vec![0u8; 4096];
        harness.vdev.write_at(wbuf.clone(), 10).await.unwrap();
        let mut f = fs::File::open(harness.path).unwrap();
        f.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
        f.read_exact(&mut rbuf).unwrap();
        assert_eq!(rbuf, wbuf.deref());
    }

    #[should_panic(expected = "Attempted to overwrite the labels!")]
    #[rstest]
    #[tokio::test]
    async fn write_at_overwrite_label(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        harness.vdev.write_at(wbuf, 0).await.unwrap();
    }

    #[should_panic(expected = "Don't overwrite the labels!")]
    #[rstest]
    #[tokio::test]
    async fn writev_at_overwrite_label(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        harness.vdev.writev_at(vec![wbuf], 0).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn write_at_lba(harness: Harness) {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let mut rbuf = vec![0u8; 4096];
        harness.vdev.write_at(wbuf.clone(), 11).await.unwrap();
        let mut f = fs::File::open(harness.path).unwrap();
        f.seek(SeekFrom::Start(11 * 4096)).unwrap();
        f.read_exact(&mut rbuf).unwrap();
        assert_eq!(rbuf, wbuf.deref());
    }

    #[rstest]
    #[tokio::test]
    async fn writev_at(harness: Harness) {
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![1u8; 4096]);
        let wbuf0 = dbs0.try_const().unwrap();
        let wbuf1 = dbs1.try_const().unwrap();
        let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
        let mut rbuf = vec![0u8; 8192];
        harness.vdev.writev_at(wbufs, 10).await.unwrap();
        let mut f = fs::File::open(harness.path).unwrap();
        f.seek(SeekFrom::Start(10 * BYTES_PER_LBA as u64)).unwrap();
        f.read_exact(&mut rbuf).unwrap();
        assert_eq!(&rbuf[0..4096], wbuf0.deref());
        assert_eq!(&rbuf[4096..8192], wbuf1.deref());
    }

    #[rstest]
    #[tokio::test]
    async fn read_after_write(harness: Harness) {
        let vd = harness.vdev;
        let dbsw = DivBufShared::from(vec![1u8; 4096]);
        let wbuf = dbsw.try_const().unwrap();
        let dbsr = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbsr.try_mut().unwrap();
        vd.write_at(wbuf.clone(), 10).await.unwrap();
        vd.read_at(rbuf, 10).await.unwrap();
        assert_eq!(wbuf, dbsr.try_const().unwrap());
    }
}

/// Tests that use a device file
mod dev {
    use crate::require_root;
    use bfffs_core::{
        vdev::Vdev,
        vdev_file::*
    };
    use divbuf::DivBufShared;
    use function_name::named;
    use mdconfig::Md;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{self, Read, Seek, SeekFrom},
        mem,
        ops::Deref,
    };

    struct Harness {
        vdev: VdevFile<'static>,
        file: fs::File,
        _md: Md,
    }

    fn harness() -> io::Result<Harness> {
        let md = mdconfig::Builder::swap(64 << 20)
            .create()
            .unwrap();
        let zones_per_lba = 8192;  // 32 MB zones
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(md.path())
            .unwrap();
        let mut vdev = VdevFile::new(&file)?;
        vdev.set(vdev.size(), zones_per_lba).unwrap();
        // Safe because vdev will drop before _file
        let vdev = unsafe{ mem::transmute::<VdevFile, VdevFile<'static>>(vdev)};
        Ok(Harness{vdev, file, _md: md})
    }

    /// For devices that support TRIM, erase_zone should do it.
    #[named]
    #[tokio::test]
    async fn erase_zone() {
        require_root!();

        let mut h = harness().unwrap();
        let mut rbuf = vec![0u8; 4096];

        // First, write a record
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            h.vdev.write_at(wbuf.clone(), 10).await.unwrap();
            h.file.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            h.file.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, wbuf.deref());
        }

        // Actually erase the zone
        h.vdev.erase_zone(0, h.vdev.zone_limits(0).1 - 1).await.unwrap();

        // verify that it got erased
        {
            let expected = vec![0u8; 4096];
            h.file.seek(SeekFrom::Start(10 * 4096)).unwrap();   // Skip the label
            h.file.read_exact(&mut rbuf).unwrap();
            assert_eq!(rbuf, expected);
        }
    }
}

/// Tests that use a simulated SMR device
mod zoned {
    use crate::require_gzoned;
    use bfffs_core::{
        vdev::Vdev,
        vdev_file::*
    };
    use divbuf::DivBufShared;
    use freebsd_zonecmd::{
        ReportOptions,
        ZonedDevice,
        ZoneType,
        gzoned::{self, Gzoned}
    };
    use function_name::named;
    use pretty_assertions::assert_eq;
    use std::{
        fs,
        io::{self, ErrorKind},
        mem,
    };

    struct Harness {
        vdev: VdevFile<'static>,
        fd: fs::File,
        _zonedev: Gzoned,
    }

    fn harness() -> io::Result<Harness> {
        let zonedev = gzoned::Builder::default()
            .zonesize(4096)         // 16 MB zones
            .sectors(4096 * 4)
            .conventional_zones(0..=1)
            .build()
            .unwrap();
        let fd = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(zonedev.path())
            .unwrap();
        let vdev = VdevFile::new(&fd)?;
        // Safe because vdev will drop before fd
        let vdev = unsafe{ mem::transmute::<VdevFile, VdevFile<'static>>(vdev)};
        Ok(Harness{vdev, fd, _zonedev: zonedev})
    }

    // TODO:
    // [ ] Fail to open an existing zoned device, if its zone size is different
    //     than how it was originally formatted.
    // [✓] erase_zone should use RWP in sequential zones
    // [ ] erase_zone should be a NOP in conventional zones
    // [ ] vdev_file will call finish_zone when the zone fills up
    // [ ] Fail to create a vdev_file if the spacemap cannot fit within the
    //     sequential zones.
    // [✓] The set method should be unable to change zone count

    /// erase_zone should use RWP on such devices
    #[named]
    #[tokio::test]
    async fn erase_zone() {
        require_gzoned!();

        let h = harness().unwrap();

        let zid = 2;
        let zl2 = h.vdev.zone_limits(zid);

        // First, write a record
        {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            h.vdev.write_at(wbuf.clone(), zl2.0).await.unwrap();
        }

        {
            let mut rz = h.fd.report_zones(ReportOptions::All, zl2.0)
                .unwrap();
            let first = rz.next().unwrap().unwrap();
            assert_eq!(first.zone_type, ZoneType::SeqRequired,
                       "This test requires a sequential zone");
        }

        // Actually erase the zone
        h.vdev.erase_zone(zl2.0, zl2.1 - 1).await.unwrap();

        // verify that it got erased.  The old data may or may not be readable,
        // depending on the zoned device's implementaiton.
        {
            let mut rz = h.fd.report_zones(ReportOptions::All, zl2.0)
                .unwrap();
            let first = rz.next().unwrap().unwrap();
            assert_eq!(first.write_pointer_lba, Some(zl2.0));
        }
    }

    #[named]
    #[tokio::test]
    async fn lba2zone() {
        require_gzoned!();
        let h = harness().unwrap();

        assert_eq!(h.vdev.lba2zone(0), None);
        assert_eq!(h.vdev.lba2zone(9), None);
        assert_eq!(h.vdev.lba2zone(10), Some(0));
        assert_eq!(h.vdev.lba2zone((1 << 12) - 1), Some(0));
        assert_eq!(h.vdev.lba2zone(1 << 12), Some(1));
    }

    #[named]
    #[tokio::test]
    async fn size() {
        require_gzoned!();
        let h = harness().unwrap();

        assert_eq!(h.vdev.size(), 12_288);
    }

    #[named]
    #[tokio::test]
    async fn zone_limits() {
        require_gzoned!();
        let h = harness().unwrap();

        assert_eq!(h.vdev.zone_limits(0), (10, 1 << 12));
        assert_eq!(h.vdev.zone_limits(1), (1 << 12, 2 << 12));
        assert_eq!(h.vdev.zone_limits(2), (2 << 12, 3 << 12));
        assert_eq!(h.vdev.zone_limits(3), (3 << 12, 4 << 12));
    }

    #[named]
    #[tokio::test]
    async fn zones() {
        require_gzoned!();
        let h = harness().unwrap();

        assert_eq!(h.vdev.zones(), 3);
    }

    #[named]
    #[tokio::test]
    async fn zone_size_is_fixed() {
        require_gzoned!();
        let mut h = harness().unwrap();

        let e = h.vdev.set(h.vdev.size(), 1<<18).unwrap_err();
        assert_eq!(ErrorKind::Unsupported, e.kind());
    }
}
