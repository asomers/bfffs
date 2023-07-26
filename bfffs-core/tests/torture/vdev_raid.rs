use std::{
    fs,
    mem,
    num::NonZeroU64,
    path::PathBuf,
    sync::Arc
};

use std::os::unix::fs::FileExt;
use divbuf::DivBufShared;
use pretty_assertions::assert_eq;
use rand::{
    Rng,
    RngCore,
    SeedableRng,
    thread_rng
};
use rstest::rstest;
use tempfile::{Builder, TempDir};
use rand_xorshift::XorShiftRng;

use bfffs_core::{
    BYTES_PER_LBA,
    LbaT,
    mirror::Mirror,
    raid::{self, VdevRaidApi},
};

struct Harness {
    vdev: Arc<dyn VdevRaidApi>,
    _tempdir: TempDir,
    k: i16,
    f: i16,
    chunksize: LbaT,
}

async fn harness(n: i16, k: i16, f: i16, chunksize: LbaT) -> Harness {
    let len = 1 << 30;  // 1 GB
    let tempdir = Builder::new()
        .prefix("test_vdev_raid_torture")
        .tempdir()
        .unwrap();
    let paths = (0..n).map(|i| {
        let mut fname = PathBuf::from(tempdir.path());
        fname.push(format!("vdev.{i}"));
        let file = fs::File::create(&fname).unwrap();
        file.set_len(len).unwrap();
        fname
    }).collect::<Vec<_>>();
    let mirrors = paths.iter().map(|fname|
        Mirror::create(&[fname], None).unwrap()
    ).collect::<Vec<_>>();
    let cs = NonZeroU64::new(chunksize);
    let vdev = raid::create(cs, k, f, mirrors);
    Harness{vdev, _tempdir: tempdir, k, f, chunksize}
}

/// Create a buffer with deterministic contents corresponding to the given file
/// location.
fn mkbuf(offs: LbaT, len: usize) -> Vec<u8> {
    const Z: usize = mem::size_of::<LbaT>();
    (0..len).map(|i| {
        let bofs = offs as usize * BYTES_PER_LBA + i - i % Z;
        let bshift = 8 * (Z - 1 - i % Z);
        ((bofs >> bshift) & 0xFF) as u8
    }).collect::<Vec<_>>()
}

/// Write and read data to a raid device using a random pattern, and verify
/// integrity.
#[rstest]
// Null RAID
#[case(harness(1, 1, 0, 1), None)]
// Stupid mirror
#[case(harness(2, 2, 1, 1), None)]
// Smallest possible PRIMES configuration
#[case(harness(3, 3, 1, 2), None)]
// Smallest PRIMES declustered configuration
#[case(harness(5, 4, 1, 2), None)]
// Smallest double-parity configuration
#[case(harness(5, 5, 2, 2), None)]
// Smallest non-ideal PRIME-S configuration
#[case(harness(7, 4, 1, 2), None)]
// Smallest triple-parity configuration
#[case(harness(7, 7, 3, 2), None)]
// Smallest quad-parity configuration
#[case(harness(11, 9, 4, 2), None)]
// Highly declustered configuration
#[case(harness(7, 3, 1, 2), None)]
#[awt]
#[tokio::test]
async fn random_io(
    #[case] #[future] h: Harness,
    #[case] seed: Option<[u8; 16]>,
) {
    let file_size: usize = ((2<<20) as f64 * crate::test_scale()) as usize;
    // A maximum write of 4 stripes should hit every special case in vdev_raid
    let max_write_lbas = 4 * h.chunksize * (h.k - h.f) as LbaT;

    let seed = seed.unwrap_or_else(|| {
        let mut seed = [0u8; 16];
        let mut seeder = thread_rng();
        seeder.fill_bytes(&mut seed);
        seed
    });
    println!("Using seed {:?}", &seed);
    // Use XorShiftRng because it's deterministic and seedable
    let mut rng = XorShiftRng::from_seed(seed);

    // Do all the writes first
    let mut nwritten = 0;
    let zone = 0;
    let zl = h.vdev.zone_limits(zone);
    h.vdev.open_zone(zone).await.unwrap();
    let mut ofs = zl.0;
    let xfile = std::fs::File::create("/tmp/xfile.bin").unwrap();
    while nwritten < file_size {
        let write_lbas: LbaT = rng.gen_range(1..=max_write_lbas);
        let write_bytes = write_lbas as usize * BYTES_PER_LBA;
        let dbs = DivBufShared::from(mkbuf(ofs, write_bytes));
        let wbuf = dbs.try_const().unwrap();
        assert!(ofs + write_lbas < zl.1, "This test is not yet zone-aware");
        xfile.write_at(&wbuf[..], ofs * BYTES_PER_LBA as u64).unwrap();
        h.vdev.write_at(wbuf, zone, ofs).await.unwrap();
        nwritten += write_bytes;
        ofs += write_lbas;
    }
    // Don't close the zone so we'll retain an open StripeBuffer.

    // Now read it back, with different offsets,and verify the contents.
    ofs = zl.0;
    let mut nread = 0;
    while nread < nwritten {
        let read_lbas: LbaT = rng.gen_range(1..=max_write_lbas);
        let read_bytes = (nwritten - nread).min(read_lbas as usize * BYTES_PER_LBA);
        let expect_buf = mkbuf(ofs, read_bytes);
        let dbs = DivBufShared::from(vec![0u8; read_bytes]);
        let rbuf = dbs.try_mut().unwrap();
        assert!(ofs + read_lbas < zl.1, "This test is not yet zone-aware");
        h.vdev.clone().read_at(rbuf, ofs).await.unwrap();
        assert_eq!(&dbs.try_const().unwrap()[..], &expect_buf[..]);
        nread += read_bytes;
        ofs += read_lbas;
    }
}

#[test]
fn mkbuf_test() {
    let v = mkbuf(0xdeadbeef7a7eb, 64);
    let expect = [
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x00,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x08,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x10,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x18,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x20,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x28,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x30,
        0xde, 0xad, 0xbe, 0xef, 0x7a, 0x7e, 0xb0, 0x38,
    ];
    assert_eq!(&v[..], &expect[..]);
}
