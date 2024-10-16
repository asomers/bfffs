// vim: tw=80
use crate::{
    cache::{self, Cache, Cacheable, CacheRef, Key},
    dml::*,
    label::*,
    pool::ClosedZone,
    types::*,
    util::*,
    vdev::*,
    writeback::Credit
};
use divbuf::DivBufShared;
use futures::{Future, FutureExt, TryFutureExt, future};
//use futures::{Future, FutureExt, TryFutureExt, channel::oneshot, future};
use metrohash::MetroHash64;
#[cfg(test)] use mockall::mock;
use std::{
    borrow,
    //collections::BTreeMap,
    hash::Hasher,
    iter,
    mem,
    pin::Pin,
    sync::{Arc, Mutex}
};
use super::DRP;
use tracing::instrument;
use tracing_futures::Instrument;

#[cfg(not(test))] use crate::pool::Pool;
#[cfg(test)] use crate::pool::MockPool as Pool;

/// Direct Data Management Layer for a single `Pool`
pub struct DDML {
    // Sadly, the Cache needs to be Mutex-protected because updating the LRU
    // list requires exclusive access.  It can be a normal Mutex instead of a
    // futures_lock::Mutex, because we will never need to block while holding
    // this lock.
    cache: Arc<Mutex<Cache>>,
    // TODO: consider moving pending_insertions into cache to share its
    // Arc<Mutex<_>>
    //pending_insertions: Arc<Mutex<BTreeMap<PBA, Vec<oneshot::Sender<()>>>>>,
    pool: Arc<Pool>,
}

// Some of these methods have no unit tests.  Their test coverage is provided
// instead by integration tests.
#[cfg_attr(test, allow(unused))]
impl DDML {
    /// Assert that the given zone was clean as of the given transaction
    #[cfg(debug_assertions)]
    pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT) {
        self.pool.assert_clean_zone(cluster, zone, txg)
    }

    /// Free a record's storage, ignoring the Cache
    pub fn delete_direct(&self, drp: &DRP, _txg: TxgT) -> BoxVdevFut
    {
        Box::pin(self.pool.free(drp.pba, drp.asize()))
    }

    pub fn flush(&self, idx: u32) -> BoxVdevFut {
        Box::pin(self.pool.flush(idx))
    }

    pub fn new(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self {
        //let pending_insertions = Default::default();
        DDML{pool: Arc::new(pool), cache}
        //DDML{pool: Arc::new(pool), cache, pending_insertions}
    }

    /// Get directly from disk, bypassing cache
    #[instrument(skip(self, drp))]
    pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        self.read(*drp).map_ok(move |dbs| {
            Box::new(T::deserialize(dbs))
        }).boxed()
    }

    //fn get_direct_selfless<T: Cacheable>(pool: Arc<Pool>, drp: &DRP)
        //-> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    //{
        //Self::read_selfless(pool, *drp).map_ok(move |dbs| {
            //Box::new(T::deserialize(dbs))
        //}).boxed()
    //}

    /// List all closed zones in the `DDML` in no particular order
    pub fn list_closed_zones(&self)
        -> impl Iterator<Item=ClosedZone> + Send
    {
        let mut next = (0, 0);
        let pool = self.pool.clone();
        iter::from_fn(move || {
            loop {
                match pool.find_closed_zone(next.0, next.1) {
                    (Some(pclz), Some((c, z))) => {
                        next = (c, z);
                        break Some(pclz);
                    },
                    (Some(_), None) => unreachable!(),  // LCOV_EXCL_LINE
                    (None, Some((c, z))) => {
                        next = (c, z);
                        continue;
                    },
                    (None, None) => {break None;}
                }
            }
        })
    }

    /// Read a record from disk
    #[instrument(skip(self))]
    fn read(&self, drp: DRP)
        -> impl Future<Output=Result<DivBufShared>> + Send
    {
        // Outline
        // 1) Read
        // 2) Truncate
        // 3) Verify checksum
        // 4) Decompress
        let len = drp.asize() as usize * BYTES_PER_LBA;
        let dbs = DivBufShared::uninitialized(len);
        Box::pin(
            // Read
            self.pool.read(dbs.try_mut().unwrap(), drp.pba)
            .and_then(move |_| {
                //Truncate
                let mut dbm = dbs.try_mut().unwrap();
                dbm.try_truncate(drp.csize as usize).unwrap();
                let db = dbm.freeze();

                // Verify checksum
                let mut hasher = MetroHash64::new();
                checksum_iovec(&db, &mut hasher);
                let checksum = hasher.finish();
                if checksum == drp.checksum {
                    // Decompress
                    let db = dbs.try_const().unwrap();
                    if drp.is_compressed() {
                        future::ok(Compression::decompress(&db))
                    } else {
                        future::ok(dbs)
                    }
                } else {
                    tracing::warn!("Checksum mismatch");
                    future::err(Error::EINTEGRITY)
                }
            })
        )
    }

    //fn read_selfless(pool: Arc<Pool>, drp: DRP)
        //-> impl Future<Output=Result<DivBufShared>> + Send
    //{
        //// Outline
        //// 1) Read
        //// 2) Truncate
        //// 3) Verify checksum
        //// 4) Decompress
        //let len = drp.asize() as usize * BYTES_PER_LBA;
        //let dbs = DivBufShared::uninitialized(len);
        //Box::pin(
            //// Read
            //pool.read(dbs.try_mut().unwrap(), drp.pba)
            //.and_then(move |_| {
                ////Truncate
                //let mut dbm = dbs.try_mut().unwrap();
                //dbm.try_truncate(drp.csize as usize).unwrap();
                //let db = dbm.freeze();

                //// Verify checksum
                //let mut hasher = MetroHash64::new();
                //checksum_iovec(&db, &mut hasher);
                //let checksum = hasher.finish();
                //if checksum == drp.checksum {
                    //// Decompress
                    //let db = dbs.try_const().unwrap();
                    //if drp.is_compressed() {
                        //future::ok(Compression::decompress(&db))
                    //} else {
                        //future::ok(dbs)
                    //}
                //} else {
                    //tracing::warn!("Checksum mismatch");
                    //future::err(Error::EINTEGRITY)
                //}
            //})
        //)
    //}

    /// Open an existing `DDML` from its underlying `Pool`.
    ///
    /// # Parameters
    ///
    /// * `cache`:      An already constructed `Cache`
    /// * `pool`:       An already constructed `Pool`
    pub fn open(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self {
        //let pending_insertions = Default::default();
        DDML{pool: Arc::new(pool), cache}
        //DDML{pool: Arc::new(pool), cache, pending_insertions}
    }

    /// Read a record and return ownership of it, bypassing Cache
    #[instrument(skip(self, drp))]
    pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Output=Result<Box<T>>> + Send
    {
        let lbas = drp.asize();
        let pba = drp.pba;
        let pool2 = self.pool.clone();
        self.read(*drp)
            .and_then(move |dbs|
                pool2.free(pba, lbas)
                .map_ok(move |_| Box::new(T::deserialize(dbs)))
            )
    }

    pub fn pool_name(&self) -> &str {
        self.pool.name()
    }

    /// Does most of the work of DDML::put
    fn put_common<T>(&self, cacheref: &T, compression: Compression,
                     txg: TxgT)
        -> impl Future<Output=Result<DRP>> + Send
        where T: borrow::Borrow<dyn CacheRef>
    {
        // Outline:
        // 1) Serialize
        // 2) Compress
        // 3) Checksum
        // 4) Write
        // 5) Cache

        // Serialize
        let serialized = cacheref.borrow().serialize();
        assert!(serialized.len() < u32::max_value() as usize,
            "Record exceeds maximum allowable length");
        let lsize = serialized.len();

        // Compress
        let (compressed_db, compression) = compression.compress(serialized);
        let compressed = compression.is_compressed();
        let csize = compressed_db.len() as u32;

        // Checksum
        let mut hasher = MetroHash64::new();
        checksum_iovec(&compressed_db, &mut hasher);
        let checksum = hasher.finish();

        // Write
        self.pool.write(compressed_db, txg)
        .map_ok(move |pba| {
            DRP { pba, compressed, lsize: lsize as u32, csize, checksum }
        })
    }

    /// Write a buffer bypassing cache.  Return the same buffer
    pub fn put_direct<T>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
        -> impl Future<Output=Result<DRP>> + Send
        where T: borrow::Borrow<dyn CacheRef>
    {
        self.put_common(cacheref, compression, txg)
    }

    /// Return approximately the usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.pool.size()
    }

    /// How many blocks are currently used?
    pub fn used(&self) -> LbaT {
        self.pool.used()
    }

    pub fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Output=Result<()>> + Send
    {
        self.pool.write_label(labeller)
    }
}

impl DML for DDML {
    type Addr = DRP;

    fn delete(&self, drp: &DRP, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
        Box::pin(self.pool.free(drp.pba, drp.asize()))
    }

    fn evict(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
    }

    #[instrument(skip(self))]
    fn get<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>
    {
        // Outline:
        // 1) Fetch from cache, or
        // 2) Wait on any pending cache insertions, or
        // 3) Read from disk, then insert into cache, then notify waiters
        let pba = drp.pba;
        let key = Key::PBA(pba);

        cache::get_or_insert!(T, R, &self.cache, key,
            self.get_direct(drp)
        )
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, drp: &DRP, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        let lbas = drp.asize();
        let pba = drp.pba;
        self.cache.lock().unwrap().remove(&Key::PBA(pba)).map(|cacheable| {
            let t = cacheable.downcast::<T>().unwrap();
            Box::pin(self.pool.free(pba, lbas).map_ok(|_| t)) as Pin<Box<_>>
        }).unwrap_or_else(|| {
            Box::pin( self.pop_direct::<T>(drp)) as Pin<Box<_>>
        })
    }

    #[instrument(skip(self))]
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<<Self as DML>::Addr>> + Send>>
    {
        let cache2 = self.cache.clone();
        let db = cacheable.make_ref();
        let fut = self.put_common(&db, compression, txg)
            .map_ok(move |drp|{
                let pba = drp.pba();
                cache2.lock().unwrap()
                    .insert(Key::PBA(pba), Box::new(cacheable));
                drp
            }).in_current_span();
        Box::pin(fut)
    }

    fn repay(&self, credit: Credit) {
        // Writes to the DDML should never attempt to borrow credit.  That could
        // lead to deadlocks.
        debug_assert!(credit.is_null());
        mem::forget(credit);
    }

    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        Box::pin(self.pool.sync_all())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock! {
    pub DDML {
        pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT);
        pub fn delete_direct(&self, drp: &DRP, txg: TxgT) -> BoxVdevFut;
        pub fn flush(&self, idx: u32) -> BoxVdevFut;
        pub fn new(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self;
        pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        pub fn list_closed_zones(&self)
            -> Box<dyn Iterator<Item=ClosedZone> + Send>;
        pub fn open(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self;
        pub fn pool_name(&self) -> &str;
        pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        pub fn put_direct<T: 'static>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<DRP>> + Send>>
            where T: borrow::Borrow<dyn CacheRef>;
        pub fn size(&self) -> LbaT;
        pub fn used(&self) -> LbaT;
        pub fn write_label(&self, labeller: LabelWriter)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
    }
    impl DML for DDML {
        type Addr = DRP;

        fn delete(&self, addr: &DRP, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
        fn evict(&self, addr: &DRP);
        fn get<T: Cacheable, R: CacheRef>(&self, addr: &DRP)
            -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>;
        fn pop<T: Cacheable, R: CacheRef>(&self, rid: &DRP, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                                 txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<DRP>> + Send>>;
        fn repay(&self, credit: Credit);
        fn sync_all(&self, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
    }
}

#[cfg(test)]
mod t {
mod drp {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn as_uncompressed() {
        let drp0 = DRP::random(Compression::Zstd(None), 5000);
        let drp0_nc = drp0.as_uncompressed();
        assert!(!drp0_nc.is_compressed());
        assert_eq!(drp0_nc.lsize, drp0_nc.csize);
        assert_eq!(drp0_nc.csize, drp0.csize);
        assert_eq!(drp0_nc.pba, drp0.pba);

        //drp1 is what DDML::put_direct will return after writing drp0_nc's
        //contents as uncompressed
        let mut drp1 = DRP::random(Compression::None, drp0.csize as usize);
        drp1.checksum = drp0_nc.checksum;

        let drp1_c = drp1.into_compressed(&drp0);
        assert!(drp1_c.is_compressed());
        assert_eq!(drp1_c.lsize, drp0.lsize);
        assert_eq!(drp1_c.csize, drp0.csize);
        assert_eq!(drp1_c.pba, drp1.pba);
        assert_eq!(drp1_c.checksum, drp0.checksum);
    }

    #[test]
    fn typical_size() {
        let drp = DRP::random(Compression::Zstd(None), 5000);
        let size = bincode::serialized_size(&drp).unwrap() as usize;
        assert_eq!(DRP::TYPICAL_SIZE, size);
    }
}

mod ddml {
    use super::super::*;
    use divbuf::{DivBuf, DivBufShared};
    use futures::{
        FutureExt,
        future,
        channel::oneshot
    };
    use mockall::{
        self,
        Sequence,
        predicate::*
    };
    use pretty_assertions::assert_eq;
    use rand::{RngCore, SeedableRng};
    use rand_xorshift::XorShiftRng;

    #[test]
    fn delete_hot() {
        let mut seq = Sequence::new();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(Key::PBA(pba), Box::new(dbs));
        let mut pool = Pool::default();
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::pin(future::ok(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.delete(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn evict() {
        let pba = PBA::default();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let key = Key::PBA(pba);
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(Key::PBA(pba), Box::new(dbs));
        let pool = Pool::default();

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        ddml.evict(&drp);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());
    }

    #[test]
    fn get_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = Pool::default();
        pool.expect_read()
            .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::pin(future::ok::<(), Error>(()))
            });

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.get_direct::<DivBufShared>(&drp)
            .now_or_never().unwrap()
            .unwrap();
    }

    mod get {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Near-simultaneous get requests should not result in multiple reads
        /// from disk.
        #[tokio::test]
        async fn duplicate() {
            let pba = PBA::default();
            let key = Key::PBA(pba);
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
            let (tx, rx) = oneshot::channel::<()>();
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = Pool::default();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .once()
                .return_once(move |mut dbm, _pba| {
                    for x in dbm.iter_mut() {
                        *x = 0;
                    }
                    Box::pin(rx.map_err(Error::unhandled_error))
                });

            let amcache = Arc::new(Mutex::new(cache));
            let ddml = DDML::new(pool, amcache.clone());
            let fut1 = ddml.get::<DivBufShared, DivBuf>(&drp);
            let fut2 = ddml.get::<DivBufShared, DivBuf>(&drp);
            tx.send(()).unwrap();
            future::try_join(fut1, fut2).await.unwrap();
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
        }

        #[test]
        fn hot() {
            let pba = PBA::default();
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 4096, checksum: 0};
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let mut cache = Cache::with_capacity(1_048_576);
            cache.insert(Key::PBA(pba), Box::new(dbs));
            let pool = Pool::default();

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap();
        }

        #[test]
        fn cold() {
            let mut seq = Sequence::new();
            let pba = PBA::default();
            let key = Key::PBA(pba);
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = Pool::default();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .once()
                .in_sequence(&mut seq)
                .returning(|mut dbm, _pba| {
                    for x in dbm.iter_mut() {
                        *x = 0;
                    }
                    Box::pin(future::ok::<(), Error>(()))
                });

            let amcache = Arc::new(Mutex::new(cache));
            let ddml = DDML::new(pool, amcache.clone());
            ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap();
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
        }

        #[test]
        fn ecksum() {
            let pba = PBA::default();
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xdead_beef_dead_beef};
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = Pool::default();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            let err = ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap_err();
            assert_eq!(err, Error::EINTEGRITY);
        }
    }

    #[test]
    fn list_closed_zones() {
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = Pool::default();

        // The first cluster has two closed zones
        let clz0 = ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5, zid: 0,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        let clz0_1 = clz0.clone();
        pool.expect_find_closed_zone()
            .with(eq(0), eq(0))
            .return_once(move |_, _| (Some(clz0_1), Some((0, 11))));

        let clz1 = ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6, zid: 1,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3)};
        let clz1_1 = clz1.clone();
        pool.expect_find_closed_zone()
            .with(eq(0), eq(11))
            .return_once(move |_, _| (Some(clz1_1), Some((0, 31))));

        pool.expect_find_closed_zone()
            .with(eq(0), eq(31))
            .return_once(|_, _| (None, Some((1, 0))));

        // The second cluster has no closed zones
        pool.expect_find_closed_zone()
            .with(eq(1), eq(0))
            .return_once(|_, _| (None, Some((2, 0))));

        // The third cluster has one closed zone
        let clz2 = ClosedZone{pba: PBA::new(2, 10), freed_blocks: 5, zid: 2,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        let clz2_1 = clz2.clone();
        pool.expect_find_closed_zone()
            .with(eq(2), eq(0))
            .return_once(move |_, _| (Some(clz2_1), Some((2, 11))));

        pool.expect_find_closed_zone()
            .with(eq(2), eq(11))
            .return_once(|_, _| (None, None));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));

        let closed_zones: Vec<ClosedZone> = ddml.list_closed_zones()
            .collect();
        let expected = vec![clz0, clz1, clz2];
        assert_eq!(closed_zones, expected);
    }

    #[test]
    fn pop_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let key = Key::PBA(pba);
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(Key::PBA(pba), Box::new(dbs));
        let mut pool = Pool::default();
        pool.expect_free()
            .with(eq(pba), eq(1))
            .return_once(|_, _| Box::pin(future::ok(())));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap();
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());

    }

    #[test]
    fn pop_cold() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let mut seq = Sequence::new();
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = Pool::default();
        pool.expect_read()
            .with(always(), eq(pba))
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::pin(future::ok::<(), Error>(()))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::pin(future::ok(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn pop_ecksum() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xdead_beef_dead_beef};
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = Pool::default();
        pool.expect_read()
            .with(always(), eq(pba))
            .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let err = ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap_err();
        assert_eq!(err, Error::EINTEGRITY);
    }

    #[test]
    fn pop_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let mut seq = Sequence::new();
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = Pool::default();
        pool.expect_read()
            .with(always(), eq(pba))
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::pin(future::ok::<(), Error>(()))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::pin(future::ok(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.pop_direct::<DivBufShared>(&drp)
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn put() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let drp = ddml.put(dbs, Compression::None, TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
        assert_eq!(drp.pba, pba);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    /// With compression enabled, compressible data should be compressed
    #[test]
    fn put_compressible() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let drp = ddml.put(dbs, Compression::Zstd(None), TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert!(drp.is_compressed());
        assert!(drp.csize < 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    /// Incompressible data should not be compressed, even when compression is
    /// enabled.
    #[test]
    fn put_incompressible() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; 8192];
        rng.fill_bytes(&mut v[..]);
        let dbs = DivBufShared::from(v);
        let drp = ddml.put(dbs, Compression::Zstd(None), TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    #[test]
    fn put_partial_lba() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let dbs = DivBufShared::from(vec![42u8; 1024]);
        let drp = ddml.put(dbs, Compression::None, TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 1024);
        assert_eq!(drp.lsize, 1024);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    #[test]
    fn put_direct() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let mut pool = Pool::default();
        let txg = TxgT::from(42);
        pool.expect_write()
            .with(always(), eq(txg))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let db = Box::new(dbs.try_const().unwrap()) as Box<dyn CacheRef>;
        let drp = ddml.put_direct(&db, Compression::None, txg)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
    }

    #[test]
    fn sync_all() {
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = Pool::default();
        pool.expect_sync_all()
            .return_once(|| Box::pin(future::ok::<(), Error>(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        assert!(ddml.sync_all(TxgT::from(0))
                .now_or_never().unwrap()
                .is_ok());
    }
}
}
// LCOV_EXCL_STOP
