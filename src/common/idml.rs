// vim: tw=80
///! Indirect Data Management Layer
///
/// Interface for working with indirect records.  An indirect record is a record
/// that is referenced by an immutable Record ID, rather than a disk address.
/// Unlike a direct record, it may be duplicated, by through snapshots, clones,
/// or deduplication.

// use the atomic crate since libstd's AtomicU64 type is still unstable
// https://github.com/rust-lang/rust/issues/32976
use atomic::{Atomic, Ordering};
use common::{
    *,
    dml::*,
    ddml::DRP,
    cache::{Cacheable, CacheRef, Key},
    label::*,
    tree::*
};
use futures::{Future, IntoFuture, Stream, future};
use futures_locks::{RwLock, RwLockReadFut};
use nix::{Error, errno::Errno};
use std::sync::{Arc, Mutex};

#[cfg(not(test))]
use common::cache::Cache;
#[cfg(test)]
use common::cache_mock::CacheMock as Cache;
#[cfg(not(test))]
use common::ddml::DDML;
#[cfg(test)]
use common::ddml_mock::DDMLMock as DDML;

pub use common::ddml::ClosedZone;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct RidtEntry {
    drp: DRP,
    refcount: u64
}

impl RidtEntry {
    fn new(drp: DRP) -> Self {
        RidtEntry{drp, refcount: 1}
    }
}

pub type DTree<K, V> = Tree<DRP, DDML, K, V>;

/// Indirect Data Management Layer for a single `Pool`
pub struct IDML {
    cache: Arc<Mutex<Cache>>,

    ddml: Arc<DDML>,

    /// Allocation table.  The reverse of `ridt`.
    ///
    /// Maps disk addresses back to record IDs.  Used for operations like
    /// garbage collection and defragmentation.
    // Even though it has a single owner, the tree must be Arc so IDML methods
    // can be 'static
    // TODO: consider a lazy delete strategy to reduce the amount of tree
    // activity on pop/delete by deferring alloct removals to the cleaner.
    alloct: Arc<DTree<PBA, RID>>,

    /// Holds the next RID to allocate.  They are never reused.
    next_rid: Atomic<u64>,

    /// Record indirection table.  Maps record IDs to disk addresses.
    ridt: Arc<DTree<RID, RidtEntry>>,

    /// Current transaction group
    transaction: RwLock<TxgT>
}

impl<'a> IDML {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.ddml.allocated()
    }

    /// Clean `zone` by moving all of its records to other zones.
    pub fn clean_zone(&'a self, zone: ClosedZone, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        // Outline:
        // 1) Lookup the Zone's PBA range in the Allocation Table.  Rewrite each
        //    record, modifying the RIDT and AllocT for each record
        // 2) Clean the Allocation table and RIDT themselves.  This must happen
        //    second, because the first step will reduce the amount of work to
        //    do in the second.
        let end = PBA::new(zone.pba.cluster, zone.pba.lba + zone.total_blocks);
        self.list_indirect_records(&zone).for_each(move |record| {
            self.move_record(record, txg)
        }).and_then(move |_| {
            let czfut = self.ridt.clean_zone(zone.pba..end, zone.txgs.clone(),
                                             txg);
            let atfut = self.alloct.clean_zone(zone.pba..end, zone.txgs,
                                               txg);
            czfut.join(atfut).map(|_| ())
        })
    }

    pub fn create(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>) -> Self {
        let alloct = Arc::new(DTree::<PBA, RID>::create(ddml.clone()));
        let next_rid = Atomic::new(0);
        let ridt = Arc::new(DTree::<RID, RidtEntry>::create(ddml.clone()));
        let transaction = RwLock::new(TxgT::from(0));
        IDML{alloct, cache, ddml, next_rid, ridt, transaction}
    }

    pub fn list_closed_zones(&'a self)
        -> impl Stream<Item=ClosedZone, Error=Error> + 'a
    {
        self.ddml.list_closed_zones()
    }

    /// Return a list of all active (not deleted) indirect Records that have
    /// been written to the IDML in the given Zone.
    ///
    /// This list should be persistent across reboots.
    fn list_indirect_records(&'a self, zone: &ClosedZone)
        -> impl Stream<Item=RID, Error=Error> + 'a
    {
        // Iterate through the AllocT to get indirect records from the target
        // zone.
        let end = PBA::new(zone.pba.cluster, zone.pba.lba + zone.total_blocks);
        self.alloct.range(zone.pba..end)
            .map(|(_pba, rid)| rid)
    }

    /// Open an existing `IDML`
    ///
    /// # Parameters
    ///
    /// * `ddml`:           An already-opened `DDML`
    /// * `cache`:          An already-constrcuted `Cache`
    /// * `label_reader`:   A `LabelReader` that has already consumed all labels
    ///                     prior to this layer.
    pub fn open(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>,
                 mut label_reader: LabelReader) -> Self
    {
        let l: Label = label_reader.deserialize().unwrap();
        let alloct = Arc::new(Tree::open(ddml.clone(), l.alloct).unwrap());
        let ridt = Arc::new(Tree::open(ddml.clone(), l.ridt).unwrap());
        let transaction = RwLock::new(l.txg);
        let next_rid = Atomic::new(l.next_rid);
        IDML{alloct, cache, ddml, next_rid, ridt, transaction}
    }

    /// Rewrite the given direct Record and update its metadata.
    fn move_record(&'a self, rid: RID, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        // Even if the cache contains the target record, we must also do an RIDT
        // lookup because we're going to rewrite the RIDT
        self.ridt.get(rid)
            .and_then(move |v| {
                let entry = v.expect(
                    "Inconsistency in alloct.  Entry not found in RIDT");
                self.cache.lock().unwrap().get::<DivBuf>(&Key::Rid(rid))
                    .map(|t: Box<DivBuf>| {
                        // XXX: this data copy could probably be removed
                        let r = (*t).to_owned()
                            .downcast::<DivBufShared>()
                            .unwrap();
                        Box::new(
                            future::ok::<Box<DivBufShared>, Error>(r)
                        ) as Box<Future<Item=Box<DivBufShared>, Error=Error>>
                    })
                    .unwrap_or_else(|| {
                        Box::new(
                            self.ddml.get_direct::<DivBufShared>(&entry.drp)
                        ) as Box<Future<Item=Box<DivBufShared>, Error=Error>>
                    }).map(move |buf| (entry, buf))
            }).and_then(move |(entry, buf)| {
                // NB: on a cache miss, this will result in decompressing and
                // recompressing the record, which is inefficient.
                let compression = entry.drp.compression();
                self.ddml.put_direct(*buf, compression, txg)
                    .map(move |(drp, _cacheable)| (entry, drp))
            }).and_then(move |(mut entry, drp)| {
                entry.drp = drp;
                let ridt_fut = self.ridt.insert(rid, entry, txg);
                let alloct_fut = self.alloct.insert(drp.pba(), rid, txg);
                ridt_fut.join(alloct_fut)
            }).map(|_| ())
    }

    /// Return approximately the usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.ddml.size()
    }

    /// Get a reference to the current transaction group.
    ///
    /// The reference will prevent the current transaction group from syncing,
    /// so don't hold it too long.
    pub fn txg(&self) -> RwLockReadFut<TxgT> {
        self.transaction.read()
    }

    /// Finish the current transaction group and start a new one.
    pub fn advance_transaction<B, F>(&'a self, f: F)
        -> impl Future<Item=(), Error=Error> + 'a
        where F: FnOnce(TxgT) -> B + 'a,
              B: IntoFuture<Item = (), Error = Error> + 'a
    {
        self.transaction.write()
            .map_err(|_| Error::Sys(Errno::EPIPE))
            .and_then(move |mut txg_guard| {
                let txg = *txg_guard;
                f(txg).into_future()
                .map(move |_| *txg_guard += 1)
            })
    }

    /// Asynchronously write this `IDML`'s label to its `Pool`
    pub fn write_label(&'a self, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        let mut labeller = LabelWriter::new();
        self.alloct.flush(txg)
        .join(self.ridt.flush(txg))
        .and_then(move |(alloct, ridt)| {
            let label = Label {
                alloct,
                next_rid: self.next_rid.load(Ordering::Relaxed),
                ridt,
                txg,
            };
            labeller.serialize(label).unwrap();
            self.ddml.write_label(labeller)
        })
    }
}

impl DML for IDML {
    type Addr = RID;

    fn delete(&self, ridp: &Self::Addr, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        let cache2 = self.cache.clone();
        let ddml2 = self.ddml.clone();
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let rid = *ridp;
        let fut = self.ridt.get(rid)
            .and_then(|r| {
                match r {
                    None => Err(Error::Sys(Errno::ENOENT)).into_future(),
                    Some(entry) => Ok(entry).into_future()
                }
            }).and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    cache2.lock().unwrap().remove(&Key::Rid(rid));
                    // TODO: usd ddml.delete_direct
                    let ddml_fut = ddml2.delete(&entry.drp, txg);
                    let alloct_fut = alloct2.remove(entry.drp.pba(), txg);
                    let ridt_fut = ridt2.remove(rid, txg);
                    Box::new(
                        ddml_fut.join3(alloct_fut, ridt_fut)
                             .map(|(_, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                             })
                     ) as Box<Future<Item=(), Error=Error> + Send>
                } else {
                    let ridt_fut = ridt2.insert(rid, entry, txg)
                        .map(|_| ());
                    Box::new(ridt_fut)
                    as Box<Future<Item=(), Error=Error> + Send>
                }
            });
        Box::new(fut)
    }

    fn evict(&self, rid: &Self::Addr) {
        self.cache.lock().unwrap().remove(&Key::Rid(*rid));
    }

    fn get<T: Cacheable, R: CacheRef>(&self, ridp: &Self::Addr)
        -> Box<Future<Item=Box<R>, Error=Error> + Send>
    {
        let rid = *ridp;
        self.cache.lock().unwrap().get::<R>(&Key::Rid(rid)).map(|t| {
            Box::new(future::ok::<Box<R>, Error>(t))
                as Box<Future<Item=Box<R>, Error=Error> + Send>
        }).unwrap_or_else(|| {
            let cache2 = self.cache.clone();
            let ddml2 = self.ddml.clone();
            let fut = self.ridt.get(rid)
                .and_then(|r| {
                    match r {
                        None => Err(Error::Sys(Errno::ENOENT)).into_future(),
                        Some(entry) => Ok(entry).into_future()
                    }
                }).and_then(move |entry| {
                    ddml2.get_direct(&entry.drp)
                }).map(move |cacheable: Box<T>| {
                    let r = cacheable.make_ref();
                    let key = Key::Rid(rid);
                    cache2.lock().unwrap().insert(key, cacheable);
                    r.downcast::<R>().unwrap()
                });
            Box::new(fut)
        })
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, ridp: &Self::Addr, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>
    {
        let rid = *ridp;
        let cache2 = self.cache.clone();
        let ddml2 = self.ddml.clone();
        let ddml3 = self.ddml.clone();
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let fut = self.ridt.get(rid)
            .and_then(|r| {
                match r {
                    None => Err(Error::Sys(Errno::ENOENT)).into_future(),
                    Some(entry) => Ok(entry).into_future()
                }
            }).and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    let cacheval = cache2.lock().unwrap()
                        .remove(&Key::Rid(rid));
                    let bfut = cacheval
                        .map(move |cacheable| {
                            let t = cacheable.downcast::<T>().unwrap();
                            Box::new(ddml2.delete(&entry.drp, txg)
                                              .map(move |_| t)
                            ) as Box<Future<Item=Box<T>, Error=Error> + Send>
                        }).unwrap_or_else(||{
                            Box::new(ddml3.pop_direct::<T>(&entry.drp))
                        }) as Box<Future<Item=Box<T>, Error=Error> + Send>;
                    let alloct_fut = alloct2.remove(entry.drp.pba(), txg);
                    let ridt_fut = ridt2.remove(rid, txg);
                    Box::new(
                        bfut.join3(alloct_fut, ridt_fut)
                             .map(|(cacheable, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                                 cacheable
                             })
                     ) as Box<Future<Item=Box<T>, Error=Error> + Send>
                } else {
                    let cacheval = cache2.lock().unwrap()
                        .get::<R>(&Key::Rid(rid));
                    let bfut = cacheval.map(|cacheref: Box<R>|{
                        let t = cacheref.to_owned().downcast::<T>().unwrap();
                        Box::new(future::ok(t))
                            as Box<Future<Item=Box<T>, Error=Error> + Send>
                    }).unwrap_or_else(|| {
                        Box::new(ddml2.get_direct::<T>(&entry.drp))
                    });
                    let ridt_fut = ridt2.insert(rid, entry, txg);
                    Box::new(
                        bfut.join(ridt_fut)
                            .map(|(cacheable, _)| {
                                cacheable
                            })
                    ) as Box<Future<Item=Box<T>, Error=Error> + Send>
                }
            });
        Box::new(fut)
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Box<Future<Item=Self::Addr, Error=Error> + Send>
    {
        // Outline:
        // 1) Write to the DDML
        // 2) Cache
        // 3) Add entry to the RIDT
        // 4) Add reverse entry to the AllocT
        let cache2 = self.cache.clone();
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));

        let fut = self.ddml.put_direct(cacheable, compression, txg)
        .and_then(move|(drp, cacheable)| {
            let alloct_fut = alloct2.insert(drp.pba(), rid, txg);
            let rid_entry = RidtEntry::new(drp);
            let ridt_fut = ridt2.insert(rid, rid_entry, txg);
            ridt_fut.join(alloct_fut)
            .map(move |(old_rid_entry, old_alloc_entry)| {
                assert!(old_rid_entry.is_none(), "RID was not unique");
                assert!(old_alloc_entry.is_none(), concat!(
                    "Double allocate without free.  ",
                    "DDML allocator leak detected!"));
                cache2.lock().unwrap().insert(Key::Rid(rid),
                    Box::new(cacheable));
                rid
            })
        });
        Box::new(fut)
    }

    fn sync_all(&self, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>
    {
        let ddml2 = self.ddml.clone();
        let fut = self.ridt.flush(txg)
            .join(self.alloct.flush(txg))
            .and_then(move |(_, _)| ddml2.sync_all(txg));
        Box::new(fut)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    alloct:             TreeOnDisk,
    next_rid:           u64,
    ridt:               TreeOnDisk,
    /// Last transaction group synced before the label was written
    txg:                TxgT,
}

// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {

    use super::*;
    use divbuf::DivBufShared;
    use futures::future;
    use simulacrum::*;
    use simulacrum::validators::trivial::any;
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::Mutex
    };
    use tokio::runtime::current_thread;

    /// Inject a record into the RIDT and AllocT
    fn inject_record(rt: &mut current_thread::Runtime, idml: &IDML, rid: &RID,
                     drp: &DRP, refcount: u64)
    {
        let entry = RidtEntry{drp: drp.clone(), refcount};
        let txg = TxgT::from(0);
        rt.block_on(idml.ridt.insert(*rid, entry, txg)).unwrap();
        rt.block_on(idml.alloct.insert(drp.pba(), *rid, txg)).unwrap();
    }

    // pet kcov
    #[test]
    fn ridtentry_debug() {
        let drp = DRP::random(Compression::None, 4096);
        let ridt_entry = RidtEntry::new(drp);
        format!("{:?}", ridt_entry);
    }

    #[test]
    fn delete_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let mut ddml = DDML::new();
        ddml.expect_delete()
            .called_once()
            .with(passes(move |args: &(*const DRP, TxgT)|
                         unsafe {*args.0 == drp} && args.1 == TxgT::from(42))
            ).returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 1);

        rt.block_on(idml.delete(&rid, TxgT::from(42))).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.ridt.get(rid)).unwrap().is_none());
        assert!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().is_none());
    }

    #[test]
    fn delete_notlast() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let cache = Cache::new();
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 2);

        rt.block_on(idml.delete(&rid, TxgT::from(42))).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp);
        assert_eq!(entry2.refcount, 1);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
            rid);
    }

    #[test]
    fn evict() {
        let rid = RID(42);
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        idml.evict(&rid);
    }

    #[test]
    fn get_hot() {
        let rid = RID(42);
        let mut cache = Cache::new();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| {
                Some(Box::new(dbs.try().unwrap()))
            });
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        let fut = idml.get::<DivBufShared, DivBuf>(&rid);
        current_thread::Runtime::new().unwrap().block_on(fut).unwrap();
    }

    #[test]
    fn get_cold() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::new();
        let owned_by_cache = Rc::new(
            RefCell::new(Vec::<Box<Cacheable>>::new())
        );
        let owned_by_cache2 = owned_by_cache.clone();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| None);
        cache.then().expect_insert()
            .called_once()
            .with(passes(move |args: &(Key, _)| {
                args.0 == Key::Rid(RID(42))
            })).returning(move |(_, dbs)| {;
                owned_by_cache2.borrow_mut().push(dbs);
            });
        let mut ddml = DDML::new();
        ddml.expect_get_direct::<DivBufShared>()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {**key == drp}
            })).returning(move |_| {
                let dbs = Box::new(DivBufShared::from(vec![0u8; 4096]));
                Box::new(future::ok::<Box<DivBufShared>, Error>(dbs))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 1);

        let fut = idml.get::<DivBufShared, DivBuf>(&rid);
        current_thread::Runtime::new().unwrap().block_on(fut).unwrap();
    }

    #[test]
    fn list_indirect_records() {
        let txgs = TxgT::from(0)..TxgT::from(2);
        let cz = ClosedZone{pba: PBA::new(0, 100), total_blocks: 100,
                            freed_blocks: 50, txgs};
        let cache = Cache::new();
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        // A record just below the target zone
        let rid0 = RID(99);
        let drp0 = DRP::new(PBA::new(0, 99), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, &rid0, &drp0, 1);
        // A record at the end of the target zone
        let rid2 = RID(102);
        let drp2 = DRP::new(PBA::new(0, 199), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, &rid2, &drp2, 1);
        // A record at the start of the target zone
        let rid1 = RID(92);
        let drp1 = DRP::new(PBA::new(0, 100), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, &rid1, &drp1, 1);
        // A record just past the target zone
        let rid3 = RID(101);
        let drp3 = DRP::new(PBA::new(0, 200), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, &rid3, &drp3, 1);
        // A record in the same LBA range as but different cluster than the
        // target zone
        let rid4 = RID(105);
        let drp4 = DRP::new(PBA::new(1, 150), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, &rid4, &drp4, 1);

        let r = rt.block_on(idml.list_indirect_records(&cz).collect());
        assert_eq!(r.unwrap(), vec![rid1, rid2]);
    }

    #[test]
    fn move_indirect_record_cold() {
        let v = vec![42u8; 4096];
        let dbs = DivBufShared::from(v.clone());
        let rid = RID(1);
        let drp0 = DRP::random(Compression::None, 4096);
        let drp1 = DRP::random(Compression::None, 4096);
        let drp2 = drp1.clone();
        let mut cache = Cache::new();
        let mut ddml = DDML::new();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(rid)}
            })).returning(move |_| {
                None
            });
        ddml.expect_get_direct()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {**key == drp0}
            })).returning(move |_| {
                let r = DivBufShared::from(&dbs.try().unwrap()[..]);
                Box::new(future::ok::<Box<DivBufShared>, Error>(Box::new(r)))
            });
        ddml.expect_put_direct::<DivBufShared>()
            .called_once()
            .returning(move |(buf, _, _)|
                       Box::new(Ok((drp1, buf)).into_future())
            );
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp0, 1);

        rt.block_on(idml.move_record(rid, TxgT::from(0))).unwrap();

        // Now verify the RIDT and alloct entries
        let entry = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp2);
        assert_eq!(rt.block_on(idml.alloct.get(drp2.pba())).unwrap().unwrap(),
                   rid);
    }

    #[test]
    fn move_indirect_record_hot() {
        let v = vec![42u8; 4096];
        let dbs = DivBufShared::from(v.clone());
        let rid = RID(1);
        let drp0 = DRP::random(Compression::None, 4096);
        let drp1 = DRP::random(Compression::None, 4096);
        let drp2 = drp1.clone();
        let mut cache = Cache::new();
        let mut ddml = DDML::new();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(rid)}
            })).returning(move |_| {
                Some(Box::new(dbs.try().unwrap()))
            });
        ddml.expect_put_direct::<DivBufShared>()
            .called_once()
            .returning(move |(buf, _, _)|
                       Box::new(Ok((drp1, buf)).into_future())
            );
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp0, 1);

        rt.block_on(idml.move_record(rid, TxgT::from(0))).unwrap();

        // Now verify the RIDT and alloct entries
        let entry = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp2);
        assert_eq!(rt.block_on(idml.alloct.get(drp2.pba())).unwrap().unwrap(),
                   rid);
    }

    #[test]
    fn pop_hot_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let mut ddml = DDML::new();
        ddml.expect_delete()
            .called_once()
            .with(passes(move |args: &(*const DRP, TxgT)|
                         unsafe {*args.0 == drp} && args.1 == TxgT::from(42))
            ).returning(|_| {
                Box::new(future::ok::<(), Error>(()))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 1);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(42));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.ridt.get(rid)).unwrap().is_none());
        assert!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().is_none());
    }

    #[test]
    fn pop_hot_notlast() {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::new();
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| {
                Some(Box::new(dbs.try().unwrap()))
            });
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 2);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp);
        assert_eq!(entry2.refcount, 1);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
            rid);
    }

    #[test]
    fn pop_cold_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| None );
        let mut ddml = DDML::new();
        ddml.expect_pop_direct::<DivBufShared>()
            .called_once()
            .with(passes(move |key: &*const DRP| unsafe {**key == drp}))
            .returning(|_| {
                let dbs = DivBufShared::from(vec![42u8; 4096]);
                Box::new(future::ok::<Box<DivBufShared>, Error>(Box::new(dbs)))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 1);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.ridt.get(rid)).unwrap().is_none());
        assert!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().is_none());
    }

    #[test]
    fn pop_cold_notlast() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::new();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| None );
        let mut ddml = DDML::new();
        ddml.expect_get_direct()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {**key == drp}
            })).returning(move |_| {
                let dbs = Box::new(DivBufShared::from(vec![42u8; 4096]));
                Box::new(future::ok::<Box<DivBufShared>, Error>(dbs))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 2);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp);
        assert_eq!(entry2.refcount, 1);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
            rid);
    }

    #[test]
    fn put() {
        let mut cache = Cache::new();
        let mut ddml = DDML::new();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdeadbeef);
        let rid = RID(0);
        cache.expect_insert()
            .called_once()
            .with(params!(Key::Rid(rid), any()))
            .returning(|_| ());
        ddml.expect_put_direct::<DivBufShared>()
            .called_once()
            .returning(move |(buf, _, _)|
                       Box::new(Ok((drp, buf)).into_future())
            );
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let actual_rid = rt.block_on(
            idml.put(dbs, Compression::None, TxgT::from(0))
        ).unwrap();
        assert_eq!(rid, actual_rid);

        // Now verify the contents of the RIDT and AllocT
        let entry = rt.block_on(idml.ridt.get(actual_rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
                   actual_rid);
    }

    #[ignore = "Simulacrum can't mock a single generic method with different type parameters more than once in the same test https://github.com/pcsm/simulacrum/issues/55"]
    #[test]
    fn sync_all() {
        let rid = RID(42);
        let cache = Cache::new();
        let mut ddml = DDML::new();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdeadbeef);
        ddml.expect_put::<Arc<tree::Node<DRP, RID, RidtEntry>>>()
            .called_any()
            .with(params!(any(), any(), TxgT::from(42)))
            .returning(move |(_, _, _)| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::new(Ok(drp).into_future())
            });
        ddml.expect_put::<Arc<tree::Node<DRP, PBA, RID>>>()
            .called_any()
            .with(params!(any(), any(), TxgT::from(42)))
            .returning(move |(_, _, _)| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::new(Ok(drp).into_future())
            });
        ddml.expect_sync_all()
            .called_once()
            .with(TxgT::from(42))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, &rid, &drp, 2);

        rt.block_on(idml.sync_all(TxgT::from(42))).unwrap();
    }

    #[test]
    fn advance_transaction() {
        let cache = Cache::new();
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(idml.advance_transaction(|_txg| Ok(()))).unwrap();
        assert_eq!(*idml.transaction.try_read().unwrap(), TxgT::from(1));
    }
}
