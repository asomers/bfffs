// vim: tw=80

//! The Database layer owns all of the Datasets
//!
//! Clients use the Database to obtain references to the Datasets.  The Database
//! also owns the Forest and manages Transactions.

use common::*;
use common::dataset::*;
use common::dml::DML;
use common::fs_tree::*;
use common::idml::*;
use common::label::*;
use common::tree::*;
use futures::{Future, IntoFuture, future};
use libc;
use nix::{Error, errno};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use time;

type ReadOnlyFilesystem = ReadOnlyDataset<FSKey, FSValue>;
type ReadWriteFilesystem = ReadWriteDataset<FSKey, FSValue>;

/// Keys into the Forest
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub enum TreeID {
    /// A filesystem, snapshot, or clone
    Fs(u32)
}

impl MinValue for TreeID {
    fn min_value() -> Self {
        TreeID::Fs(u32::min_value())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    forest:             TreeOnDisk
}

struct Inner {
    filesystems: Mutex<BTreeMap<TreeID, Arc<ITree<FSKey, FSValue>>>>,
    forest: ITree<TreeID, TreeOnDisk>,
    idml: Arc<IDML>,
}

impl Inner {
    fn ro_filesystem(&self, tree_id: TreeID) -> ReadOnlyFilesystem
    {
        let fs = self.filesystems.lock().unwrap()[&tree_id].clone();
        ReadOnlyFilesystem::new(self.idml.clone(), fs)
    }

    fn rw_filesystem(&self, tree_id: TreeID, txg: TxgT) -> ReadWriteFilesystem
    {
        let fs = self.filesystems.lock().unwrap()[&tree_id].clone();
        ReadWriteFilesystem::new(self.idml.clone(), fs, txg)
    }

    /// Asynchronously write this `Database`'s label to its `IDML`
    fn write_label(&self, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        let mut labeller = LabelWriter::new();
        let idml2 = self.idml.clone();
        self.forest.flush(txg)
            .and_then(move |tod| {
                let label = Label { forest: tod };
                labeller.serialize(label).unwrap();
                idml2.write_label(labeller, txg)
            })
    }
}

pub struct Database {
    inner: Arc<Inner>,
}

impl Database {
    /// Construct a new `Database` from its `IDML`.
    pub fn create(idml: Arc<IDML>) -> Self {
        let forest = ITree::create(idml.clone());
        Database::new(idml, forest)
    }

    /// Create a new, blank filesystem
    pub fn new_fs(&self) -> impl Future<Item=TreeID, Error=Error> {
        let mut guard = self.inner.filesystems.lock().unwrap();
        let k = (0..=u32::max_value()).filter(|i| {
            !guard.contains_key(&TreeID::Fs(*i))
        }).nth(0).expect("Maximum number of filesystems reached");
        let key = TreeID::Fs(k);
        let fs = Arc::new(ITree::create(self.inner.idml.clone()));
        guard.insert(key, fs);

        // Create the filesystem's root directory
        self.fswrite(key, move |dataset| {
            let ino = 1;    // FUSE requires root dir to have inode 1
            let key = FSKey::new(ino, ObjKey::Inode);
            let now = time::get_time();
            let inode = Inode {
                size: 0,
                nlink: 0,
                flags: 0,
                atime: now,
                mtime: now,
                ctime: now,
                birthtime: now,
                uid: 0,
                gid: 0,
                mode: libc::S_IFDIR | 0o755
            };
            let value = FSValue::Inode(inode);
            dataset.insert(key, value)
        }).map(move |_| key)
    }

    /// Perform a read-only operation on a Filesystem
    pub fn fsread<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadOnlyFilesystem) -> B + 'static,
              B: IntoFuture<Item = R, Error = Error> + 'static,
              R: 'static
    {
        let ds = self.inner.ro_filesystem(tree_id);
        f(ds).into_future()
    }

    fn new(idml: Arc<IDML>, forest: ITree<TreeID, TreeOnDisk>) -> Self {
        let filesystems = Mutex::new(BTreeMap::new());
        let inner = Arc::new(Inner{filesystems, idml, forest});
        Database{inner}
    }

    /// Open an existing `Database`
    ///
    /// # Parameters
    ///
    /// * `idml`:           An already-opened `IDML`
    /// * `label_reader`:   A `LabelReader` that has already consumed all labels
    ///                     prior to this layer.
    pub fn open(idml: Arc<IDML>, mut label_reader: LabelReader) -> Self
    {
        let l: Label = label_reader.deserialize().unwrap();
        let forest = Tree::open(idml.clone(), l.forest).unwrap();
        Database::new(idml, forest)
    }

    /// Finish the current transaction group and start a new one.
    pub fn sync_transaction(&self) -> impl Future<Item=(), Error=Error>
    {
        // Outline:
        // 1) Flush the trees
        // 2) Sync the pool
        // 3) Write the label
        // 4) Sync the pool again
        // TODO: use two labels, so the pool will be recoverable even if power
        // is lost while writing a label.
        let inner2 = self.inner.clone();
        self.inner.idml.advance_transaction(move |txg|{
            let inner4 = inner2.clone();
            let idml2 = inner2.idml.clone();
            let idml3 = inner2.idml.clone();
            let fsfuts = {
                let guard = inner2.filesystems.lock().unwrap();
                guard.iter()
                    .map(move |(tree_id, itree)| {
                        let inner5 = inner4.clone();
                        let tree_id2 = *tree_id;
                        itree.flush(txg)
                            .and_then(move |tod| {
                                inner5.forest.insert(tree_id2, tod, txg)
                            })
                    }).collect::<Vec<_>>()
            };
            future::join_all(fsfuts)
            .and_then(move |_| idml2.sync_all(txg))
            .and_then(move |_| inner2.write_label(txg))
            .and_then(move |_| idml3.sync_all(txg))
        })
    }

    /// Perform a read-write operation on a Filesystem
    ///
    /// All operations conducted by the supplied closure will be completed
    /// within the same Pool transaction group.  Thus, after a power failure and
    /// recovery, either all will have completed, or none will have.
    pub fn fswrite<F, B, R>(&self, tree_id: TreeID, f: F)
        -> impl Future<Item = R, Error = Error>
        where F: FnOnce(ReadWriteFilesystem) -> B ,
              B: Future<Item = R, Error = Error>,
    {
        let inner = self.inner.clone();
        self.inner.idml.txg()
            .map_err(|_| Error::Sys(errno::Errno::EPIPE))
            .and_then(move |txg| {
                let ds = inner.rw_filesystem(tree_id, *txg);
                f(ds)
            })
    }
}


// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {
    use super::*;
    use common::cache_mock::CacheMock as Cache;
    use common::dml::*;
    use common::ddml::DRP;
    use common::ddml_mock::DDMLMock as DDML;
    use futures::future;
    use simulacrum::validators::trivial::any;
    use std::sync::Mutex;
    use tokio::runtime::current_thread;

    #[ignore = "Simulacrum can't mock a single generic method with different type parameters more than once in the same test https://github.com/pcsm/simulacrum/issues/55"]
    #[test]
    fn sync_transaction() {
        let cache = Cache::new();
        let mut ddml = DDML::new();
        ddml.expect_put::<Arc<tree::Node<RID, FSKey, FSValue>>>()
            .called_any()
            .returning(move |(_, _, _)| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::new(future::ok::<DRP, Error>(drp))
            });
        // Can't set this expectation, because RidtEntry isn't pubic
        //ddml.expect_put::<Arc<tree::Node<DRP, RID, RidtEntry>>>()
            //.called_any()
            //.returning(move |(_, _, _)| {
                //let drp = DRP::random(Compression::None, 4096);
                 //Box::new(future::ok::<DRP, Error>(drp)))
            //});
        ddml.expect_put::<Arc<tree::Node<DRP, PBA, RID>>>()
            .called_any()
            .returning(move |(_, _, _)| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::new(future::ok::<DRP, Error>(drp))
            });
        ddml.expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        ddml.expect_write_label()
            .called_once()
            .with(any())
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        ddml.expect_sync_all()
            .called_once()
            .with(TxgT::from(0))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let db = Database::create(Arc::new(idml));
        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(db.sync_transaction()).unwrap();
    }
}
