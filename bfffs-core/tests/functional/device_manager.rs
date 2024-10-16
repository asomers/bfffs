// vim: tw=80

/// Constructs a real filesystem and tests the common FS routines, without
/// mounting
mod device_manager {
    use bfffs_core::{
        Error,
        Uuid,
        database::*,
        device_manager::*,
        cache::*,
        ddml::*,
        idml::*
    };
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use rstest_reuse::{apply, template};
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex}
    };
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    type Harness = (Runtime, DevManager, Vec<PathBuf>, TempDir);

    fn harness(n: usize, m: usize, k: i16, f: i16, cs: Option<usize>,
               wb: Option<usize>)
        -> Harness
    {
        let (tempdir, paths, pool) = crate::PoolBuilder::new()
            .disks(n)
            .mirror_size(m)
            .stripe_size(k)
            .redundancy_level(f)
            .build();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
        let ddml = Arc::new(DDML::new(pool, cache.clone()));
        let idml = Arc::new(IDML::create(ddml, cache));
        rt.block_on( async {
            let db = Database::create(idml);
            db.sync_transaction().await
        }).unwrap();
        let mut dev_manager = DevManager::default();
        if let Some(cs) = cs {
            dev_manager.cache_size(cs);
        }
        if let Some(wb) = wb {
            dev_manager.writeback_size(wb);
        }
        (rt, dev_manager, paths, tempdir)
    }

    #[template]
    #[rstest(h,
             case(harness(1, 1, 1, 0, None, None)), // Single-disk configuration
             case(harness(2, 2, 1, 0, None, None)), // RAID1
             case(harness(3, 1, 3, 1, None, None)), // RAID5 configuration
             case(harness(6, 2, 3, 1, None, None)), // RAID51 configuration
     )]
    fn all_configs(h: Harness) {}

    /// No disks have been tasted
    #[apply(all_configs)]
    fn empty(h: Harness) {
        assert!(h.1.importable_pools().is_empty());
    }

    #[rstest(h, case(harness(1, 1, 1, 0, Some(100_000_000), None)))]
    fn cache_size(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        let db = rt.block_on(async move {
            dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
            dm.import_by_name("functional_test_pool").await
        }).unwrap();
        assert_eq!(db.cache_size(), 100_000_000);
    }

    /// Import a single pool by its name.  Try both single-disk and raid pools
    #[apply(all_configs)]
    fn import_by_name(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        rt.block_on(async move {
            for path in paths.iter() {
                dm.taste(path).await.unwrap();
            }
            dm.import_by_name("functional_test_pool").await.unwrap();
        })
    }

    /// Fail to import a nonexistent pool by name
    #[rstest(h, case(harness(1, 1, 1, 0, None, None)))]
    fn import_by_name_enoent(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        let e = rt.block_on(async move {
            dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
            dm.import_by_name("does_not_exist").await
        }).err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }


    /// Import a single pool by its UUID
    #[apply(all_configs)]
    fn import_by_uuid(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        rt.block_on(async move {
            for path in paths.iter() {
                dm.taste(path).await.unwrap();
            }
            let (name, uuid) = dm.importable_pools().pop().unwrap();
            assert_eq!(name, "functional_test_pool");
            dm.import_by_uuid(uuid).await.unwrap();
        });
    }

    /// Fail to import a nonexistent pool by UUID
    #[rstest(h, case(harness(1, 1, 1, 0, None, None)))]
    fn import_by_uuid_enoent(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        let e = rt.block_on(async move {
            dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
            dm.import_by_uuid(Uuid::new_v4()).await
        }).err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    /// DeviceManager::import_clusters on a single pool
    #[apply(all_configs)]
    fn import_clusters(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        let clusters = rt.block_on(async move {
            for path in paths.iter() {
                dm.taste(path).await?;
            }
            let (name, uuid) = dm.importable_pools().pop().unwrap();
            assert_eq!(name, "functional_test_pool");
            dm.import_clusters(uuid).await
        }).unwrap();
        assert_eq!(clusters.len(), 1);
    }

    /// DeviceManager::import_clusters for a nonexistent pool
    #[rstest(h, case(harness(1, 1, 1, 0, None, None)))]
    fn import_clusters_enoent(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        let e = rt.block_on(async move {
            dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
            dm.import_clusters(Uuid::new_v4()).await
        }).err().unwrap();
        assert_eq!(e, Error::ENOENT);
    }

    #[rstest(h, case(harness(1, 1, 1, 0, None, Some(100_000_000))))]
    fn writeback_size(h: Harness) {
        let (rt, dm, paths, _tempdir) = h;
        let db = rt.block_on(async move {
            dm.taste(paths.into_iter().next().unwrap()).await.unwrap();
            dm.import_by_name("functional_test_pool").await
        }).unwrap();
        assert_eq!(db.writeback_size(), 100_000_000);
    }
}
