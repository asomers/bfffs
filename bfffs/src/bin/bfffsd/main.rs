// vim: tw=80

use std::{
    fs::Permissions,
    os::unix::{fs::PermissionsExt, io::RawFd},
    path::{Path, PathBuf},
    process::exit,
    sync::Arc,
};

use bfffs_core::{
    controller::Controller,
    device_manager::DevManager,
    property::{Property, PropertyName},
    rpc,
    Error,
    Result,
};
use cfg_if::cfg_if;
use clap::{crate_version, Parser};
use fuse3::{
    raw::{MountHandle, Session},
    MountOptions,
};
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    TryFutureExt,
    TryStreamExt,
};
use nix::{
    fcntl::{open, OFlag},
    sys::stat::Mode,
    unistd,
};
use tokio_seqpacket::{UCred, UnixSeqpacket, UnixSeqpacketListener};
use tracing::{error, warn};
use tracing_subscriber::EnvFilter;

mod fs;

use crate::fs::FuseFs;

#[derive(Parser, Clone, Debug)]
#[clap(version = crate_version!())]
struct Cli {
    // TODO: configurable log level
    /// Mount options, comma delimited.  Apply to all BFFFS mounts
    #[clap(
        short = 'o',
        long,
        require_value_delimiter(true),
        value_delimiter(',')
    )]
    options:   Vec<String>,
    #[clap(long, default_value = "/var/run/bfffsd.sock")]
    sock:      PathBuf,
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    devices:   Vec<String>,
}

/// bfffsd's communications socket
struct Socket {
    sockpath: PathBuf,
    listener: UnixSeqpacketListener,
    _lockfd:  RawFd,
}

impl Drop for Socket {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ignore = std::fs::remove_file(&self.sockpath);
            self.sockpath.set_extension("lock");
            let _ignore = std::fs::remove_file(&self.sockpath);
        }
    }
}

impl Socket {
    fn new(path: &Path) -> Self {
        let sockpath = path.to_owned();
        let mut lockaddr = path.to_owned();
        lockaddr.set_extension("lock");
        let _lockfd = open(
            &lockaddr,
            OFlag::O_EXLOCK | OFlag::O_RDWR | OFlag::O_CREAT,
            Mode::from_bits(0o600).unwrap(),
        )
        .unwrap_or_else(|_| {
            eprintln!("Could not obtain lockfile");
            std::process::exit(1);
        });
        let _ignore_result = std::fs::remove_file(path);
        let listener = UnixSeqpacketListener::bind(path).unwrap();
        std::fs::set_permissions(path, Permissions::from_mode(0o666)).unwrap();
        Socket {
            sockpath,
            listener,
            _lockfd,
        }
    }
}

struct Bfffsd {
    controller:   Controller,
    _dev_manager: DevManager,
    mount_opts:   MountOptions,
}

impl Bfffsd {
    async fn handle_client(self: Arc<Self>, peer: UnixSeqpacket) {
        const BUFSIZ: usize = 4096;
        let mut buf = vec![0u8; BUFSIZ];

        loop {
            let nread = peer.recv(&mut buf).await.unwrap();
            if nread == 0 {
                // Client disconnected normally
                break;
            } else if nread >= BUFSIZ {
                warn!("Client sent unexpectedly large request");
                break;
            } else {
                buf.truncate(nread);
                let req: rpc::Request = bincode::deserialize(&buf[..]).unwrap();
                let creds = peer.peer_cred().unwrap();
                let resp = self.process_rpc(req, creds).await;
                let encoded: Vec<u8> = bincode::serialize(&resp).unwrap();
                let nwrite = peer.send(&encoded).await;
                if nwrite.is_err() || nwrite.unwrap() != encoded.len() {
                    warn!("Client disconnected before reading response");
                    break;
                }
            }
            // XXX The resize operation can be eliminated after
            // tokio-seqpacket-rs gains support for Rust's read_buf feature.
            buf.resize(BUFSIZ, 0);
        }
    }

    async fn new(cli: Cli) -> Self {
        let mut cache_size: Option<usize> = None;
        let mut writeback_size: Option<usize> = None;

        let mut mount_opts = MountOptions::default();
        mount_opts.fs_name("bfffs");
        if nix::unistd::getuid().is_root() {
            mount_opts.allow_other(true);
            mount_opts.default_permissions(true);
        }
        mount_opts.no_open_support(true);
        mount_opts.no_open_dir_support(true);
        // Unconditionally disable the kernel's buffer cache; BFFFS has its own
        mount_opts.custom_options("direct_io");
        for o in cli.options.iter() {
            if let Some((name, value)) = o.split_once('=') {
                if name == "cache_size" {
                    let v = value.parse().unwrap_or_else(|_| {
                        eprintln!("cache_size must be numeric");
                        exit(2);
                    });
                    cache_size = Some(v);
                    continue;
                } else if name == "writeback_size" {
                    let v = value.parse().unwrap_or_else(|_| {
                        eprintln!("writeback_size must be numeric");
                        exit(2);
                    });
                    writeback_size = Some(v);
                    continue;
                }
                // else, must be a mount_fusefs option
            }
            // Must be a mount_fusefs option
            mount_opts.custom_options(o);
        }

        let mut dev_manager = DevManager::default();
        if let Some(cs) = cache_size {
            dev_manager.cache_size(cs);
        }
        if let Some(wbs) = writeback_size {
            dev_manager.writeback_size(wbs);
        }

        for dev in cli.devices.iter() {
            // TODO: taste devices in parallel
            dev_manager.taste(dev).await.unwrap();
        }

        let uuid = dev_manager
            .importable_pools()
            .iter()
            .find(|(name, _uuid)| **name == cli.pool_name)
            .unwrap_or_else(|| {
                eprintln!("error: pool {} not found", cli.pool_name);
                std::process::exit(1);
            })
            .1;
        let db = dev_manager.import_by_uuid(uuid).await.unwrap();
        let controller = Controller::new(db);

        Bfffsd {
            controller,
            _dev_manager: dev_manager,
            mount_opts,
        }
    }

    #[tracing::instrument(skip(self))]
    #[cfg_attr(test, allow(unused_variables))]
    async fn mount(&self, name: String) -> Result<MountHandle> {
        let mo2 = self.mount_opts.clone();
        let mp = self
            .controller
            .get_prop(name.clone(), PropertyName::Mountpoint)
            .map_ok(|(prop, _source)| PathBuf::from(prop.as_str()))
            .await?;
        tracing::debug!("mounting {:?}", mp);
        cfg_if! {
            if #[cfg(test)] {
                Session::new(mo2).mount(FuseFs::default(), mp)
                    .map_err(Error::from)
                    .await
            } else {
                self.controller.new_fs(&name)
                    .and_then(|fs| {
                        let fusefs = FuseFs::new(fs);
                        Session::new(mo2).mount(fusefs, mp)
                            .map_err(|e| {
                                tracing::debug!("mount failed: {}", e);
                                Error::from(e)
                            })
                    })
                .await
            }
        }
    }

    async fn process_rpc(
        &self,
        req: rpc::Request,
        creds: UCred,
    ) -> rpc::Response {
        match req {
            rpc::Request::DebugDropCache => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::FsMount(Err(Error::EPERM))
                } else {
                    self.controller.drop_cache();
                    rpc::Response::DebugDropCache(Ok(()))
                }
            }
            rpc::Request::FsCreate(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::FsMount(Err(Error::EPERM))
                } else {
                    let r = self
                        .controller
                        .create_fs(&req.name)
                        .and_then(|tree_id| {
                            req.props
                                .into_iter()
                                .map(|prop| {
                                    self.controller.set_prop(&req.name, prop)
                                })
                                .collect::<FuturesUnordered<_>>()
                                .try_collect::<Vec<_>>()
                                .map_ok(move |_| tree_id)
                        })
                        .await;
                    rpc::Response::FsCreate(r)
                }
            }
            rpc::Request::FsDestroy(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::FsMount(Err(Error::EPERM))
                } else {
                    let r = self.controller.destroy_fs(&req.name).await;
                    rpc::Response::FsDestroy(r)
                }
            }
            rpc::Request::FsList(req) => {
                // this value of chunkqty is a guess, not well-calculated
                const CHUNKQTY: usize = 64;

                let r = self
                    .controller
                    .list_fs(&req.name, req.offset)
                    .try_chunks(CHUNKQTY)
                    .try_next()
                    .await;
                let r = match r {
                    Ok(Some(v)) => {
                        // It's tempting to move the get_prop call into an
                        // and_then method after list_fs.  But it doesn't work
                        // due to a Rust lifetime analysis bug.
                        // https://github.com/rust-lang/rust/issues/64552
                        v.into_iter()
                            .map(|de| {
                                req.props
                                    .iter()
                                    .map(|propname| {
                                        let name = de.name.clone();
                                        self.controller
                                            .get_prop(name, *propname)
                                    })
                                    .collect::<FuturesOrdered<_>>()
                                    .try_collect::<Vec<_>>()
                                    .map_ok(move |props| {
                                        rpc::fs::DsInfo {
                                            name: de.name,
                                            props,
                                            offset: de.offs,
                                        }
                                    })
                            })
                            .collect::<FuturesOrdered<_>>()
                            .try_collect::<Vec<_>>()
                            .await
                    }
                    Ok(None) => Ok(vec![]),
                    Err(tce) => Err(tce.1),
                };
                rpc::Response::FsList(r)
            }
            rpc::Request::FsMount(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::FsMount(Err(Error::EPERM))
                } else {
                    match self.mount(req.name).await {
                        Ok(_) => rpc::Response::FsMount(Ok(())),
                        Err(e) => {
                            error!("mount: {:?}", e);
                            rpc::Response::FsMount(Err(e))
                        }
                    }
                }
            }
            rpc::Request::FsSet(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::FsSet(Err(Error::EPERM))
                } else {
                    match self.set(&req.name, req.props).await {
                        Ok(_) => rpc::Response::FsSet(Ok(())),
                        Err(e) => {
                            error!("set: {:?}", e);
                            rpc::Response::FsSet(Err(e))
                        }
                    }
                }
            }
            rpc::Request::FsStat(req) => {
                let r = req
                    .props
                    .iter()
                    .map(|propname| {
                        let name = req.name.clone();
                        self.controller.get_prop(name, *propname)
                    })
                    .collect::<FuturesOrdered<_>>()
                    .try_collect::<Vec<_>>()
                    .map_ok(move |props| {
                        rpc::fs::DsInfo {
                            name: req.name,
                            props,
                            offset: 0,
                        }
                    })
                    .await;
                rpc::Response::FsStat(r)
            }
            rpc::Request::FsUnmount(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::FsUnmount(Err(Error::EPERM))
                } else {
                    match self.unmount(&req.name, req.force).await {
                        Ok(_) => rpc::Response::FsUnmount(Ok(())),
                        Err(e) => {
                            error!("unmount: {:?}", e);
                            rpc::Response::FsUnmount(Err(e))
                        }
                    }
                }
            }
            rpc::Request::PoolClean(req) => {
                if creds.uid() != unistd::geteuid().as_raw() {
                    rpc::Response::PoolClean(Err(Error::EPERM))
                } else {
                    let r = self.controller.clean(&req.pool).map(drop);
                    rpc::Response::PoolClean(r)
                }
            }
        }
    }

    async fn run(self: Arc<Self>, mut sock: Socket) {
        loop {
            let peer = sock.listener.accept().await.unwrap();
            tokio::spawn(self.clone().handle_client(peer));
        }
    }

    async fn set(&self, name: &str, props: Vec<Property>) -> Result<()> {
        for prop in props.into_iter() {
            self.controller.set_prop(name, prop).await?;
        }
        Ok(())
    }

    async fn unmount(&self, name: &str, force: bool) -> Result<()> {
        self.controller.unmount(name, force).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli: Cli = Cli::parse();

    let sock = Socket::new(&cli.sock);
    let bfffsd = Arc::new(Bfffsd::new(cli).await);

    bfffsd.run(sock).await;
}

#[cfg(test)]
mod t {
    use clap::ErrorKind::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Vec::new())]
    #[case(vec!["bfffsd"])]
    #[case(vec!["bfffsd", "testpool"])]
    fn missing_arg(#[case] args: Vec<&str>) {
        let e = Cli::try_parse_from(args).unwrap_err();
        assert!(
            e.kind() == MissingRequiredArgument ||
                e.kind() == DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn options() {
        let args = vec![
            "bfffsd",
            "-o",
            "allow_other,default_permissions",
            "testpool",
            "--sock",
            "/tmp/bfffs.sock",
            "/dev/da0",
        ];
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(cli.pool_name, "testpool");
        assert_eq!(cli.sock, Path::new("/tmp/bfffs.sock"));
        assert_eq!(cli.options, vec!["allow_other", "default_permissions"]);
        assert_eq!(cli.devices[0], "/dev/da0");
    }

    #[test]
    fn plain() {
        let args = vec!["bfffsd", "testpool", "/dev/da0"];
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(cli.pool_name, "testpool");
        assert_eq!(cli.sock, Path::new("/var/run/bfffsd.sock"));
        assert!(cli.options.is_empty());
        assert_eq!(cli.devices[0], "/dev/da0");
    }
}
