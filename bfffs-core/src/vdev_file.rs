// vim: tw=80

use crate::{
    label::*,
    types::*,
    util::*,
    vdev::*
};
use atomic_enum::atomic_enum;
use cfg_if::cfg_if;
use divbuf::DivBuf;
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    future,
    task::{Context, Poll}
};
#[cfg(test)] use mockall::mock;
use nix::libc::{c_int, off_t};
use pin_project::pin_project;
use std::{
    borrow::Borrow,
    fs::{self, OpenOptions},
    io::{self, IoSlice, IoSliceMut},
    mem::{self, MaybeUninit},
    num::NonZeroU64,
    os::{
        fd::AsFd,
        unix::{
            fs::{FileTypeExt, OpenOptionsExt},
            io::{AsRawFd, BorrowedFd, RawFd}
        },
    },
    path::Path,
    pin::Pin,
    sync::atomic::Ordering
};
use tokio_file::AioFileExt;
use tokio::task;

/// How does this device deallocate sectors?
#[atomic_enum]
enum EraseMethod {
    None,
    Diocgdelete,
    #[cfg(have_fspacectl)]
    Fspacectl,
    #[cfg(have_fspacectl)]
    MaybeFspacectl,
}

impl AtomicEraseMethod {
    /// Get the initial erase method.
    fn initial(fd: RawFd) -> Result<Self> {
        if VdevFile::candelete(fd)? {
            // The file supports DIOCGDELETE.
            Ok(AtomicEraseMethod::new(EraseMethod::Diocgdelete))
        } else {
            cfg_if! {
                if #[cfg(have_fspacectl)] {
                    // The file does not support DIOCGDELETE.  Optimistically
                    // guess that it supports fspacectl.
                    Ok(AtomicEraseMethod::new(EraseMethod::MaybeFspacectl))
                } else {
                    Ok(AtomicEraseMethod::new(EraseMethod::None))
                }
            }
        }
    }
}

/// FFI definitions that don't belong in libc.  The ioctls can't go in libc
/// because they use Nix's macros.  The structs probably shouldn't go in libc,
/// because they're not really intended to be a stable interface.
#[doc(hidden)]
mod ffi {
    use nix::{ ioctl_readwrite, ioctl_write_ptr, libc::{c_int, off_t} };
    const DISK_IDENT_SIZE: usize = 256;

    #[repr(C)]
    #[doc(hidden)]
    pub union diocgattr_arg_value {
        pub c_str:  [u8; DISK_IDENT_SIZE],
        pub off: off_t,
        pub i: c_int,
        pub c_u16: u16
    }

    #[repr(C)]
    #[doc(hidden)]
    // This should be pub(super), but it must be plain pub instead because Nix's
    // ioctl macros make the ioctl functions `pub`.
    pub struct diocgattr_arg {
        pub name: [u8; 64],
        pub len: c_int,
        pub value: diocgattr_arg_value
    }

    ioctl_readwrite! {
        #[doc(hidden)]
        diocgattr, b'd', 142, diocgattr_arg
    }

    nix::ioctl_read! {
        /// get the size of the entire device in bytes.  this should be a
        /// multiple of the sector size.
        diocgmediasize, 'd', 129, nix::libc::off_t
    }

    ioctl_write_ptr! {
        /// FreeBSD's catch-all ioctl for hole-punching, TRIM, UNMAP,
        /// Deallocate, and EraseWritePointer of GEOM devices.
        #[doc(hidden)]
        diocgdelete, b'd', 136, [off_t; 2]
    }

    nix::ioctl_read! {
        diocgsectorsize, 'd', 128, nix::libc::c_uint
    }

    nix::ioctl_read! {
        diocgstripesize, 'd', 139, nix::libc::off_t
    }

}

use ffi::{diocgdelete, diocgattr, diocgattr_arg, diocgmediasize, diocgsectorsize, diocgstripesize};


/// `VdevFile`: File-backed implementation of `VdevBlock`
///
/// This is used by the FUSE implementation of BFFFS.  It works with both
/// regular files and device files.
///
/// I/O operations on `VdevFile` happen immediately; they are not scheduled.
///
#[derive(Debug)]
pub struct VdevFile<'fd> {
    fd:             BorrowedFd<'fd>,
    /// Number of reserved LBAS in first zone for each spacemap
    spacemap_space: LbaT,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    size:           LbaT,
    /// How does the underlying file deallocate data?
    erase_method:   AtomicEraseMethod,
}

impl<'fd> Vdev for VdevFile<'fd> {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        if lba >= self.reserved_space() {
            Some((lba / self.lbas_per_zone) as ZoneT)
        } else {
            None
        }
    }

    fn optimum_queue_depth(&self) -> u32 {
        // The value `10` is just a total guess.
        10
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn sync_all<'a>(&'a self) -> Pin<Box<dyn futures::Future<Output = Result<()>> + Send + Sync + 'a>>
    //fn sync_all<'a>(&'a self) ->  impl Future<Output = Result<()>> + Send + Sync + 'a
    {
        let fut = AioFileExt::sync_all(&self.fd).unwrap()
            .map_ok(drop)
            .map_err(Error::from);
        //Box::pin(fut) as Pin<Box<dyn futures::Future<Output = Result<()>> + Send + Sync + 'fd>>
        Box::pin(fut)
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        if zone == 0 {
            (self.reserved_space(), self.lbas_per_zone)
        } else {
            (u64::from(zone) * self.lbas_per_zone,
             u64::from(zone + 1) * self.lbas_per_zone)
        }
    }

    fn zones(&self) -> ZoneT {
        div_roundup(self.size, self.lbas_per_zone) as ZoneT
    }
}

impl<'fd> VdevFile<'fd> {
    /// Size of a simulated zone
    const DEFAULT_LBAS_PER_ZONE: LbaT = 1 << 16;  // 256 MB

    fn candelete(fd: RawFd) -> Result<bool> {
        let mut arg = MaybeUninit::<diocgattr_arg>::uninit();
        let r = unsafe {
            let p = arg.as_mut_ptr();
            (*p).name[0..16].copy_from_slice(b"GEOM::candelete\0");
            (*p).len = mem::size_of::<c_int>() as c_int;
            diocgattr(fd, p)
        }.map(|_| unsafe { arg.assume_init().value.i } != 0)
        .map_err(Error::from);
        if r == Err(Error::ENOTTY) {
            // This vdev doesn't support DIOCGATTR, so it must not support
            // DIOCGDELETE either.
            Ok(false)
        } else {
            r
        }
    }

    /// Create a new Vdev, backed by a file
    ///
    /// * `path`:           Pathname for the file.  It may be a device node.
    /// * `lbas_per_zone`:  If specified, this many LBAs will be assigned to
    ///                     simulated zones on devices that don't have native
    ///                     zones.
    pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
        -> io::Result<Self>
        where P: AsRef<Path>
    {
        let pb = path.as_ref().to_path_buf();
        let mut vdev = VdevFile::new(pb).unwrap();
        if let Some(x) = lbas_per_zone {
            vdev.lbas_per_zone = x.get();
        }
        Ok(vdev)
    }

    pub fn create2<P>(path: P, f: &fs::File, lbas_per_zone: Option<NonZeroU64>)
        -> io::Result<Self>
        where P: AsRef<Path>
    {
        let pb = path.as_ref().to_path_buf();
        todo!()
    }


    /// Asynchronously erase the given zone.
    ///
    /// After this, the zone will be in the empty state.  The data may or may
    /// not be inaccessible, and should not be considered securely erased.
    ///
    /// # Parameters
    ///
    /// - `lba`: The first LBA of the zone to erase
    /// - `end`: The last LBA of the zone to erase
    // There isn't (yet) a way to asynchronously trim, so use a synchronous
    // method in a blocking_task
    pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let off = start as off_t * (BYTES_PER_LBA as off_t);
        let len = (end + 1 - start) as off_t * BYTES_PER_LBA as off_t;
        let em = self.erase_method.load(Ordering::Relaxed);
        match em {
            EraseMethod::None => Box::pin(future::ok(())),
            EraseMethod::Diocgdelete => {
                let fd = self.fd.as_raw_fd();
                let t = task::spawn_blocking(move || {
                    let args = [off, len];
                    unsafe {
                        diocgdelete(fd, &args)
                    }.map(drop)
                }).map(std::result::Result::unwrap)
                .map_err(Error::from);
                Box::pin(t)
            },
            #[cfg(have_fspacectl)]
            EraseMethod::MaybeFspacectl => {
                use nix::fcntl::fspacectl_all;

                // The first time erasing a zone, do it synchronously so we can
                // change the erase_method.
                match fspacectl_all(&self.fd, off, len) {
                    Err(nix::Error::ENOSYS) =>  {
                        // fspacectl is not supported by the running system
                        self.erase_method.store(EraseMethod::None,
                                                Ordering::Relaxed);
                        Box::pin(future::ok(()))
                    },
                    Err(nix::Error::ENODEV) => {
                        // fspacectl is not supported by this file
                        self.erase_method.store(EraseMethod::None,
                                                Ordering::Relaxed);
                        Box::pin(future::ok(()))
                    },
                    Ok(()) => {
                        // fspacectl is definitely supported
                        self.erase_method.store(EraseMethod::Fspacectl,
                                                Ordering::Relaxed);
                        Box::pin(future::ok(()))
                    },
                    Err(e) => Box::pin(future::err(e.into()))
                }
            },
            #[cfg(have_fspacectl)]
            EraseMethod::Fspacectl => {
                use nix::fcntl::fspacectl_all;

                let t = task::spawn_blocking(move || {
                    fspacectl_all(&self.fd, off, len)
                }).map(std::result::Result::unwrap)
                .map_err(Error::from);
                Box::pin(t)
            }
        }
    }

    /// Asynchronously finish the given zone.
    ///
    /// After this, the zone will be in the Full state and writes will not be
    /// allowed.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to finish
    pub fn finish_zone(&self, _lba: LbaT) -> BoxVdevFut {
        // ordinary files don't have Zone operations
        Box::pin(future::ok(()))
    }

    pub fn lbas_per_zone(&self) -> LbaT{
        self.lbas_per_zone
    }

    /// Get the file's size in bytes, whether it's a device file or a regular
    /// file
    fn devlen(f: &fs::File, sectorsize: usize) -> io::Result<u64> {
        let md = f.metadata()?;
        if sectorsize > 1 {
            let mut mediasize = mem::MaybeUninit::<nix::libc::off_t>::uninit();
            // This ioctl is always safe
            unsafe {
                diocgmediasize(f.as_raw_fd(), mediasize.as_mut_ptr())
            }.map_err(|_| io::Error::from_raw_os_error(nix::errno::Errno::last_raw()))?;
            // Safe because we know the ioctl succeeded
            unsafe { Ok(mediasize.assume_init() as u64) }
        } else {
            Ok(md.len())
        }
    }

    /// Partially construct a VdevFile.
    fn new(pb: std::path::PathBuf) -> Result<Self> {
        todo!()
        // NB: Annoyingly, using O_EXLOCK without O_NONBLOCK means that we can
        // block indefinitely.  However, using O_NONBLOCK is worse because it
        // can cause spurious failures, such as when another thread fork()s.
        // That happens frequently in the functional tests.
        //let f = OpenOptions::new()
            //.read(true)
            //.write(true)
            //.custom_flags(libc::O_DIRECT | libc::O_EXLOCK)
            //.open(pb.as_path())?;
        //let md = f.metadata()?;
        //let ft = md.file_type();
        //// The preferred (not necessarily minimum) sector size for accessing
        //// the device
        //let sectorsize = if ft.is_block_device() || ft.is_char_device() {
            //let mut sectorsize = mem::MaybeUninit::<u32>::uninit();
            //let mut stripesize = mem::MaybeUninit::<nix::libc::off_t>::uninit();
            //let fd = f.as_raw_fd();
            //unsafe {
                //// TODO: use stripesize if it's greater than sector size
                //diocgsectorsize(fd, sectorsize.as_mut_ptr())?;
                //diocgstripesize(fd, stripesize.as_mut_ptr())?;
                //if stripesize.assume_init() > 0 {
                    //stripesize.assume_init() as usize
                //} else {
                    //sectorsize.assume_init() as usize
                //}
            //}
        //} else {
            //1
        //};
        //let erase_method = AtomicEraseMethod::initial(f.as_raw_fd())?;
        //let size = Self::devlen(&f, sectorsize)? / BYTES_PER_LBA as u64;
        //let lbas_per_zone = VdevFile::DEFAULT_LBAS_PER_ZONE;
        //let nzones = div_roundup(size, lbas_per_zone);
        //let spacemap_space = spacemap_space(nzones);
        //let vdev = VdevFile {
            //erase_method,
            //file: f,
            //lbas_per_zone,
            //path: pb,
            //size,
            //spacemap_space,
            //uuid: Uuid::default()
        //};
        //Ok(vdev)
    }

    pub fn new2(f: &'fd fs::File) -> Self
    {
        todo!()
    }

    /// Open a Vdev, backed by a file.
    ///
    /// * `path`:           Pathname for the file.  It may be a device node.
    //pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // NB: Annoyingly, using O_EXLOCK without O_NONBLOCK means that we can
        // block indefinitely.  However, using O_NONBLOCK is worse because it
        // can cause spurious failures, such as when another thread fork()s.
        // That happens frequently in the functional tests.
        //let stdfile = OpenOptions::new()
            //.read(true)
            //.write(true)
            //.custom_flags(libc::O_DIRECT | libc::O_EXLOCK)
            //.open(path)
            //.map_err(|e| Error::from_i32(e.raw_os_error().unwrap()).unwrap())?;
        //let f = File::new(stdfile);
        //let lbas_per_zone = VdevFile::DEFAULT_LBAS_PER_ZONE;
        //let size = f.len().unwrap() / BYTES_PER_LBA as u64;
        //let nzones = div_roundup(size, lbas_per_zone);
        //let spacemap_space = spacemap_space(nzones);
        //let erase_method = AtomicEraseMethod::initial(f.as_raw_fd())?;
        //let vdev = VdevFile {
            //file: f,
            //spacemap_space,
            //lbas_per_zone,
            //size,
            //erase_method
        //};
        //Ok(vdev)
    //}

    /// Returns both a new `VdevFile` object, and a `LabelReader` that may be
    /// used to construct other vdevs stacked on top of this one.
    ///
    /// * `path`    Pathname for the file.  It may be a device node.
    //pub async fn open<P: AsRef<Path>>(path: P)
        //-> Result<(Self, LabelReader)>
    //{
        //let pb = path.as_ref().to_path_buf();
        //let mut vdev = Self::new(pb)?;
        //let mut label_reader = VdevFile::read_label(&vdev.file).await?;
        //let label: Label = label_reader.deserialize().unwrap();
        //assert!(vdev.size >= label.lbas,
                //"Vdev has shrunk since creation");
        //vdev.spacemap_space = label.spacemap_space;
        //vdev.lbas_per_zone = label.lbas_per_zone;
        //vdev.size = label.lbas;
        //vdev.uuid = label.uuid;
        //Ok((vdev, label_reader))
    //}

    pub async fn open2(pb: std::path::PathBuf, f: &fs::File)
        -> Result<(Self, LabelReader)>
    {
        todo!()
    }

    /// Asynchronously open the given zone.
    ///
    /// This should be called on an empty zone before writing to that zone.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to open
    pub fn open_zone(&self, _lba: LbaT) -> BoxVdevFut {
        // ordinary files don't have Zone operations
        Box::pin(future::ok(()))
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    pub fn read_at(&'fd self, mut buf: IoVecMut, lba: LbaT) -> ReadAt<'fd>
    //pub fn read_at(&self, mut buf: IoVecMut, lba: LbaT) -> impl Future<Output = Result<()>> + Send + Sync + 'fd
    {
        // Unlike write_at, the upper layers will never read into a buffer that
        // isn't a multiple of a block size.  DDML::read ensures that.
        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);

        let off = lba * (BYTES_PER_LBA as u64);
        let bufaddr: &'static mut [u8] = unsafe {
            // Safe because fut's lifetime will be equal to buf's once we move
            // it into the ReadAt struct.  Also, because buf is already
            // heap-allocated, moving it won't change the data's address.
            mem::transmute::<&mut[u8], &'static mut [u8]>(buf.as_mut())
        };
        let fut = self.fd.read_at(&mut *bufaddr, off).unwrap();
        ReadAt { _buf: buf, fut }
    }

    /// Read one of the spacemaps from disk.
    ///
    /// # Parameters
    /// - `buf`:        Place the still-serialized spacemap here.  `buf` will be
    ///                 resized as needed.
    /// * `lba`     LBA to read from
    pub fn read_spacemap(&'fd self, buf: IoVecMut, lba: LbaT) -> ReadAt<'fd>
    //pub fn read_spacemap(&'fd self, buf: IoVecMut, idx: u32) -> impl Future<Output = Result<()>> + Send + Sync + 'fd
    {
        self.read_at(buf, lba)
    }

    /// The asynchronous scatter/gather read function.
    ///
    /// * `sglist   Scatter-gather list of buffers to receive data
    /// * `lba`     LBA to read from
    #[allow(clippy::transmute_ptr_to_ptr)]  // Clippy false positive
    pub fn readv_at(&'fd self, mut sglist: SGListMut, lba: LbaT) -> ReadvAt<'fd>
    {
        for iovec in sglist.iter() {
            debug_assert_eq!(iovec.len() % BYTES_PER_LBA, 0);
        }
        let off = lba * (BYTES_PER_LBA as u64);
        let mut slices: Box<[IoSliceMut<'fd>]> = unsafe {
            // Safe because fut's lifetime will be equal to slices's once we
            // move it into the ReadvAt struct.  Also, because sglist is already
            // heap-allocated, so moving it won't change the data's address.
            sglist.iter_mut()
            .map(|b| {
                let sl = mem::transmute::<&mut [u8], &'fd mut[u8]>(&mut b[..]);
                IoSliceMut::new(sl)
            }).collect::<Vec<_>>()
            .into_boxed_slice()
        };
        let bufs: &'fd mut [IoSliceMut<'fd>] = unsafe {
            // Safe because fut's lifetime will be equal to bufs's once we
            // move it into the ReadvAt struct.  Also, because slies is already
            // heap-allocated, so moving it won't change the data's address.
            mem::transmute::<&mut[IoSliceMut<'fd>],
                             &'fd mut [IoSliceMut<'fd>]>(
                &mut slices
            )
        };
        let fut = self.fd.readv_at(bufs, off).unwrap();
        ReadvAt {
            _sglist: sglist,
            _slices: slices,
            fut
        }
    }

    fn reserved_space(&self) -> LbaT {
        LABEL_COUNT * (LABEL_LBAS + self.spacemap_space)
    }

    /// Size of a single serialized spacemap, in LBAs, rounded up.
    pub fn spacemap_space(&self) -> LbaT {
        self.spacemap_space
    }

    /// Set certain properties that are stored in the label
    ///
    /// * `lbas_per_zone`:  If specified, this many LBAs will be assigned to
    ///                     simulated zones on devices that don't have native
    ///                     zones.
    pub fn set(&mut self, size: LbaT, lbas_per_zone: LbaT) {
        self.size = size;
        let nzones = div_roundup(size, lbas_per_zone);
        self.spacemap_space = spacemap_space(nzones);
        self.lbas_per_zone = lbas_per_zone;
    }

    /// Asynchronously write a contiguous portion of the vdev.
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> WriteAt<'fd>
    //pub fn write_at(&'fd self, buf: IoVec, lba: LbaT) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'fd>>
    {
        assert!(lba >= self.reserved_space(), "Don't overwrite the labels!");
        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);
        self.write_at_unchecked(buf, lba)
        //Box::pin(self.write_at_unchecked(buf, lba))
    }

    // NB: functions like this don't submit to the kernel immediately with
    // aio_write.  They don't do that until polled.  So the return value's
    // lifetime must include both that of the BorrowedFd and self.
    fn write_at_unchecked<'a>(&'a self, buf: IoVec, lba: LbaT) -> WriteAt<'fd>
    //fn write_at_unchecked(&'fd self, buf: IoVec, lba: LbaT) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'fd>>
    {
        let off = lba * (BYTES_PER_LBA as u64);
        {
            let b: &[u8] = buf.borrow();
            debug_assert!(b.len() % BYTES_PER_LBA == 0);
        }

        // Safe because fut's lifetime is equal to buf's (or rather, it will
        // be once we move it into the WriteAt struct
        let sbuf: &'static [u8] = unsafe {
            mem::transmute::<&[u8], &'static [u8]>( buf.as_ref())
        };
        let fut = self.fd.write_at(sbuf, off).unwrap();
        // Drop the borrow on fut.  We don't actually need it, since three
        // levels downstack nix::sys::aio::AioCb::common_init does as_raw_fd().
        // Or to put it another way, tokio_file takes fd by value, so its return
        // value doesn't need to capture the lifetime of self.  But it's hard
        // for rustc to understand that.
        let fut = unsafe { mem::transmute::<_, tokio_file::WriteAt<'fd>>(fut) };
        //let fut = unsafe { mem::transmute::<tokio_file::WriteAt<'fd + '1>, tokio_file::WriteAt<'fd>>(fut) };

        static_assertions::assert_impl_all!(&std::os::unix::io::OwnedFd: Send);
        static_assertions::assert_impl_all!(WriteAt: Send);
        WriteAt { _buf:buf, fut }
        //Box::pin(WriteAt { _buf: buf, fut })
    }

    //fn write_at_unchecked2(&self, buf: IoVec, lba: LbaT) -> impl Future<Output = Result<()>> + Send + Sync
    //{
        //let off = lba * (BYTES_PER_LBA as u64);
        //{
            //let b: &[u8] = (*buf).borrow();
            //debug_assert!(b.len() % BYTES_PER_LBA == 0);
        //}

        //// Safe because fut's lifetime is equal to buf's (or rather, it will
        //// be once we move it into the WriteAt struct
        //let sbuf: &'static [u8] = unsafe {
            //mem::transmute::<&[u8], &'static [u8]>( buf.as_ref())
        //};
        //let fut = self.file.write_at(sbuf, off).unwrap();

        //WriteAt { _buf: buf, fut }
    //}

    /// Asynchronously write this Vdev's label.
    ///
    /// `label_writer` should already contain the serialized labels of every
    /// vdev stacked on top of this one.
    pub fn write_label(&'fd self, label_writer: LabelWriter) -> WritevAt<'fd>
    {
        let lba = label_writer.lba();
        let sglist = label_writer.into_sglist();
        let sglist = copy_and_pad_sglist(sglist);
        self.writev_at_unchecked(sglist, lba)
    }

    /// Asynchronously write to the Vdev's spacemap area.
    ///
    /// # Parameters
    ///
    /// - `sglist`:     Buffers of data to write
    /// * `lba`     LBA to write to
    pub fn write_spacemap(&'fd self, sglist: SGList, lba: LbaT)
        -> WritevAt<'fd>
    {
        let bytes: u64 = sglist.iter()
            .map(DivBuf::len)
            .sum::<usize>() as u64;
        debug_assert_eq!(bytes % BYTES_PER_LBA as u64, 0);
        let lbas = bytes / BYTES_PER_LBA as LbaT;
        assert!(lba + lbas <= self.reserved_space());
        self.writev_at_unchecked(sglist, lba)
    }

    /// The asynchronous scatter/gather write function.
    ///
    /// * `sglist`  Scatter-gather list of buffers to write
    /// * `lba`     LBA to write to
    pub fn writev_at(&'fd self, sglist: SGList, lba: LbaT) -> WritevAt<'fd>
    {
        assert!(lba >= self.reserved_space(), "Don't overwrite the labels!");
        self.writev_at_unchecked(sglist, lba)
    }

    fn writev_at_unchecked(&'fd self, sglist: SGList, lba: LbaT)
        -> WritevAt<'fd>
    {
        let off = lba * (BYTES_PER_LBA as u64);

        let slices: Box<[IoSlice<'static>]> =
            sglist.iter()
                .map(|b| {
                    debug_assert_eq!(b.len() % BYTES_PER_LBA, 0);
                    // Safe because fut's lifetime is equal to slices' (or
                    // rather, it will be once we move it into the WriteAt
                    // struct
                    let sb = unsafe {
                        mem::transmute::<&[u8], &'static [u8]>(&b[..])
                    };
                    IoSlice::new(sb)
                }).collect::<Vec<_>>()
                .into_boxed_slice();
        let fut = self.fd.writev_at(&slices, off).unwrap();

        WritevAt {
            _sglist: sglist,
            _slices: slices,
            fut
        }
    }
}

#[pin_project]
struct ReadAt<'a> {
    // Owns the buffer used by the Future
    _buf: IoVecMut,
    #[pin]
    fut: tokio_file::ReadAt<'a>
}

impl<'a> Future for ReadAt<'a> {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_aio_result)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

#[pin_project]
struct WriteAt<'fd> {
    #[pin]
    fut: tokio_file::WriteAt<'fd>,
    // Owns the buffer used by the Future
    _buf: IoVec,
}

impl<'fd> Future for WriteAt<'fd> {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_aio_result)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

#[pin_project]
struct ReadvAt<'fd> {
    #[pin]
    fut: tokio_file::ReadvAt<'fd>,
    // Owns the pointer array used by the Future
    _slices: Box<[IoSliceMut<'fd>]>,
    // Owns the buffers used by the Future
    _sglist: SGListMut,
}

impl<'fd> Future for ReadvAt<'fd> {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_l)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

#[pin_project]
struct WritevAt<'a> {
    #[pin]
    fut: tokio_file::WritevAt<'a>,
    // Owns the pointer array used by the Future
    _slices: Box<[IoSlice<'static>]>,
    // Owns the buffers used by the Future
    _sglist: SGList,
}

impl<'a> Future for WritevAt<'a> {
    type Output = Result<()>;

    // aio_write and friends will sometimes return an error synchronously (like
    // EAGAIN).  VdevBlock handles those errors synchronously by calling poll()
    // once before spawning the future into the event loop.  But that results in
    // calling poll again after it returns an error, which is incompatible with
    // FuturesExt::{map, map_err}'s implementations.  So we have to define a
    // custom poll method here, with map's and map_err's functionality inlined.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().fut.poll(cx) {
            Poll::Ready(Ok(_l)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::from(e))),
            Poll::Pending => Poll::Pending
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock!{
    pub VdevFile {
        #[mockall::concretize]
        pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path>;
        pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn finish_zone(&self, _lba: LbaT) -> BoxVdevFut;
        pub fn lbas_per_zone(&self) -> LbaT;
        #[mockall::concretize]
        pub async fn open<P>(path: P) -> Result<Self> where P: AsRef<Path>;
        pub fn open_zone(&self, _lba: LbaT) -> BoxVdevFut;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_spacemap(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
        pub fn set(&mut self, size: LbaT);
        pub fn spacemap_space(&self) -> LbaT;
        pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut;
        pub fn write_label(&self, mut label_writer: LabelWriter) -> BoxVdevFut;
        pub fn write_spacemap(&self, buf: SGList, lba: LbaT) -> BoxVdevFut;
        pub fn writev_at(&self, buf: SGList, lba: LbaT) -> BoxVdevFut;
    }
    impl Vdev for VdevFile {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> BoxVdevFut;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}
// LCOV_EXCL_STOP
