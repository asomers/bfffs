// vim: tw=80

//! COW B+-Trees, based on B-trees, Shadowing, and Clones[^CowBtrees]
//!
//! [^CowBtrees]: Rodeh, Ohad. "B-trees, shadowing, and clones." ACM Transactions on Storage (TOS) 3.4 (2008): 2.

use common::*;
use common::dml::*;
use futures::{
    Async,
    Future,
    future::{self, IntoFuture},
    Poll,
    stream::{self, Stream}
};
use futures_locks::*;
use nix::{Error, errno};
use serde::{Serializer, de::{Deserializer, DeserializeOwned}};
#[cfg(test)] use serde_yaml;
#[cfg(test)] use std::fmt::{self, Display, Formatter};
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::VecDeque,
    fmt::Debug,
    mem,
    rc::Rc,
    ops::{Bound, DerefMut, Range, RangeBounds},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    }
};
mod node;
use self::node::*;
// Node must be visible for the IDML's unit tests
pub(super) use self::node::Node;

mod atomic_usize_serializer {
    use super::*;
    use serde::Deserialize;

    pub fn deserialize<'de, D>(d: D) -> Result<AtomicUsize, D::Error>
        where D: Deserializer<'de>
    {
        usize::deserialize(d)
            .map(|u| AtomicUsize::new(u))
    }

    pub fn serialize<S>(x: &AtomicUsize, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        s.serialize_u64(x.load(Ordering::Relaxed) as u64)
    }
}

/// Uniquely identifies any Node in the Tree.
#[derive(Debug)]
struct NodeId<K: Key> {
    /// Tree level of the Node.  Leaves are 0.
    height: u8,
    /// Less than or equal to the Node's first child/item.  Greater than the
    /// previous Node's last child/item.
    key: K
}

mod tree_root_serializer {
    use super::*;
    use serde::{Deserialize, Serialize, ser::Error};

    pub(super) fn deserialize<'de, A, DE, K, V>(d: DE)
        -> Result<RwLock<IntElem<A, K, V>>, DE::Error>
        where A: Addr, DE: Deserializer<'de>, K: Key, V: Value
    {
        IntElem::deserialize(d)
            .map(|int_elem| RwLock::new(int_elem))
    }

    pub(super) fn serialize<A, K, S, V>(x: &RwLock<IntElem<A, K, V>>, s: S)
        -> Result<S::Ok, S::Error>
        where A: Addr, K: Key, S: Serializer, V: Value
    {
        match x.try_read() {
            Ok(guard) => (*guard).serialize(s),
            Err(_) => Err(S::Error::custom("EAGAIN"))
        }
    }
}

pub struct RangeQuery<'tree, A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A> + 'tree,
          K: Key + Borrow<T>,
          T: Ord + Clone + 'tree,
          V: Value
{
    /// If Some, then there are more nodes in the Tree to query
    cursor: Option<Bound<T>>,
    /// Data that can be returned immediately
    data: VecDeque<(K, V)>,
    end: Bound<T>,
    last_fut: Option<Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + 'tree>>,
    /// Handle to the tree
    tree: &'tree Tree<A, D, K, V>
}

impl<'tree, A, D, K, T, V> RangeQuery<'tree, A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone,
          V: Value
    {

    fn new<R>(range: R, tree: &'tree Tree<A, D, K, V>)
        -> RangeQuery<'tree, A, D, K, T, V>
        where R: RangeBounds<T>
    {
        let cursor: Option<Bound<T>> = Some(match range.start_bound() {
            Bound::Included(&ref b) => Bound::Included(b.clone()),
            Bound::Excluded(&ref b) => Bound::Excluded(b.clone()),
            Bound::Unbounded => Bound::Unbounded,
        });
        let end: Bound<T> = match range.end_bound() {
            Bound::Included(&ref e) => Bound::Included(e.clone()),
            Bound::Excluded(&ref e) => Bound::Excluded(e.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let data = VecDeque::new();
        RangeQuery{cursor, data, end, last_fut: None, tree: tree}
    }
}

impl<'tree, A, D, K, T, V> Stream for RangeQuery<'tree, A, D, K, T, V>
    where A: Addr,
          D: DML<Addr=A>,
          K: Key + Borrow<T>,
          T: Ord + Clone + 'static,
          V: Value
{
    type Item = (K, V);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.data.pop_front()
            .map(|x| Ok(Async::Ready(Some(x))))
            .unwrap_or_else(|| {
                if self.cursor.is_some() {
                    let mut fut = self.last_fut.take().unwrap_or_else(|| {
                        let l = self.cursor.clone().unwrap();
                        let r = (l, self.end.clone());
                        Box::new(self.tree.get_range(r))
                    });
                    match fut.poll() {
                        Ok(Async::Ready((v, bound))) => {
                            self.data = v;
                            self.cursor = bound;
                            self.last_fut = None;
                            Ok(Async::Ready(self.data.pop_front()))
                        },
                        Ok(Async::NotReady) => {
                            self.last_fut = Some(fut);
                            Ok(Async::NotReady)
                        },
                        Err(e) => Err(e)
                    }
                } else {
                    Ok(Async::Ready(None))
                }
            })
    }
}

struct CleanZonePass1Inner<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP> + 'tree,
          K: Key,
          V: Value
{
    /// If Some, then there are more nodes in the Tree to query
    cursor: Option<K>,

    /// Data that can be returned immediately
    data: VecDeque<NodeId<K>>,

    /// Level of the Tree that this object is meant to clean.  Leaves are 0.
    echelon: u8,

    /// Used when an operation must block
    last_fut: Option<Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                       Error=Error> + 'tree>>,

    /// Range of addresses to move
    range: Range<PBA>,

    /// Handle to the tree
    tree: &'tree Tree<ddml::DRP, D, K, V>
}

/// Result type of `Tree::clean_zone`
struct CleanZonePass1<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP> + 'tree,
          K: Key,
          V: Value
{
    inner: RefCell<CleanZonePass1Inner<'tree, D, K, V>>
}

impl<'tree, D, K, V> CleanZonePass1<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
    {

    fn new(range: Range<PBA>, echelon: u8,
           tree: &'tree Tree<ddml::DRP, D, K, V>)
        -> CleanZonePass1<'tree, D, K, V>
    {
        let cursor = Some(K::min_value());
        let data = VecDeque::new();
        let last_fut = None;
        let inner = CleanZonePass1Inner{cursor, data, echelon, last_fut, range,
                                        tree};
        CleanZonePass1{inner: RefCell::new(inner)}
    }
}

impl<'tree, D, K, V> Stream for CleanZonePass1<'tree, D, K, V>
    where D: DML<Addr=ddml::DRP>,
          K: Key,
          V: Value
{
    type Item = NodeId<K>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let first = {
            let mut i = self.inner.borrow_mut();
            i.data.pop_front()
        };
        first.map(|x| Ok(Async::Ready(Some(x))))
            .unwrap_or_else(|| {
                let i = self.inner.borrow();
                if i.cursor.is_some() {
                    drop(i);
                    let mut f = stream::poll_fn(|| -> Poll<Option<()>, Error> {
                        let mut i = self.inner.borrow_mut();
                        let mut f = i.last_fut.take().unwrap_or_else(|| {
                            let l = i.cursor.clone().unwrap();
                            let range = i.range.clone();
                            let e = i.echelon;
                            Box::new(i.tree.get_dirty_nodes(l, range, e))
                        });
                        match f.poll() {
                            Ok(Async::Ready((v, bound))) => {
                                i.data = v;
                                i.cursor = bound;
                                i.last_fut = None;
                                if i.data.is_empty() && i.cursor.is_some() {
                                    // Restart the search at the next bound
                                    Ok(Async::Ready(Some(())))
                                } else {
                                    // Search is done or data is ready
                                    Ok(Async::Ready(None))
                                }
                            },
                            Ok(Async::NotReady) => {
                                i.last_fut = Some(f);
                                Ok(Async::NotReady)
                            },
                            Err(e) => Err(e)
                        }
                    }).fold((), |_, _| future::ok::<(), Error>(()));
                    match f.poll() {
                        Ok(Async::Ready(())) => {
                            let mut i = self.inner.borrow_mut();
                            if i.last_fut.is_some() {
                                Ok(Async::NotReady)
                            } else {
                                Ok(Async::Ready(i.data.pop_front()))
                            }
                        },
                        Ok(Async::NotReady) => {
                            Ok(Async::NotReady)
                        },
                        Err(e) => Err(e)
                    }
                } else {
                    Ok(Async::Ready(None))
                }
            })
    }
}

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
#[serde(bound(deserialize = "K: DeserializeOwned"))]
struct Inner<A: Addr, K: Key, V: Value> {
    /// Tree height.  1 if the Tree consists of a single Leaf node.
    // Use atomics so it can be modified from an immutable reference.  Accesses
    // should be very rare, so performance is not a concern.
    #[serde(with = "atomic_usize_serializer")]
    height: AtomicUsize,
    /// Minimum node fanout.  Smaller nodes will be merged, or will steal
    /// children from their neighbors.
    min_fanout: usize,
    /// Maximum node fanout.  Larger nodes will be split.
    max_fanout: usize,
    /// Maximum node size in bytes.  Larger nodes will be split or their message
    /// buffers flushed
    _max_size: usize,
    /// Root node
    #[serde(with = "tree_root_serializer")]
    root: RwLock<IntElem<A, K, V>>
}

/// In-memory representation of a COW B+-Tree
///
/// # Generic Parameters
///
/// *`K`:   Key type.  Must be ordered and copyable; should be compact
/// *`V`:   Value type in the leaves.
pub struct Tree<A: Addr, D: DML<Addr=A>, K: Key, V: Value> {
    dml: Arc<D>,
    i: Inner<A, K, V>
}

impl<'a, A: Addr, D: DML<Addr=A>, K: Key, V: Value> Tree<A, D, K, V> {

    pub fn create(dml: Arc<D>) -> Self {
        Tree::new(dml,
                  4,        // BetrFS's min fanout
                  16,       // BetrFS's max fanout
                  1<<22,    // BetrFS's max size
        )
    }

    /// Fix an Int node in danger of being underfull, returning the parent guard
    /// back to the caller
    fn fix_int<Q>(&'a self, parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, mut child: TreeWriteGuard<A, K, V>)
        -> impl Future<Item=(TreeWriteGuard<A, K, V>, i8, i8), Error=Error> + 'a
        where K: Borrow<Q>, Q: Ord
    {
        // Outline:
        // First, try to merge with the right sibling
        // Then, try to steal keys from the right sibling
        // Then, try to merge with the left sibling
        // Then, try to steal keys from the left sibling
        // TODO: adjust txg range when stealing keys
        let nchildren = parent.as_int().nchildren();
        let (fut, sib_idx, right) = {
            if child_idx < nchildren - 1 {
                let sib_idx = child_idx + 1;
                (parent.xlock(&*self.dml, sib_idx), sib_idx, true)
            } else {
                let sib_idx = child_idx - 1;
                (parent.xlock(&*self.dml, sib_idx), sib_idx, false)
            }
        };
        fut.map(move |(mut parent, mut sibling)| {
            let (before, after) = if right {
                if child.can_merge(&sibling, self.i.max_fanout) {
                    child.merge(sibling);
                    parent.as_int_mut().children.remove(sib_idx);
                    (0, 1)
                } else {
                    child.take_low_keys(&mut sibling);
                    parent.as_int_mut().children[sib_idx].key = *sibling.key();
                    (0, 0)
                }
            } else {
                if sibling.can_merge(&child, self.i.max_fanout) {
                    sibling.merge(child);
                    parent.as_int_mut().children.remove(child_idx);
                    (1, 0)
                } else {
                    child.take_high_keys(&mut sibling);
                    parent.as_int_mut().children[child_idx].key = *child.key();
                    (1, 1)
                }
            };
            (parent, before, after)
        })
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Subroutine of range_delete.  Fixes a node that is in danger of an
    /// underflow.  Returns the node guard, and two ints which are the number of
    /// nodes that were merged before and after the fixed child, respectively.
    fn fix_if_in_danger<R, T>(&'a self, parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, range: R,
                  ubound: Option<K>)
        -> Box<Future<Item=(TreeWriteGuard<A, K, V>, i8, i8), Error=Error> + 'a>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Is the child underfull?  The limit here is b-1, not b as in
        // Tree::remove, because we've already removed keys
        if child.underflow(self.i.min_fanout - 1) {
            Box::new(self.fix_int(parent, child_idx, child))
        } else if !child.is_leaf() {
            // How many grandchildren are in the cut?
            let (start_idx_bound, end_idx_bound) = self.range_delete_get_bounds(
                &child, &range, ubound);
            let cut_grandkids = match (start_idx_bound, end_idx_bound) {
                (Bound::Included(_), Bound::Excluded(_)) => 0,
                (Bound::Included(_), Bound::Included(_)) => 1,
                (Bound::Excluded(_), Bound::Excluded(_)) => 1,
                (Bound::Excluded(i), Bound::Included(j)) if j > i => 2,
                (Bound::Excluded(i), Bound::Included(j)) if j <= i => 1,
                // LCOV_EXCL_START  kcov false negative
                (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
                (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
                // LCOV_EXCL_STOP
            };
            let b = self.i.min_fanout;
            if child.as_int().nchildren() - cut_grandkids <= b - 1 {
                Box::new(self.fix_int(parent, child_idx, child))
            } else {
                Box::new(Ok((parent, 0, 0)).into_future())
            }
        } else {
            Box::new(Ok((parent, 0, 0)).into_future())
        }
    }

    #[cfg(test)]
    pub fn from_str(dml: Arc<D>, s: &str) -> Self {
        let i: Inner<A, K, V> = serde_yaml::from_str(s).unwrap();
        Tree{dml, i}
    }

    /// Insert value `v` into the tree at key `k`, returning the previous value
    /// for that key, if any.
    pub fn insert(&'a self, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error> + 'a {

        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .and_then(move |(root_guard, child_guard)| {
                         self.insert_locked(root_guard, child_guard, k, v)
                     })
            })
    }

    /// Insert value `v` into an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn insert_int(&'a self, mut parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a> {

        // First, split the node, if necessary
        if (*child).should_split(self.i.max_fanout) {
            let (old_txgs, new_elem) = child.split(self.dml.txg());
            parent.as_int_mut().children[child_idx].txgs = old_txgs;
            parent.as_int_mut().children.insert(child_idx + 1, new_elem);
            // Reinsert into the parent, which will choose the correct child
            Box::new(self.insert_int_no_split(parent, k, v))
        } else {
            if child.is_leaf() {
                let elem = &mut parent.as_int_mut().children[child_idx];
                Box::new(self.insert_leaf_no_split( elem, child, k, v))
            } else {
                drop(parent);
                Box::new(self.insert_int_no_split(child, k, v))
            }
        }
    }

    /// Insert a value into a leaf node without splitting it
    fn insert_leaf_no_split(&'a self, elem: &mut IntElem<A, K, V>,
                  mut child: TreeWriteGuard<A, K, V>, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error> + 'a
    {
        let old_v = child.as_leaf_mut().insert(k, v);
        let txg = self.dml.txg();
        elem.txgs = txg..txg + 1;
        return Ok(old_v).into_future()
    }

    /// Helper for `insert`.  Handles insertion once the tree is locked
    fn insert_locked(&'a self, mut relem: RwLockWriteGuard<IntElem<A, K, V>>,
                     mut rnode: TreeWriteGuard<A, K, V>, k: K, v: V)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {
        // First, split the root node, if necessary
        if rnode.should_split(self.i.max_fanout) {
            let (old_txgs, new_elem) = rnode.split(self.dml.txg());
            let new_root_data = NodeData::Int( IntData::new(vec![new_elem]));
            let old_root_data = mem::replace(rnode.deref_mut(), new_root_data);
            let old_root_node = Node::new(old_root_data);
            let old_ptr = TreePtr::Mem(Box::new(old_root_node));
            let old_elem = IntElem::new(K::min_value(), old_txgs, old_ptr );
            rnode.as_int_mut().children.insert(0, old_elem);
            self.i.height.fetch_add(1, Ordering::Relaxed);
        }

        if rnode.is_leaf() {
            Box::new(self.insert_leaf_no_split(&mut *relem, rnode, k, v))
        } else {
            drop(relem);
            Box::new(self.insert_int_no_split(rnode, k, v))
        }
    }

    /// Insert a value into an int node without splitting it
    fn insert_int_no_split(&'a self, node: TreeWriteGuard<A, K, V>, k: K, v: V)
        -> impl Future<Item=Option<V>, Error=Error> + 'a
    {
        let child_idx = node.as_int().position(&k);
        let fut = node.xlock(&*self.dml, child_idx);
        fut.and_then(move |(parent, child)| {
                self.insert_int(parent, child_idx, child, k, v)
        })
    }

    /// Lookup the value of key `k`.  Return `None` if no value is present.
    pub fn get(&'a self, k: K) -> impl Future<Item=Option<V>, Error=Error> + 'a
    {
        self.read()
            .and_then(move |guard| {
                guard.rlock(&*self.dml)
                     .and_then(move |guard| self.get_r(guard, k))
            })
    }

    /// Lookup the value of key `k` in a node, which must already be locked.
    fn get_r(&'a self, node: TreeReadGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {

        let next_node_fut = match *node {
            NodeData::Leaf(ref leaf) => {
                return Box::new(Ok(leaf.get(&k)).into_future())
            },
            NodeData::Int(ref int) => {
                let child_elem = &int.children[int.position(&k)];
                child_elem.rlock(&*self.dml)
            }
        };
        drop(node);
        Box::new(
            next_node_fut
            .and_then(move |next_node| self.get_r(next_node, k))
        )
    }

    /// Private helper for `Range::poll`.  Returns a subset of the total
    /// results, consisting of all matching (K,V) pairs within a single Leaf
    /// Node, plus an optional Bound for the next iteration of the search.  If
    /// the Bound is `None`, then the search is complete.
    fn get_range<R, T>(&'a self, range: R)
        -> impl Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                       Error=Error> + 'a
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static
    {
        self.read()
            .and_then(move |guard| {
                guard.rlock(&*self.dml)
                     .and_then(move |g| self.get_range_r(g, None, range))
            })
    }

    /// Range lookup beginning in the node `guard`.  `next_guard`, if present,
    /// must be the node immediately to the right (and possibly up one or more
    /// levels) from `guard`.
    fn get_range_r<R, T>(&'a self, guard: TreeReadGuard<A, K, V>,
                            next_guard: Option<TreeReadGuard<A, K, V>>, range: R)
        -> Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>),
                      Error=Error> + 'a>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static
    {
        let (child_fut, next_fut) = match *guard {
            NodeData::Leaf(ref leaf) => {
                let (v, more) = leaf.range(range.clone());
                let ret = if v.is_empty() && more && next_guard.is_some() {
                    // We must've started the query with a key that's not
                    // present, and lies between two leaves.  Check the next
                    // node
                    self.get_range_r(next_guard.unwrap(), None, range)
                } else if v.is_empty() {
                    // The range is truly empty
                    Box::new(Ok((v, None)).into_future())
                } else {
                    let bound = if more && next_guard.is_some() {
                        Some(Bound::Included(next_guard.unwrap()
                                                       .key()
                                                       .borrow()
                                                       .clone()))
                    } else {
                        None
                    };
                    Box::new(Ok((v, bound)).into_future())
                };
                return ret;
            },
            NodeData::Int(ref int) => {
                let child_idx = match range.start_bound() {
                    Bound::Included(i) | Bound::Excluded(i) => int.position(i),
                    Bound::Unbounded => 0
                };
                let child_elem = &int.children[child_idx];
                let next_fut = if child_idx < int.nchildren() - 1 {
                    Box::new(
                        int.children[child_idx + 1].rlock(&*self.dml)
                            .map(|guard| Some(guard))
                    ) as Box<Future<Item=Option<TreeReadGuard<A, K, V>>,
                                    Error=Error>>
                } else {
                    Box::new(Ok(next_guard).into_future())
                        as Box<Future<Item=Option<TreeReadGuard<A, K, V>>,
                                      Error=Error>>
                };
                let child_fut = child_elem.rlock(&*self.dml);
                (child_fut, next_fut)
            } // LCOV_EXCL_LINE kcov false negative
        };
        drop(guard);
        Box::new(
            child_fut.join(next_fut)
                .and_then(move |(child_guard, next_guard)| {
                self.get_range_r(child_guard, next_guard, range)
            })
        ) as Box<Future<Item=(VecDeque<(K, V)>, Option<Bound<T>>), Error=Error>>
    }

    /// Merge the root node with its children, if necessary
    fn merge_root(&self, root_guard: &mut TreeWriteGuard<A, K, V>) {
        if ! root_guard.is_leaf() && root_guard.as_int().nchildren() == 1
        {
            // Merge root node with its child
            let child = root_guard.as_int_mut().children.pop().unwrap();
            let new_root_data = match child.ptr {
                TreePtr::Mem(n) => n.0.try_unwrap().unwrap(),
                // LCOV_EXCL_START
                _ => unreachable!(
                    "Can't merge_root without first dirtying the tree"),
                //LCOV_EXCL_STOP
            };
            mem::replace(root_guard.deref_mut(), new_root_data);
            self.i.height.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Lookup a range of (key, value) pairs for keys within the range `range`.
    pub fn range<R, T>(&'a self, range: R) -> RangeQuery<'a, A, D, K, T, V>
        where K: Borrow<T>,
              R: RangeBounds<T>,
              T: Ord + Clone
    {
        RangeQuery::new(range, self)
    }

    /// Delete a range of keys
    pub fn range_delete<R, T>(&'a self, range: R)
        -> impl Future<Item=(), Error=Error> + 'a
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Outline:
        // 1) Traverse the tree removing all requested KV-pairs, leaving damaged
        //    nodes
        // 2) Traverse the tree again, fixing in-danger nodes from top-down
        // 3) Collapse the root node, if it has 1 child
        let rangeclone = range.clone();
        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                     .and_then(move |(tree_guard, root_guard)| {
                         self.range_delete_pass1(root_guard, range, None)
                             .map(|_| tree_guard)
                     })
            })
            .and_then(move |guard| {
                self.xlock_root(guard)
                    .and_then(move |(tree_guard, root_guard)| {
                        self.range_delete_pass2(root_guard, rangeclone, None)
                            .map(|_| tree_guard)
                    })
            })
            // Finally, collapse the root node, if it has 1 child.  During
            // normal remove operations we can't do this, because we drop the
            // lock on the root node before fixing all of its children.  But the
            // entire tree stays locked during range_delete, so it's possible to
            // fix the root node at the end
            .and_then(move |tree_guard| {
                self.xlock_root(tree_guard)
                    .map(move |(tree_guard, mut root_guard)| {
                        self.merge_root(&mut root_guard);
                        // Keep the whole tree locked during range_delete
                        drop(tree_guard)
                    })  // LCOV_EXCL_LINE   kcov false negative
            })
    }

    /// Subroutine of range_delete.  Returns the bounds, as indices, of the
    /// affected children of this node.
    fn range_delete_get_bounds<R, T>(&self, guard: &TreeWriteGuard<A, K, V>,
                                     range: &R, ubound: Option<K>)
        -> (Bound<usize>, Bound<usize>)
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        debug_assert!(!guard.is_leaf());
        let l = guard.as_int().nchildren();
        let start_idx_bound = match range.start_bound() {
            Bound::Unbounded => Bound::Included(0),
            Bound::Included(t) | Bound::Excluded(t)
                if t < guard.key().borrow() =>
            {
                Bound::Included(0)
            },
            Bound::Included(t) => {
                let idx = guard.as_int().position(t);
                if guard.as_int().children[idx].key.borrow() == t {
                    // Remove the entire Node
                    Bound::Included(idx)
                } else {
                    // Recurse into the first Node
                    Bound::Excluded(idx)
                }
            },
            Bound::Excluded(t) => {
                // Recurse into the first Node
                let idx = guard.as_int().position(t);
                Bound::Excluded(idx)
            },
        };
        let end_idx_bound = match range.end_bound() {
            Bound::Unbounded => Bound::Excluded(l + 1),
            Bound::Included(t) => {
                let idx = guard.as_int().position(t);
                if ubound.is_some() && t >= ubound.unwrap().borrow() {
                    Bound::Excluded(idx + 1)
                } else {
                    Bound::Included(idx)
                }
            },
            Bound::Excluded(t) => {
                let idx = guard.as_int().position(t);
                if ubound.is_some() && t >= ubound.unwrap().borrow() {
                    Bound::Excluded(idx + 1)
                } else if guard.as_int().children[idx].key.borrow() == t {
                    Bound::Excluded(idx)
                } else {
                    Bound::Included(idx)
                }
            }
        };
        (start_idx_bound, end_idx_bound)
    }

    /// Depth-first traversal deleting keys without reshaping tree
    /// `ubound` is the first key in the Node immediately to the right of
    /// this one, unless this is the rightmost Node on its level.
    fn range_delete_pass1<R, T>(&'a self, mut guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>)
        -> impl Future<Item=(), Error=Error> + 'a
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        if guard.is_leaf() {
            guard.as_leaf_mut().range_delete(range);
            return Box::new(Ok(()).into_future())
                as Box<Future<Item=(), Error=Error>>;
        }

        // We must recurse into at most two children (at the limits of the
        // range), and completely delete 0 or more children (in the middle
        // of the range)
        let l = guard.as_int().nchildren();
        let (start_idx_bound, end_idx_bound) = self.range_delete_get_bounds(
            &guard, &range, ubound);
        let fut: Box<Future<Item=TreeWriteGuard<A, K, V>, Error=Error>>
            = match (start_idx_bound, end_idx_bound) {
            (Bound::Included(_), Bound::Excluded(_)) => {
                // Don't recurse
                Box::new(Ok(guard).into_future())
            },
            (Bound::Included(_), Bound::Included(j)) => {
                // Recurse into a Node at the end
                let ubound = if j < l - 1 {
                    Some(guard.as_int().children[j + 1].key)
                } else {
                    ubound
                };
                Box::new(guard.xlock(&*self.dml, j)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ubound)
                            .map(move |_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Excluded(_)) => {
                // Recurse into a Node at the beginning
                let ubound = if i < l - 1 {
                    Some(guard.as_int().children[i + 1].key)
                } else {
                    ubound
                };
                Box::new(guard.xlock(&*self.dml, i)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ubound)
                            .map(move |_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Included(j)) if j > i => {
                // Recurse into a Node at the beginning and end
                let range2 = range.clone();
                let ub_l = Some(guard.as_int().children[i + 1].key);
                let ub_h = if j < l - 1 {
                    Some(guard.as_int().children[j + 1].key)
                } else {
                    ubound
                };
                Box::new(guard.xlock(&*self.dml, i)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ub_l)
                            .map(|_| parent_guard)
                    }).and_then(move |parent_guard| {
                        parent_guard.xlock(&*self.dml, j)
                    }).and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range2, ub_h)
                            .map(|_| parent_guard)
                    }))
            },
            (Bound::Excluded(i), Bound::Included(j)) if j <= i => {
                // Recurse into a single Node
                let ubound = if i < l - 1 {
                    Some(guard.as_int().children[i + 1].key)
                } else {
                    ubound
                };
                Box::new(guard.xlock(&*self.dml, i)
                    .and_then(move |(parent_guard, child_guard)| {
                        self.range_delete_pass1(child_guard, range, ubound)
                            .map(move |_| parent_guard)
                    }))
            },
            // LCOV_EXCL_START  kcov false negative
            (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
            // LCOV_EXCL_STOP
        };

        // Finally, remove nodes in the middle
        Box::new(fut.map(move |mut guard| {
            let low = match start_idx_bound {
                Bound::Excluded(i) => i + 1,
                Bound::Included(i) => i,
                Bound::Unbounded => unreachable!()  // LCOV_EXCL_LINE
            };
            let high = match end_idx_bound {
                Bound::Excluded(j) | Bound::Included(j) => j,
                Bound::Unbounded => unreachable!()  // LCOV_EXCL_LINE
            };
            if high > low {
                guard.as_int_mut().children.drain(low..high);
            }
        }))
    }

    /// Depth-first traversal reshaping the tree after some keys were deleted by
    /// range_delete_pass1.
    fn range_delete_pass2<R, T>(&'a self, guard: TreeWriteGuard<A, K, V>,
                                range: R, ubound: Option<K>)
        -> Box<Future<Item=(), Error=Error> + 'a>
        where K: Borrow<T>,
              R: Clone + RangeBounds<T> + 'static,
              T: Ord + Clone + 'static + Debug
    {
        // Outline:
        // Traverse the tree just as in range_delete_pass1, but fixup any nodes
        // that are in danger.  A node is in-danger if it is in the cut and:
        // a) it has an underflow, or
        // b) it has b entries and one child in the cut, or
        // c) it has b + 1 entries and two children in the cut
        if guard.is_leaf() {
            // This node was already fixed.  No need to recurse further
            return Box::new(Ok(()).into_future());
        }

        let (start_idx_bound, end_idx_bound) = self.range_delete_get_bounds(
            &guard, &range, ubound);
        let range2 = range.clone();
        let range3 = range.clone();
        let children_to_fix = match (start_idx_bound, end_idx_bound) {
            (Bound::Included(_), Bound::Excluded(_)) => (None, None),
            (Bound::Included(_), Bound::Included(j)) => (None, Some(j)),
            (Bound::Excluded(i), Bound::Excluded(_)) => (Some(i), None),
            (Bound::Excluded(i), Bound::Included(j)) if j > i =>
                (Some(i), Some(j)),
            (Bound::Excluded(i), Bound::Included(j)) if j <= i =>
                (Some(i), None),
            // LCOV_EXCL_START  kcov false negative
            (Bound::Excluded(_), Bound::Included(_)) => unreachable!(),
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => unreachable!(),
            // LCOV_EXCL_STOP
        };
        let fixit = move |parent_guard: TreeWriteGuard<A, K, V>, idx: usize,
                          range: R|
        {
            let l = parent_guard.as_int().nchildren();
            let child_ubound = if idx < l - 1 {
                Some(parent_guard.as_int().children[idx + 1].key)
            } else {
                ubound
            };
            let range2 = range.clone();
            Box::new(
                parent_guard.xlock(&*self.dml, idx)
                .and_then(move |(parent_guard, child_guard)| {
                    self.fix_if_in_danger(parent_guard, idx, child_guard,
                                          range, child_ubound)
                }).and_then(move |(parent_guard, merged_before, merged_after)| {
                    let merged = merged_before + merged_after;
                    parent_guard.xlock(&*self.dml, idx - merged_before as usize)
                        .map(move |(parent, child)| (parent, child, merged))
                }).and_then(move |(parent_guard, child_guard, merged)| {
                    self.range_delete_pass2(child_guard, range2, child_ubound)
                        .map(move |_| (parent_guard, merged))
                })
            ) as Box<Future<Item=(TreeWriteGuard<A, K, V>, i8), Error=Error>>
        };
        let fut = match children_to_fix.0 {
            None => Box::new(Ok((guard, 0i8)).into_future())
                as Box<Future<Item=(TreeWriteGuard<A, K, V>, i8), Error=Error>>,
            Some(idx) => fixit(guard, idx, range2)
        }
        .and_then(move |(parent_guard, merged)|
            match children_to_fix.1 {
                None => Box::new(Ok((parent_guard, merged)).into_future())
                    as Box<Future<Item=(TreeWriteGuard<A, K, V>, i8),
                                  Error=Error>>,
                Some(idx) => fixit(parent_guard, idx - merged as usize, range3)
            }
        ).map(|_| ());
        Box::new(fut)
    }

    fn new(dml: Arc<D>, min_fanout: usize, max_fanout: usize,
           max_size: usize) -> Self
    {
        // Since there are no on-disk children, the initial TXG range is empty
        let txgs = 0..0;
        let i: Inner<A, K, V> = Inner {
            height: AtomicUsize::new(1),
            min_fanout, max_fanout,
            _max_size: max_size,
            root: RwLock::new(
                IntElem::new(K::min_value(),
                    txgs,
                    TreePtr::Mem(
                        Box::new(
                            Node::new(
                                NodeData::Leaf(
                                    LeafData::new()
                                )
                            )
                        )
                    )
                )
            )
        };
        Tree{ dml, i }
    }

    /// Remove and return the value at key `k`, if any.
    pub fn remove(&'a self, k: K)
        -> impl Future<Item=Option<V>, Error=Error> + 'a
    {
        self.write()
            .and_then(move |guard| {
                self.xlock_root(guard)
                    .and_then(move |(_root_guard, child_guard)| {
                        self.remove_locked(child_guard, k)
                    })
        })
    }

    /// Remove key `k` from an internal node.  The internal node and its
    /// relevant child must both be already locked.
    fn remove_int(&'a self, parent: TreeWriteGuard<A, K, V>,
                  child_idx: usize, child: TreeWriteGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {
        // First, fix the node, if necessary
        if child.underflow(self.i.min_fanout) {
            Box::new(
                self.fix_int(parent, child_idx, child)
                    .and_then(move |(parent, _, _)| {
                        let child_idx = parent.as_int().position(&k);
                        parent.xlock(&*self.dml, child_idx)
                    }).and_then(move |(parent, child)| {
                        drop(parent);
                        self.remove_no_fix(child, k)
                    })
            )
        } else {
            drop(parent);
            self.remove_no_fix(child, k)
        }
    }

    /// Helper for `remove`.  Handles removal once the tree is locked
    fn remove_locked(&'a self, mut root: TreeWriteGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {
        self.merge_root(&mut root);
        self.remove_no_fix(root, k)
    }

    /// Remove key `k` from a node, but don't try to fixup the node.
    fn remove_no_fix(&'a self, mut node: TreeWriteGuard<A, K, V>, k: K)
        -> Box<Future<Item=Option<V>, Error=Error> + 'a>
    {

        if node.is_leaf() {
            let old_v = node.as_leaf_mut().remove(&k);
            return Box::new(Ok(old_v).into_future());
        } else {
            let child_idx = node.as_int().position(&k);
            let fut = node.xlock(&*self.dml, child_idx);
            Box::new(fut.and_then(move |(parent, child)| {
                    self.remove_int(parent, child_idx, child, k)
                })
            )
        }
    }

    /// Flush all in-memory Nodes to disk.
    // Like range_delete, keep the entire Tree locked during flush.  That's
    // because we need to write child nodes before we have valid addresses for
    // their parents' child pointers.  It's also the only way to guarantee that
    // the Tree will be completely clean by the time that flush returns.  Flush
    // will probably only happen during TXG flush, which is once every few
    // seconds.
    //
    // Alternatively, it would be possible to create a streaming flusher like
    // RangeQuery that would descend through the tree multiple times, flushing a
    // portion at each time.  But it wouldn't be able to guarantee a clean tree.
    pub fn flush(&'a self) -> impl Future<Item=(), Error=Error> + 'a {
        self.write()
            .and_then(move |root_guard| {
            if root_guard.ptr.is_dirty() {
                // If the root is dirty, then we have ownership over it.  But
                // another task may still have a lock on it.  We must acquire
                // then release the lock to ensure that we have the sole
                // reference.
                let fut = self.xlock_root(root_guard)
                    .and_then(move |(mut root_guard, child_guard)|
                {
                    drop(child_guard);
                    let ptr = mem::replace(&mut root_guard.ptr, TreePtr::None);
                    Box::new(
                        self.flush_r(ptr.into_node())
                            .map(move |(addr, txgs)| {
                                root_guard.ptr = TreePtr::Addr(addr);
                                root_guard.txgs = txgs;
                            })
                    )
                });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            } else {
                Box::new(future::ok::<(), Error>(()))
            }
        })
    }

    fn write_leaf(&'a self, node: Box<Node<A, K, V>>)
        -> impl Future<Item=A, Error=Error> + 'a
    {
        let arc: Arc<Node<A, K, V>> = Arc::new(*node);
        let (addr, fut) = self.dml.put(arc, Compression::None);
        fut.map(move |_| addr)
    }

    fn flush_r(&'a self, mut node: Box<Node<A, K, V>>)
        -> Box<Future<Item=(D::Addr, Range<TxgT>), Error=Error> + 'a>
    {
        if node.0.get_mut().unwrap().is_leaf() {
            let fut = self.write_leaf(node)
                .map(move |addr| {
                    let txg = self.dml.txg();
                    (addr, txg..txg + 1)
                });
            return Box::new(fut);
        }
        let ndata = node.0.try_write().unwrap();

        // Rust's borrow checker doesn't understand that children_fut will
        // complete before its continuation will run, so it won't let ndata
        // be borrowed in both places.  So we'll have to use RefCell to allow
        // dynamic borrowing and Rc to allow moving into both closures.
        let rndata = Rc::new(RefCell::new(ndata));
        let nchildren = RefCell::borrow(&Rc::borrow(&rndata)).as_int().nchildren();
        let children_fut = (0..nchildren)
        .map(move |idx| {
            let rndata3 = rndata.clone();
            if rndata.borrow_mut()
                     .as_int_mut()
                     .children[idx].is_dirty()
            {
                // If the child is dirty, then we have ownership over it.  We
                // need to lock it, then release the lock.  Then we'll know that
                // we have exclusive access to it, and we can move it into the
                // Cache.
                let fut = rndata.borrow_mut()
                                .as_int_mut()
                                .children[idx]
                                .ptr
                                .as_mem()
                                .xlock()
                                .and_then(move |guard|
                {
                    drop(guard);

                    let ptr = mem::replace(&mut rndata3.borrow_mut()
                                                       .as_int_mut()
                                                       .children[idx].ptr,
                                           TreePtr::None);
                    self.flush_r(ptr.into_node())
                        .map(move |(addr, txgs)| {
                            let mut borrowed = rndata3.borrow_mut();
                            let elem = &mut borrowed.as_int_mut().children[idx];
                            elem.ptr = TreePtr::Addr(addr);
                            let start_txg = txgs.start;
                            elem.txgs = txgs;
                            start_txg
                        })
                });
                Box::new(fut) as Box<Future<Item=TxgT, Error=Error>>
            } else { // LCOV_EXCL_LINE kcov false negative
                let borrowed = RefCell::borrow(&rndata3);
                let txg = borrowed.as_int().children[idx].txgs.start;
                Box::new(future::ok(txg)) as Box<Future<Item=TxgT, Error=Error>>
            }
        })
        .collect::<Vec<_>>();
        Box::new(
            future::join_all(children_fut)
            .and_then(move |txgs| {
                let start_txg = *txgs.iter().min().unwrap();
                let arc: Arc<Node<A, K, V>> = Arc::new(*node);
                let (addr, fut) = self.dml.put(arc, Compression::None);
                fut.map(move |_| (addr, start_txg..self.dml.txg() + 1))
            })
        )
    }

    /// Lock the Tree for reading
    fn read(&'a self) -> impl Future<Item=RwLockReadGuard<IntElem<A, K, V>>,
                                     Error=Error> + 'a
    {
        self.i.root.read().map_err(|_| Error::Sys(errno::Errno::EPIPE))
    }

    /// Lock the Tree for writing
    fn write(&'a self) -> impl Future<Item=RwLockWriteGuard<IntElem<A, K, V>>,
                                      Error=Error> + 'a
    {
        self.i.root.write().map_err(|_| Error::Sys(errno::Errno::EPIPE))
    }

    /// Lock the root `IntElem` exclusively.  If it is not already resident in
    /// memory, then COW it.
    fn xlock_root(&'a self, mut guard: RwLockWriteGuard<IntElem<A, K, V>>)
        -> (Box<Future<Item=(RwLockWriteGuard<IntElem<A, K, V>>,
                             TreeWriteGuard<A, K, V>), Error=Error> + 'a>)
    {
        guard.txgs.end = self.dml.txg() + 1;
        if guard.ptr.is_mem() {
            Box::new(
                guard.ptr.as_mem().0.write()
                     .map(move |child_guard| {
                          (guard, TreeWriteGuard::Mem(child_guard))
                     }).map_err(|_| Error::Sys(errno::Errno::EPIPE))
            )
        } else {
            let addr = *guard.ptr.as_addr();
            Box::new(
                self.dml.pop::<Arc<Node<A, K, V>>, Arc<Node<A, K, V>>>(
                    &addr).map(move |arc|
                {
                    let child_node = Box::new(Arc::try_unwrap(*arc)
                        .expect("We should be the Node's only owner"));
                    guard.ptr = TreePtr::Mem(child_node);
                    let child_guard = TreeWriteGuard::Mem(
                        guard.ptr.as_mem().0.try_write().unwrap()
                    );
                    (guard, child_guard)
                })
            )
        }
    }
}

#[cfg(test)]
impl<A: Addr, D: DML<Addr=A>, K: Key, V: Value> Display for Tree<A, D, K, V> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&serde_yaml::to_string(&self.i).unwrap())
    }
}

// These methods are only for direct trees
impl<'a, D: DML<Addr=ddml::DRP>, K: Key, V: Value> Tree<ddml::DRP, D, K, V> {
    /// Clean `zone` by moving all of its records to other zones.
    pub fn clean_zone(&'a self, range: Range<PBA>)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        // We can't rewrite children before their parents while sticking to a
        // lock-coupling discipline.  And we can't rewrite parents before their
        // children, because we can't tell which parents have children that must
        // be modified.  So we'll use a two-pass approach.
        // Pass 1) Build a list of Nodes that must be rewritten
        // Pass 2) Rewrite each affected Node.
        // It's safe to do this without locking the entire tree, because we
        // should only be cleaning Closed zones, and no new Nodes will be
        // written to a closed zone until after it gets erased and reopened, and
        // that won't happen before we finish cleaning it.
        //
        // Furthermore, we'll repeat both passes for each level of the tree.
        // Not because we strictly need to, but because rewriting the lowest
        // levels first will modify many mid-level nodes, obliviating the need
        // to rewrite them.  It simplifies the first pass, too.
        //
        // TODO: Store the TXG range of each zone and the TXG range of the
        // subtree represented by each Node.  Use that information to prune the
        // number of Nodes that must be walked.
        let tree_height = self.i.height.load(Ordering::Relaxed) as u8;
        stream::iter_ok(0..tree_height).for_each(move|echelon| {
            CleanZonePass1::new(range.clone(), echelon, self)
                .collect()
                .and_then(move |nodes| {
                    stream::iter_ok(nodes.into_iter()).for_each(move |node| {
                        // TODO: consider attempting to rewrite multiple nodes
                        // at once, so as not to spend so much time traversing
                        // the tree
                        self.rewrite_node(node)
                    })
                })
        })
    }

    fn get_dirty_nodes(&'a self, key: K, range: Range<PBA>, echelon: u8)
        -> impl Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error> + 'a
    {
        self.read()
            .and_then(move |guard| {
                let h = self.i.height.load(Ordering::Relaxed) as u8;
                if h == echelon + 1 {
                    // Clean the tree root
                    let dirty = if guard.ptr.is_addr() &&
                        guard.ptr.as_addr().pba() >= range.start &&
                        guard.ptr.as_addr().pba() < range.end {
                        let mut v = VecDeque::new();
                        v.push_back(NodeId{height: echelon, key: guard.key});
                        v
                    } else {
                        VecDeque::new()
                    };
                    Box::new(future::ok((dirty, None)))
                        as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                                      Error=Error>>
                } else {
                    let fut = guard.rlock(&*self.dml)
                         .and_then(move |guard| {
                             self.get_dirty_nodes_r(guard, h - 1, None, key,
                                                    range, echelon)
                         });
                    Box::new(fut)
                        as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>),
                                      Error=Error>>
                }
            })
    }

    /// Find dirty nodes in `PBA` range `range`, beginning at `key`.
    /// `next_key`, if present, must be the key of the node immediately to the
    /// right (and possibly up one or more levels) from `guard`.  `height` is
    /// the tree height of `guard`, where leaves are 0.
    fn get_dirty_nodes_r(&'a self, guard: TreeReadGuard<ddml::DRP, K, V>,
                         height: u8,
                         next_key: Option<K>,
                         key: K, range: Range<PBA>, echelon: u8)
        -> Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error> + 'a>
    {
        if height == echelon + 1 {
            let nodes = guard.as_int().children.iter().filter_map(|child| {
                if child.ptr.is_addr() &&
                    child.ptr.as_addr().pba() >= range.start &&
                    child.ptr.as_addr().pba() < range.end {
                    Some(NodeId{height: height - 1, key: child.key})
                } else {
                    None
                }
            }).collect::<VecDeque<_>>();
            return Box::new(future::ok((nodes, next_key)))
        }
        let idx = guard.as_int().position(&key);
        let next_key = if idx < guard.as_int().nchildren() - 1 {
            Some(guard.as_int().children[idx + 1].key)
        } else {
            next_key
        };
        let child_fut = guard.as_int().children[idx].rlock(&*self.dml);
        drop(guard);
        Box::new(
            child_fut.and_then(move |child_guard| {
                self.get_dirty_nodes_r(child_guard, height - 1, next_key,
                                       key, range, echelon)
            })
        ) as Box<Future<Item=(VecDeque<NodeId<K>>, Option<K>), Error=Error>>
    }

    /// Rewrite `node`, without modifying its contents
    fn rewrite_node(&'a self, node: NodeId<K>)
        -> impl Future<Item=(), Error=Error> + 'a
    {
        self.write()
            .and_then(move |mut guard| {
            let h = self.i.height.load(Ordering::Relaxed) as u8;
            if h == node.height + 1 {
                // Clean the root node
                if guard.ptr.is_mem() {
                    // Another thread has already dirtied the root.  Nothing to
                    // do!
                    let fut = Box::new(future::ok(()));
                    return fut as Box<Future<Item=(), Error=Error>>;
                }
                let fut = self.dml.pop::<Arc<Node<ddml::DRP, K, V>>,
                                         Arc<Node<ddml::DRP, K, V>>>(
                                         guard.ptr.as_addr())
                    .and_then(move |arc| {
                        let (addr, fut) = self.dml.put(*arc, Compression::None);
                        let new = TreePtr::Addr(addr);
                        guard.ptr = new;
                        fut
                    });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            } else {
                let fut = self.xlock_root(guard)
                     .and_then(move |(_root_guard, child_guard)| {
                         self.rewrite_node_r(child_guard, h - 1, node)
                     });
                Box::new(fut) as Box<Future<Item=(), Error=Error>>
            }
        })
    }

    fn rewrite_node_r(&'a self, mut guard: TreeWriteGuard<ddml::DRP, K, V>,
                      height: u8, node: NodeId<K>)
        -> Box<Future<Item=(), Error=Error> + 'a>
    {
        debug_assert!(height > 0);
        let child_idx = guard.as_int().position(&node.key);
        if height == node.height + 1 {
            if guard.as_int().children[child_idx].ptr.is_mem() {
                // Another thread has already dirtied this node.  Nothing to do!
                return Box::new(future::ok(()));
            }
            // TODO: bypass the cache for this part
            // Need a solution for this issue first:
            // https://github.com/pcsm/simulacrum/issues/55
            let fut = self.dml.pop::<Arc<Node<ddml::DRP, K, V>>,
                                     Arc<Node<ddml::DRP, K, V>>>(
                            guard.as_int().children[child_idx].ptr.as_addr())
                .and_then(move |arc| {
                    #[cfg(debug_assertions)]
                    {
                        if let Ok(guard) = arc.0.try_read() {
                            assert!(node.key <= *guard.key());
                        }
                    }
                    let (addr, fut) = self.dml.put(*arc, Compression::None);
                    let new = TreePtr::Addr(addr);
                    guard.as_int_mut().children[child_idx].ptr = new;
                    fut
                });
            Box::new(fut)
        } else {
            let fut = guard.xlock(&*self.dml, child_idx)
                .and_then(move |(parent_guard, child_guard)| {
                    drop(parent_guard);
                    self.rewrite_node_r(child_guard, height - 1, node)
                });
            Box::new(fut)
        }
    }
}

#[cfg(test)] mod tests;
