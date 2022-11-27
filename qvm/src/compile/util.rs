use core::borrow::Borrow;
use std::collections::BTreeMap;

use crate::compile::inference::Constrainable;

#[derive(Debug, Clone)]
pub struct InsertionOrderMap<K, V> {
    map: BTreeMap<K, V>,
    order: Vec<K>,
}

// These methods are mostly copied from BTreeMap
// https://doc.rust-lang.org/src/alloc/collections/btree/map.rs.html
impl<K, V> InsertionOrderMap<K, V> {
    pub fn new() -> InsertionOrderMap<K, V> {
        InsertionOrderMap {
            map: BTreeMap::new(),
            order: Vec::new(),
        }
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        self.map.get(key)
    }

    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        self.map.get_mut(key)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Ord + Clone,
    {
        match self.map.insert(key.clone(), value) {
            Some(v) => Some(v),
            None => {
                self.order.push(key);
                None
            }
        }
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, K, V> {
        Iter {
            ordered_iter: self.order.iter(),
            map: &self.map,
        }
    }
}

pub struct Iter<'a, K: 'a, V: 'a> {
    ordered_iter: std::slice::Iter<'a, K>,
    map: &'a BTreeMap<K, V>,
}

impl<'a, K: 'a, V: 'a> Iter<'a, K, V> {
    fn get(&mut self, key: Option<&'a K>) -> Option<(&'a K, &'a V)>
    where
        K: Ord,
    {
        match key {
            Some(k) => Some((
                k,
                self.map
                    .get(k)
                    .expect("Broken InsertionOrderMap missing key"),
            )),
            None => None,
        }
    }
}

impl<'a, K: 'a, V: 'a> Iterator for Iter<'a, K, V>
where
    K: Ord,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        let next = self.ordered_iter.next();
        self.get(next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ordered_iter.size_hint()
    }

    fn last(mut self) -> Option<(&'a K, &'a V)> {
        let last = (&mut self.ordered_iter).last();
        (&mut self).get(last)
    }
}

impl<K, V> Constrainable for InsertionOrderMap<K, V>
where
    K: Constrainable,
    V: Constrainable,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity() {
        let mut map = InsertionOrderMap::new();
        for i in 0..10 {
            map.insert(10 - i, i);
        }

        let mut i = 0;
        for (k, v) in map.iter() {
            assert_eq!(*k, 10 - i);
            assert_eq!(*v, i);
            i += 1;
        }
    }
}
