//! Ordered iterators.

#![cfg_attr(all(feature = "nightly", test), feature(test))]

extern crate bit_set;
extern crate bit_vec;
extern crate vec_map;

use std::cmp::Ordering::*;
use std::iter::Peekable;
use std::collections::{
    btree_map, btree_set,
};

/// Allows an iterator to do an inner join with another
/// iterator to combine their values or filter based on their keys.
///
/// This trait is applied to an iterator over a map-like structure.
pub trait OrderedMapIterator: Iterator<Item=(<Self as OrderedMapIterator>::Key, <Self as OrderedMapIterator>::Val)> + Sized {
    type Key;
    type Val;
    /// Joins two ordered maps together.
    fn inner_join_map<I>(self, map: I) -> InnerJoinMap<Self, I>
    where I: OrderedMapIterator<Key=Self::Key> {
        InnerJoinMap {
            a: self,
            b: map
        }
    }

    /// Filters an ordered map with an ordered set.
    fn inner_join_set<I>(self, set: I) -> InnerJoinMapSet<Self, I>
    where I: OrderedSetIterator<Item=Self::Key> {
        InnerJoinMapSet {
            map: self,
            set: set
        }
    }

    /// Joins an ordered iterator with another ordered iterator.
    ///
    /// The new iterator will return a key-value pair for every key in
    /// either iterator. If a key is present in both iterators, they
    /// will be returned together (two values). If a value is in `other`
    /// but not `self`, it will be returned without the value in
    /// `self`. If the value is in `self` but not `other`,
    /// it will be returned without the value from `other`.
    fn outer_join<I>(self, other: I) -> OuterJoin<Self, I>
    where I: OrderedMapIterator<Key=Self::Key> {
        OuterJoin {
            left: self.peekable(),
            right: other.peekable()
        }
    }
}

/// Allows an iterator to do an inner join with another
/// iterator to combine their values or filter based on their keys.
///
/// This trait is applied to an iterator over a set-like structure.
pub trait OrderedSetIterator: Iterator + Sized {
    /// Joins two ordered maps together.
    fn inner_join_map<I>(self, map: I) -> InnerJoinMapSet<I, Self>
    where I: OrderedMapIterator<Key=Self::Item> {
        InnerJoinMapSet {
            map: map,
            set: self
        }
    }

    /// Filters an ordered map with an ordered set.
    fn inner_join_set<I>(self, map: I) -> InnerJoinSet<Self, I>
    where I: OrderedSetIterator<Item=Self::Item> {
        InnerJoinSet {
            a: self,
            b: map
        }
    }
}

#[derive(Clone)]
pub struct InnerJoinMap<A, B> {a: A, b: B}
#[derive(Clone)]
pub struct InnerJoinMapSet<A, B> {map: A, set: B}
#[derive(Clone)]
pub struct InnerJoinSet<A, B> {a: A, b: B}
pub struct OuterJoin<A: Iterator, B: Iterator> {
    left: Peekable<A>,
    right: Peekable<B>,
}

impl<A, B> Clone for OuterJoin<A, B>
where A: Clone + Iterator, B: Clone + Iterator, A::Item: Clone, B::Item: Clone {
    fn clone(&self) -> OuterJoin<A, B> {
        OuterJoin { left: self.left.clone(), right: self.right.clone() }
    }
}

impl<A, B> Iterator for InnerJoinMap<A, B>
where A: OrderedMapIterator,
      B: OrderedMapIterator<Key=A::Key>,
      A::Key: Ord,
{

    type Item = (A::Key, (A::Val, B::Val));

    fn next(&mut self) -> Option<(A::Key, (A::Val, B::Val))> {
        let (mut key_a, mut data_a) = match self.a.next() {
            None => return None,
            Some((key, data)) => (key, data)
        };

        let (mut key_b, mut data_b) = match self.b.next() {
            None => return None,
            Some((key, data)) => (key, data)
        };

        loop {
            match key_a.cmp(&key_b) {
                Less => {
                    match self.a.next() {
                        None => return None,
                        Some((key, data)) => {
                            key_a = key;
                            data_a = data;
                        }
                    };
                },
                Equal => return Some((key_a, (data_a, data_b))),
                Greater => {
                    match self.b.next() {
                        None => return None,
                        Some((key, data)) => {
                            key_b = key;
                            data_b = data;
                        }
                    };
                }
            }
        }
    }
}


impl<A, B> Iterator for InnerJoinSet<A, B>
where A: OrderedSetIterator,
      B: OrderedSetIterator<Item=A::Item>,
      A::Item: Ord,
{

    type Item = A::Item;

    fn next(&mut self) -> Option<A::Item> {
        let mut key_a = match self.a.next() {
            None => return None,
            Some(key) => key
        };

        let mut key_b = match self.b.next() {
            None => return None,
            Some(key) => key
        };

        loop {
            match key_a.cmp(&key_b) {
                Less => {
                    match self.a.next() {
                        None => return None,
                        Some(key) => { key_a = key; }
                    };
                },
                Equal => return Some(key_a),
                Greater => {
                    match self.b.next() {
                        None => return None,
                        Some(key) => { key_b = key; }
                    };
                }
            }
        }
    }
}

impl<MapIter, SetIter> Iterator for InnerJoinMapSet<MapIter, SetIter>
where SetIter: OrderedSetIterator,
      MapIter: OrderedMapIterator<Key=SetIter::Item>,
      MapIter::Key: Ord,
{

    type Item = (MapIter::Key, MapIter::Val);

    fn next(&mut self) -> Option<(MapIter::Key, MapIter::Val)> {
        let mut key_set = match self.set.next() {
            None => return None,
            Some(key) => key
        };

        let (mut key_map, mut data) = match self.map.next() {
            None => return None,
            Some((key, data)) => (key, data)
        };

        loop {
            match key_set.cmp(&key_map) {
                Less => {
                    match self.set.next() {
                        None => return None,
                        Some(key) => { key_set = key; }
                    };
                },
                Equal => return Some((key_set, data)),
                Greater => {
                    match self.map.next() {
                        None => return None,
                        Some((key, d)) => {
                            key_map = key;
                            data = d;
                        }
                    };
                }
            }
        }
    }
}

impl<A, B> Iterator for OuterJoin<A, B>
where A: OrderedMapIterator,
      B: OrderedMapIterator<Key=A::Key>,
      A::Key: Ord + Eq,
{

    type Item = (A::Key, (Option<A::Val>, Option<B::Val>));

    fn next(&mut self) -> Option<(A::Key, (Option<A::Val>, Option<B::Val>))> {
        let which = match (self.left.peek(), self.right.peek()) {
            (Some(&(ref ka, _)), Some(&(ref kb, _))) => kb.cmp(ka),
            (None, Some(_)) => Less,
            (Some(_), None) => Greater,
            (None, None) => return None
        };

        match which {
            Equal => {
                let ((k, a), (_, b)) =
                    (self.left.next().expect("no value found"),
                     self.right.next().expect("no value found"));

                Some((k, (Some(a), Some(b))))
            }
            Less => {
                let (k, v) = self.right.next().expect("no value found");
                Some((k, (None, Some(v))))
            }
            Greater => {
                let (k, v) = self.left.next().expect("no value found");
                Some((k, (Some(v), None)))
            }
        }
    }
}

impl<'a, K: Ord> OrderedSetIterator for btree_set::Iter<'a, K> {}
impl<'a, K: Ord, V> OrderedMapIterator for btree_map::Iter<'a, K, V> {
    type Key = &'a K;
    type Val = &'a V;
}

impl<K: Ord, V> OrderedMapIterator for btree_map::IntoIter<K, V> {
    type Key = K;
    type Val = V;
}

impl<'a, K: Ord, V> OrderedMapIterator for btree_map::IterMut<'a, K, V> {
    type Key = &'a K;
    type Val = &'a mut V;
}

impl<'a, K: Ord, V> OrderedSetIterator for btree_map::Keys<'a, K, V> {}

impl<'a, V> OrderedMapIterator for vec_map::Iter<'a, V> {
    type Key = usize;
    type Val = &'a V;
}

impl<'a, B: bit_vec::BitBlock> OrderedSetIterator for bit_set::Iter<'a, B> {}

impl<A, B> OrderedMapIterator for InnerJoinMap<A, B>
where A: OrderedMapIterator,
      B: OrderedMapIterator<Key=A::Key>,
      A::Key: Ord,
{
    type Key = A::Key;
    type Val = (A::Val, B::Val);
}

impl<A, B> OrderedMapIterator for InnerJoinMapSet<A, B>
where A: OrderedMapIterator,
      B: OrderedSetIterator<Item=A::Key>,
      A::Key: Ord,
{
    type Key = A::Key;
    type Val = A::Val;
}

impl<A, B> OrderedSetIterator for InnerJoinSet<A, B>
where A: OrderedSetIterator,
      B: OrderedSetIterator<Item=A::Item>,
      A::Item: Ord,
{}

impl<A, B> OrderedMapIterator for OuterJoin<A, B>
where A: OrderedMapIterator,
      B: OrderedMapIterator<Key=A::Key>,
      A::Key: Ord,
{
    type Key = A::Key;
    type Val = (Option<A::Val>, Option<B::Val>);
}


#[cfg(test)]
mod tests {
    #[cfg(all(feature = "nightly", test))]
    extern crate test;

    use super::{OrderedSetIterator, OrderedMapIterator};

    #[test]
    fn join_two_sets() {
        use std::collections::BTreeSet;

        let powers_of_two: BTreeSet<i32> = (1..10).map(|x| x * 2).collect();
        let powers_of_three: BTreeSet<i32> = (1..10).map(|x| x * 3).collect();

        let expected = vec![6, 12, 18];

        let powers_of_two_and_three: Vec<i32> =
            powers_of_two.iter()
            .inner_join_set(powers_of_three.iter())
            .map(|&x| x)
            .collect();

        assert_eq!(expected, powers_of_two_and_three);
    }

    #[test]
    fn join_three_sets() {
        use std::collections::BTreeSet;

        let powers_of_two: BTreeSet<i32> = (1..100).map(|x| x * 2).collect();
        let powers_of_three: BTreeSet<i32> = (1..100).map(|x| x * 3).collect();
        let powers_of_five: BTreeSet<i32> = (1..100).map(|x| x * 5).collect();

        let expected = vec![30, 60, 90, 120, 150, 180];

        let powers_of_two_and_three: Vec<i32> =
            powers_of_two.iter()
            .inner_join_set(powers_of_three.iter())
            .inner_join_set(powers_of_five.iter())
            .map(|&x| x)
            .collect();

        assert_eq!(expected, powers_of_two_and_three);
    }

    #[test]
    fn join_two_maps() {
        use std::collections::BTreeMap;

        let powers_of_two: BTreeMap<i32, i32> = (1..10).map(|x| (x * 2, x)).collect();
        let powers_of_three: BTreeMap<i32, i32> = (1..10).map(|x| (x * 3, x)).collect();

        let mut powers_of_two_and_three =
            powers_of_two.iter().inner_join_map(powers_of_three.iter())
            .map(|(&k, (&a, &b))| (k, a, b));

        assert_eq!(Some((6, 3, 2)), powers_of_two_and_three.next());
        assert_eq!(Some((12, 6, 4)), powers_of_two_and_three.next());
        assert_eq!(Some((18, 9, 6)), powers_of_two_and_three.next());
        assert_eq!(None, powers_of_two_and_three.next());
    }

    #[test]
    fn join_two_maps_to_set() {
        use std::collections::{BTreeMap, BTreeSet};

        let powers_of_two: BTreeSet<i32> = (1..10).map(|x| x * 2).collect();
        let powers_of_three: BTreeMap<i32, i32> = (1..10).map(|x| (x * 3, x)).collect();

        let mut powers_of_two_and_three =
            powers_of_two.iter().inner_join_map(powers_of_three.iter())
            .map(|(&k, &a)| (k, a));

        assert_eq!(Some((6, 2)), powers_of_two_and_three.next());
        assert_eq!(Some((12, 4)), powers_of_two_and_three.next());
        assert_eq!(Some((18, 6)), powers_of_two_and_three.next());
        assert_eq!(None, powers_of_two_and_three.next());
    }

    #[test]
    fn outer_join_fizz_buzz() {
        use std::collections::BTreeMap;

        let mul_of_three: BTreeMap<i32, i32> = (0..100).map(|x| (x*3, x)).collect();
        let mul_of_five: BTreeMap<i32, i32> = (0..100).map(|x| (x*5, x)).collect();

        let mut fizz_buzz = BTreeMap::new();

        for (key, (three, five)) in mul_of_three.iter()
                                                .outer_join(mul_of_five.iter()) {
            fizz_buzz.insert(key, (three.is_some(), five.is_some()));
        }

        let res: BTreeMap<i32, String> = (1..100).map(|i|
            (i, match fizz_buzz.get(&i) {
                None => format!("{}", i),
                Some(&(true, false)) => format!("Fizz"),
                Some(&(false, true)) => format!("Buzz"),
                Some(&(true, true)) => format!("FizzBuzz"),
                Some(&(false, false)) => panic!("Outer join failed...")
            })).collect();

        for i in 1..100 {
            match (i % 3, i % 5) {
                (0, 0) => assert_eq!("FizzBuzz", res[&i]),
                (0, _) => assert_eq!("Fizz", res[&i]),
                (_, 0) => assert_eq!("Buzz", res[&i]),
                _ => assert_eq!(format!("{}", i), res[&i])
            }
        }
    }

    #[bench]
    #[cfg(all(feature = "nightly", test))]
    pub fn inner_join_map(b: &mut self::test::Bencher) {
        use std::collections::BTreeSet;

        let powers_of_two: BTreeSet<u32> = (1..1000000).map(|x| x * 2).collect();
        let powers_of_three: BTreeSet<u32> = (1..1000000).map(|x| x * 3).collect();

        b.iter(||{
            for x in powers_of_two.iter()
                .inner_join_set(powers_of_three.iter()) {

                test::black_box(x);
            }
        })
    }
}
