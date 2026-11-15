package io.github.vanish073.driftline.sim.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * One table of {@link InMemoryStore}: the committed rows, keyed and ordered by {@code K}, and a per-transaction
 * {@link View} that buffers writes (puts and deletes) until commit. Abort is simply forgetting the view. Reads in a
 * view see the view's own writes, as in a database transaction.
 */
final class Table<K extends Comparable<K>, V> {

    private final TreeMap<K, V> committed = new TreeMap<>();

    /** Committed rows only, for the simulator's read-only views. */
    TreeMap<K, V> committed() {
        return committed;
    }

    View view() {
        return new View();
    }

    final class View {

        /** Pending writes: a present value is a put, an empty one a delete. */
        private final TreeMap<K, Optional<V>> pending = new TreeMap<>();

        Optional<V> get(K key) {
            Optional<V> p = pending.get(key);
            return p != null ? p : Optional.ofNullable(committed.get(key));
        }

        void put(K key, V value) {
            pending.put(key, Optional.of(value));
        }

        void delete(K key) {
            pending.put(key, Optional.empty());
        }

        /**
         * Every visible row whose value matches {@code filter}, in key order: a merge of the committed rows and this
         * view's writes, both already sorted, so nothing is copied.
         */
        List<V> where(Predicate<V> filter) {
            List<V> out = new ArrayList<>();
            Iterator<Map.Entry<K, V>> c = committed.entrySet().iterator();
            Iterator<Map.Entry<K, Optional<V>>> p = pending.entrySet().iterator();
            Map.Entry<K, V> ce = c.hasNext() ? c.next() : null;
            Map.Entry<K, Optional<V>> pe = p.hasNext() ? p.next() : null;
            while (ce != null || pe != null) {
                int cmp = ce == null ? 1 : pe == null ? -1 : ce.getKey().compareTo(pe.getKey());
                if (cmp < 0) {
                    if (filter.test(ce.getValue())) {
                        out.add(ce.getValue());
                    }
                    ce = c.hasNext() ? c.next() : null;
                } else {
                    if (pe.getValue().isPresent() && filter.test(pe.getValue().get())) {
                        out.add(pe.getValue().get());
                    }
                    if (cmp == 0) {
                        ce = c.hasNext() ? c.next() : null;
                    }
                    pe = p.hasNext() ? p.next() : null;
                }
            }
            return out;
        }

        /** Every visible row with {@code from <= key < to}, in key order (a merge of two sorted ranges). */
        List<V> range(K from, K to) {
            List<V> out = new ArrayList<>();
            Iterator<Map.Entry<K, V>> c = committed.subMap(from, true, to, false).entrySet().iterator();
            Iterator<Map.Entry<K, Optional<V>>> p = pending.subMap(from, true, to, false).entrySet().iterator();
            Map.Entry<K, V> ce = c.hasNext() ? c.next() : null;
            Map.Entry<K, Optional<V>> pe = p.hasNext() ? p.next() : null;
            while (ce != null || pe != null) {
                int cmp = ce == null ? 1 : pe == null ? -1 : ce.getKey().compareTo(pe.getKey());
                if (cmp < 0) {
                    out.add(ce.getValue());
                    ce = c.hasNext() ? c.next() : null;
                } else {
                    pe.getValue().ifPresent(out::add);
                    if (cmp == 0) {
                        ce = c.hasNext() ? c.next() : null;
                    }
                    pe = p.hasNext() ? p.next() : null;
                }
            }
            return out;
        }

        /** The first visible row with {@code from <= key < to}, if any (two ceiling lookups per step, no copying). */
        Optional<V> first(K from, K to) {
            K at = from;
            boolean inclusive = true;
            while (true) {
                Map.Entry<K, V> ce = inclusive ? committed.ceilingEntry(at) : committed.higherEntry(at);
                Map.Entry<K, Optional<V>> pe = pending.isEmpty() ? null
                        : inclusive ? pending.ceilingEntry(at) : pending.higherEntry(at);
                if (ce != null && ce.getKey().compareTo(to) >= 0) {
                    ce = null;
                }
                if (pe != null && pe.getKey().compareTo(to) >= 0) {
                    pe = null;
                }
                if (pe == null) {
                    return ce == null ? Optional.empty() : Optional.of(ce.getValue());
                }
                int cmp = ce == null ? 1 : ce.getKey().compareTo(pe.getKey());
                if (cmp < 0) {
                    return Optional.of(ce.getValue());
                }
                if (pe.getValue().isPresent()) {
                    return pe.getValue();
                }
                at = pe.getKey();      // a delete hides that key: look past it
                inclusive = false;
            }
        }

        /** The rows this view wrote (puts only), in key order. */
        List<V> written() {
            List<V> out = new ArrayList<>();
            for (Optional<V> v : pending.values()) {
                v.ifPresent(out::add);
            }
            return out;
        }

        void commit() {
            for (Map.Entry<K, Optional<V>> e : pending.entrySet()) {
                if (e.getValue().isPresent()) {
                    committed.put(e.getKey(), e.getValue().get());
                } else {
                    committed.remove(e.getKey());
                }
            }
        }
    }
}
