package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.payment.PaymentState;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The matcher's write-through cache of open payments and orders (DESIGN §5.2). The Store stays the source of truth;
 * the cache only saves reads. It is safe under rollback and fencing because of one rule: a key written in a batch is
 * removed from the cache at once, and the cache is refilled only from a {@link Delta} the driver applies <em>after</em>
 * the batch's transaction committed. A batch that rolls back (a crash, a store outage, a fenced zombie) therefore
 * leaves only missing entries, which read through to the Store, never stale ones.
 *
 * <p>Values read from the Store for keys the batch has not written are committed values, so they are cached on read.
 * Closed payments (settled) and closed orders (fulfilled, expired, cancelled) are evicted. A partition's entries are
 * dropped whenever its ownership changes (acquire, loss, crash), because another owner may have written them since.
 */
public final class KeyedState {

    /** Writes of one committed batch, to apply to the cache. Opaque outside the matcher. */
    public static final class Delta {
        private final List<Entry> entries;

        private Delta(List<Entry> entries) {
            this.entries = List.copyOf(entries);
        }

        static final Delta EMPTY = new Delta(List.of());

        public int size() {
            return entries.size();
        }
    }

    private record Entry(int partition, String key, Optional<PaymentState> payment, Optional<OrderState> order) {
    }

    private final TreeMap<String, PaymentState> payments = new TreeMap<>();
    private final TreeMap<String, OrderState> orders = new TreeMap<>();
    private final TreeMap<String, Integer> partitionOf = new TreeMap<>();
    private long hits;
    private long misses;

    static String paymentKey(MerchantId m, PaymentId id) {
        return "p/" + m.value() + "/" + id.value();
    }

    static String orderKey(MerchantId m, OrderId id) {
        return "o/" + m.value() + "/" + id.value();
    }

    static boolean isOpen(OrderState o) {
        return o.status() == OrderStatus.CREATED || o.status() == OrderStatus.PENDING_PAYMENT
                || o.status() == OrderStatus.PAID;
    }

    Optional<PaymentState> payment(String key) {
        PaymentState p = payments.get(key);
        if (p != null) {
            hits++;
        } else {
            misses++;
        }
        return Optional.ofNullable(p);
    }

    Optional<OrderState> order(String key) {
        OrderState o = orders.get(key);
        if (o != null) {
            hits++;
        } else {
            misses++;
        }
        return Optional.ofNullable(o);
    }

    void cachePayment(int partition, String key, PaymentState p) {
        payments.put(key, p);
        partitionOf.put(key, partition);
    }

    void cacheOrder(int partition, String key, OrderState o) {
        if (isOpen(o)) {
            orders.put(key, o);
            partitionOf.put(key, partition);
        }
    }

    /** A write is about to happen: the cached value, if any, is no longer known to be committed. */
    void invalidate(String key) {
        payments.remove(key);
        orders.remove(key);
        partitionOf.remove(key);
    }

    /** Collects the final writes of one batch. */
    static final class Recorder {
        private final TreeMap<String, Entry> writes = new TreeMap<>();

        void payment(int partition, String key, PaymentState p, boolean open) {
            writes.put(key, new Entry(partition, key, open ? Optional.of(p) : Optional.empty(), Optional.empty()));
        }

        void order(int partition, String key, OrderState o) {
            writes.put(key, new Entry(partition, key, Optional.empty(), isOpen(o) ? Optional.of(o) : Optional.empty()));
        }

        /** The payment was settled in this batch: evict it whatever was written. */
        void close(int partition, String key) {
            writes.put(key, new Entry(partition, key, Optional.empty(), Optional.empty()));
        }

        Delta delta() {
            return writes.isEmpty() ? Delta.EMPTY : new Delta(new ArrayList<>(writes.values()));
        }
    }

    /** Applies a committed batch's writes. */
    public void apply(Delta delta) {
        for (Entry e : delta.entries) {
            invalidate(e.key());
            e.payment().ifPresent(p -> cachePayment(e.partition(), e.key(), p));
            e.order().ifPresent(o -> cacheOrder(e.partition(), e.key(), o));
        }
    }

    /** Drops every entry of {@code partition} (ownership changed). */
    public void dropPartition(int partition) {
        List<String> drop = new ArrayList<>();
        partitionOf.forEach((key, p) -> {
            if (p == partition) {
                drop.add(key);
            }
        });
        drop.forEach(this::invalidate);
    }

    /** Drops everything (the process crashed and lost its memory). */
    public void clear() {
        payments.clear();
        orders.clear();
        partitionOf.clear();
    }

    public int size() {
        return payments.size() + orders.size();
    }

    public long hits() {
        return hits;
    }

    public long misses() {
        return misses;
    }
}
