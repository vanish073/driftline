package io.github.vanish073.driftline.core.id;

/** The merchant's order identifier. A token of 1 to 64 characters from {@code [A-Za-z0-9._-]}. */
public record OrderId(String value) implements Comparable<OrderId> {

    public OrderId {
        IdRules.requireToken("orderId", value);
    }

    public static OrderId of(String value) {
        return new OrderId(value);
    }

    @Override
    public int compareTo(OrderId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
