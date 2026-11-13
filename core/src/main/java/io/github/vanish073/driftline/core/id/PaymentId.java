package io.github.vanish073.driftline.core.id;

/** The gateway's payment identifier. A token of 1 to 64 characters from {@code [A-Za-z0-9._-]}. */
public record PaymentId(String value) implements Comparable<PaymentId> {

    public PaymentId {
        IdRules.requireToken("paymentId", value);
    }

    public static PaymentId of(String value) {
        return new PaymentId(value);
    }

    @Override
    public int compareTo(PaymentId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
