package io.github.vanish073.driftline.core.id;

/** A merchant onboarded to Driftline. A token of 1 to 64 characters from {@code [A-Za-z0-9._-]}. */
public record MerchantId(String value) implements Comparable<MerchantId> {

    public MerchantId {
        IdRules.requireToken("merchantId", value);
    }

    public static MerchantId of(String value) {
        return new MerchantId(value);
    }

    @Override
    public int compareTo(MerchantId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
