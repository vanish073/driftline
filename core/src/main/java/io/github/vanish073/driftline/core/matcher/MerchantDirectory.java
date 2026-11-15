package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * The merchants the matcher serves, by id (immutable). Production loads it from {@code merchants} at startup; the
 * simulator builds it from the run's configuration.
 */
public final class MerchantDirectory {

    private final TreeMap<MerchantId, MerchantTerms> terms = new TreeMap<>();

    public MerchantDirectory(List<MerchantTerms> merchants) {
        for (MerchantTerms t : merchants) {
            if (terms.putIfAbsent(t.merchantId(), t) != null) {
                throw new IllegalArgumentException("merchant " + t.merchantId() + " is listed twice");
            }
        }
    }

    /** @throws IllegalArgumentException for a merchant Driftline does not serve */
    public MerchantTerms terms(MerchantId merchantId) {
        MerchantTerms t = terms.get(merchantId);
        if (t == null) {
            throw new IllegalArgumentException("unknown merchant " + merchantId);
        }
        return t;
    }

    /** The merchants of one partition, by id. */
    public List<MerchantTerms> inPartition(int partition) {
        List<MerchantTerms> out = new ArrayList<>();
        for (MerchantTerms t : terms.values()) {
            if (t.partition() == partition) {
                out.add(t);
            }
        }
        return List.copyOf(out);
    }
}
