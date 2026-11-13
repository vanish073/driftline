package io.github.vanish073.driftline.core.ledger;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import java.util.Objects;

/** One line of a ledger transaction: a debit (positive) or a credit (negative) to one merchant account. */
public record Entry(MerchantId merchantId, AccountCode account, Money amount) {

    public Entry {
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(account, "account");
        Objects.requireNonNull(amount, "amount");
    }

    public static Entry debit(MerchantId merchantId, AccountCode account, Money amount) {
        return new Entry(merchantId, account, amount);
    }

    public static Entry credit(MerchantId merchantId, AccountCode account, Money amount) {
        return new Entry(merchantId, account, amount.negate());
    }
}
