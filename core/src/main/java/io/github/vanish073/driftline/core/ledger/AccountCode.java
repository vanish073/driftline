package io.github.vanish073.driftline.core.ledger;

/** The chart of accounts each merchant has (DESIGN §7.1). */
public enum AccountCode {
    /** Captured gross not yet settled, net of refunds. */
    GATEWAY_RECEIVABLE(AccountType.ASSET),
    /** Settled cash. */
    BANK(AccountType.ASSET),
    /** Customer money not yet tied to a paid order. */
    PAYMENTS_CLEARING(AccountType.LIABILITY),
    /** Payments matched to paid orders. */
    SALES(AccountType.INCOME),
    /** Fees deducted at settlement. */
    GATEWAY_FEES(AccountType.EXPENSE),
    /** GST paid on fees, which is claimable. */
    GST_INPUT_CREDIT(AccountType.ASSET),
    /** Bank money not yet attributed to a payment. */
    SUSPENSE(AccountType.LIABILITY),
    /** Used only through audited manual resolution. */
    SETTLEMENT_DIFFERENCES(AccountType.EXPENSE);

    private final AccountType type;

    AccountCode(AccountType type) {
        this.type = type;
    }

    public AccountType type() {
        return type;
    }
}
