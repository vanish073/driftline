package io.github.vanish073.driftline.core.ledger;

/** The kind of a ledger account (DESIGN §7.1). Debits are positive and credits negative for every kind. */
public enum AccountType {
    ASSET,
    LIABILITY,
    INCOME,
    EXPENSE
}
