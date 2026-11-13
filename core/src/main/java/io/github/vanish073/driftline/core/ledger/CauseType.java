package io.github.vanish073.driftline.core.ledger;

/** Why a ledger transaction exists (DESIGN §7.2). With the cause id it is unique per merchant (§7.4). */
public enum CauseType {
    CAPTURE,
    ORDER_MATCH,
    REFUND,
    SETTLEMENT,
    SETTLEMENT_PARTIAL,
    SUSPENSE_IN,
    SUSPENSE_RELEASE,
    MANUAL_ADJUSTMENT
}
