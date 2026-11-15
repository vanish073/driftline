package io.github.vanish073.driftline.core.breaks;

/** What a finding is (DESIGN §8.2): a break with a playbook, an anomaly that is always escalated, or an alert. */
public enum BreakKind {
    BREAK,
    ANOMALY,
    ALERT
}
