package io.github.vanish073.driftline.sim.invariant;

import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.world.FakeGateway;
import io.github.vanish073.driftline.sim.world.FakeMerchant;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The end-state oracle (DESIGN §11.8): the one-to-one breaks the final <em>world</em> state implies, derived from what
 * the fakes actually did rather than from the workload's scenario labels, because faults create breaks nobody
 * planned (a lost callback turns a normal order into DEBITED_NO_ORDER). Each expected break is
 * {@code "TYPE merchant/subject"}, the same key as a Driftline break.
 *
 * <ul>
 *   <li>LATE_SUCCESS: the gateway reported a failure, then captured the payment. A failure whose only webhook was
 *       lost (WEBHOOK_LOST) was never reported, and no API reveals it, so that payment is an ordinary success.</li>
 *   <li>DEBITED_NO_ORDER: captured, and the merchant's final order is paid with no payment at all.</li>
 *   <li>DUPLICATE_CAPTURE: an order with two or more captured payments; every one but the kept one (the payment the
 *       order is paid with, else the earliest capture, then the lower id) is a break.</li>
 *   <li>MISSING_SETTLEMENT: captured, and not settled in any file up to its due date plus grace, provided the world
 *       produced files that far; a payment refunded before it was settled is never settled, so it is not missing.</li>
 *   <li>SHORT_SETTLEMENT and ORPHAN_SETTLEMENT: every short line and every extra line the bank wrote.</li>
 * </ul>
 */
public final class Oracle {

    private Oracle() {
    }

    public static String key(BreakType type, MerchantId merchant, String subject) {
        return type.name() + " " + merchant.value() + "/" + subject;
    }

    /** The settlement date by which a payment captured at {@code captured} must be settled to not be missing. */
    public static LocalDate lastOnTimeDate(Instant captured, int cycleDays, int graceDays) {
        return IstCalendar.tDate(captured).plusDays(cycleDays).plusDays(graceDays);
    }

    public static TreeSet<String> expected(FakeGateway gateway, TreeMap<MerchantId, FakeMerchant> merchants,
                                           Instant epoch, int cycleDays, int graceDays, LocalDate lastFileDate) {
        TreeSet<String> out = new TreeSet<>();
        TreeMap<String, List<FakeGateway.Payment>> capturedByOrder = new TreeMap<>();
        for (FakeGateway.Payment p : gateway.payments().values()) {
            if (!p.captured()) {
                continue;
            }
            MerchantId m = p.merchant();
            String id = p.id().value();
            capturedByOrder.computeIfAbsent(m.value() + "/" + p.order().value(), k -> new ArrayList<>()).add(p);
            if (p.failedBeforeCapture() && p.failureReported()) {
                out.add(key(BreakType.LATE_SUCCESS, m, id));
            }
            Optional<PaymentId> paidWith = merchants.get(m).order(p.order()).flatMap(FakeMerchant.Order::paidWith);
            if (paidWith.isEmpty()) {
                out.add(key(BreakType.DEBITED_NO_ORDER, m, id));
            }
            Instant captured = epoch.plusMillis(p.capturedAtMillis());
            LocalDate deadline = lastOnTimeDate(captured, cycleDays, graceDays);
            boolean onTime = p.settledOn().filter(d -> !d.isAfter(deadline)).isPresent();
            boolean refundedUnsettled = !p.refunds().isEmpty() && p.settledOn().isEmpty();   // never settled, by design
            if (!onTime && !refundedUnsettled && !deadline.isAfter(lastFileDate)) {
                out.add(key(BreakType.MISSING_SETTLEMENT, m, id));
            }
        }
        for (List<FakeGateway.Payment> payments : capturedByOrder.values()) {
            if (payments.size() < 2) {
                continue;
            }
            FakeGateway.Payment first = payments.get(0);
            OrderId order = first.order();
            Optional<PaymentId> paidWith = merchants.get(first.merchant()).order(order)
                    .flatMap(FakeMerchant.Order::paidWith);
            FakeGateway.Payment kept = payments.stream()
                    .filter(p -> paidWith.filter(p.id()::equals).isPresent())
                    .findFirst()
                    .orElseGet(() -> payments.stream()
                            .min(Comparator.comparingLong(FakeGateway.Payment::capturedAtMillis)
                                    .thenComparing(p -> p.id().value()))
                            .orElseThrow());
            for (FakeGateway.Payment p : payments) {
                if (!p.id().equals(kept.id())) {
                    out.add(key(BreakType.DUPLICATE_CAPTURE, p.merchant(), p.id().value()));
                }
            }
        }
        for (String line : gateway.shortLines()) {
            out.add(BreakType.SHORT_SETTLEMENT.name() + " " + line);
        }
        for (String line : gateway.orphanLines()) {
            out.add(BreakType.ORPHAN_SETTLEMENT.name() + " " + line);
        }
        return out;
    }
}
