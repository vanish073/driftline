package io.github.vanish073.driftline.core.dispatcher;

import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.core.port.Tx;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The dispatcher (DESIGN §9.2), sans-I/O: {@link #claim} leases due outbox commands and turns them into calls for the
 * driver to send; {@link #onResult} records what came back. Results are facts, applied once whatever attempt produced
 * them; the lease decides only who may send.
 *
 * <ul>
 *   <li><b>Timeout.</b> A claim sets the lease to {@code callTimeout} and the next attempt after it plus a deterministic
 *       backoff, {@code min(cap, base × 2^attempt) + H(commandId, attempt) mod base}: a call with no result is claimed
 *       again then, with the same idempotency key.</li>
 *   <li><b>Old refunds.</b> A REFUND attempt whose first send is older than {@code gatewayKeyTtl − lookupMargin} sends a
 *       REFUND_LOOKUP first, because the gateway may have forgotten the key; the refund is resent only if no refund
 *       exists, and only while this dispatcher still holds the lease.</li>
 *   <li><b>New facts</b> (a status the matcher has not seen, a refund outcome) are published as observations, so they
 *       enter through Kafka like every other event (D10); their event keys are derived, so repeats are deduplicated.</li>
 * </ul>
 *
 * <p>Crash point CP5 sits after a result is recorded and before the caller commits: a crash there loses the result, and
 * the retry (or the lookup) finds the same outcome again.
 */
public final class Dispatcher {

    /**
     * @param callTimeout   how long a sent call may go unanswered before it is retried
     * @param gatewayKeyTtl how long the gateway honours an idempotency key
     * @param lookupMargin  how much earlier than the TTL a retry looks up first
     */
    public record Config(Duration callTimeout, Duration backoffBase, Duration backoffCap, Duration gatewayKeyTtl,
                         Duration lookupMargin, int claimLimit) {
        public static final Config DEFAULT = new Config(Duration.ofSeconds(30), Duration.ofSeconds(5),
                Duration.ofMinutes(10), Duration.ofHours(24), Duration.ofHours(1), 20);

        public Config {
            for (Duration d : new Duration[] {callTimeout, backoffBase, backoffCap, gatewayKeyTtl, lookupMargin}) {
                if (d.isNegative() || d.isZero()) {
                    throw new IllegalArgumentException("durations must be positive");
                }
            }
            if (lookupMargin.compareTo(gatewayKeyTtl) >= 0) {
                throw new IllegalArgumentException("the lookup margin must be shorter than the key TTL");
            }
            if (claimLimit < 1) {
                throw new IllegalArgumentException("claim at least one command");
            }
        }
    }

    private final Config config;
    private final CrashPoints crashPoints;
    private final boolean markBeforeSend;
    private final boolean skipTtlLookup;

    public Dispatcher(Config config, Canaries canaries, CrashPoints crashPoints) {
        this.config = Objects.requireNonNull(config, "config");
        this.crashPoints = Objects.requireNonNull(crashPoints, "crashPoints");
        this.markBeforeSend = canaries.isEnabled(Canary.MARK_BEFORE_SEND);
        this.skipTtlLookup = canaries.isEnabled(Canary.SKIP_TTL_LOOKUP);
    }

    /** The production dispatcher: default constants, no canary, no crash point. */
    public Dispatcher() {
        this(Config.DEFAULT, Canaries.NONE, CrashPoints.NONE);
    }

    public Config config() {
        return config;
    }

    /** The deterministic backoff after a claim (§9.2 step 5): no RNG, so a seed replays it exactly. */
    public Duration backoff(Command c) {
        long base = config.backoffBase().toMillis();
        long exp;
        try {
            exp = c.attempt() >= 62 ? Long.MAX_VALUE : Math.multiplyExact(base, 1L << c.attempt());
        } catch (ArithmeticException e) {
            exp = Long.MAX_VALUE;
        }
        long capped = Math.min(config.backoffCap().toMillis(), exp);
        String text = c.commandId() + ":" + c.attempt();
        long h = Long.parseLong(Sha256.of(text.getBytes(StandardCharsets.UTF_8)).hex().substring(0, 15), 16);
        return Duration.ofMillis(capped + h % base);
    }

    /** Leases due commands to {@code owner} and returns the calls to send now (§9.2 steps 1–2). */
    public List<OutboundCall> claim(Tx tx, Instant now, String owner) {
        Instant leaseUntil = now.plus(config.callTimeout());
        List<Command> claimed = tx.outbox().claim(now, owner, leaseUntil,
                c -> leaseUntil.plus(backoff(c)), config.claimLimit());
        List<OutboundCall> calls = new ArrayList<>();
        for (Command c : claimed) {
            if (markBeforeSend && c.kind() == CommandKind.REFUND) {
                tx.outbox().complete(c.commandId(), kv("status", "MARKED_BEFORE_SEND"));   // canary C03
            }
            calls.add(lookupFirst(c, now) ? lookupCall(c) : call(c));
        }
        return calls;
    }

    /** §9.2 step 6: is this a REFUND retry old enough that the gateway may have forgotten its key? */
    boolean lookupFirst(Command c, Instant now) {
        return !skipTtlLookup && c.kind() == CommandKind.REFUND && c.attempt() >= 2
                && !c.firstSentAt().orElseThrow().isAfter(now.minus(config.gatewayKeyTtl().minus(config.lookupMargin())));
    }

    /**
     * Records a call's result (§9.2 step 3) and returns any call to send at once: only the refund itself, after a lookup
     * found none and while {@code owner} still holds the command's lease.
     */
    public List<OutboundCall> onResult(Tx tx, OutboundCall call, Map<String, String> facts, Instant now, String owner) {
        String commandId = call.callId().substring(0, call.callId().indexOf('.'));
        Optional<Command> found = tx.outbox().find(commandId);
        List<OutboundCall> sendNow = new ArrayList<>();
        if (found.isEmpty()) {
            return sendNow;
        }
        Command c = found.get();
        if (call.request() instanceof CallRequest.RefundLookup) {
            if ("yes".equals(facts.get("found"))) {
                TreeMap<String, String> result = kv("status", "PROCESSED", "via", "lookup");
                Optional.ofNullable(facts.get("refund_id")).ifPresent(r -> result.put("refund_id", r));
                Optional.ofNullable(facts.get("processed_at")).ifPresent(t -> result.put("processed_at", t));
                if (tx.outbox().complete(commandId, result)) {
                    followUps(tx, c, result, now);
                }
            } else if (c.status() == CommandStatus.IN_FLIGHT && c.leaseOwner().filter(owner::equals).isPresent()
                    && c.leaseUntil().filter(now::isBefore).isPresent()) {
                sendNow.add(call(c));   // no refund exists: send it now, under this lease
            }
        } else if (tx.outbox().complete(commandId, facts)) {
            followUps(tx, c, facts, now);
        }
        crashPoints.reached(CrashPoint.CP5);
        return sendNow;
    }

    /** §9.2 step 7: publish new facts as observations, then wake the break. */
    private void followUps(Tx tx, Command c, Map<String, String> facts, Instant now) {
        MerchantId m = c.merchantId();
        switch (c.kind()) {
            case STATUS_QUERY_PAYMENT -> {
                String status = facts.getOrDefault("status", "");
                PaymentId p = PaymentId.of(c.args().get("payment_id"));
                Optional<PaymentStatus> known = tx.payments().find(m, p).map(PaymentState::status);
                if ((status.equals("SUCCEEDED") || status.equals("FAILED")) && facts.containsKey("order_id")
                        && facts.containsKey("amount") && facts.containsKey("at")
                        && known.filter(k -> k.name().equals(status)).isEmpty()) {
                    EventType type = status.equals("SUCCEEDED") ? EventType.PAYMENT_SUCCEEDED : EventType.PAYMENT_FAILED;
                    observe(tx, c, type, Instant.parse(facts.get("at")), new PaymentPayload(p,
                            OrderId.of(facts.get("order_id")), Money.parse(facts.get("amount"))), now);
                }
            }
            case REFUND -> {
                String status = facts.getOrDefault("status", "");
                if (status.equals("PROCESSED") || status.equals("FAILED")) {
                    PaymentId p = PaymentId.of(c.args().get("payment_id"));
                    Instant at = Optional.ofNullable(facts.get("processed_at")).map(Instant::parse).orElse(now);
                    observe(tx, c, status.equals("PROCESSED") ? EventType.REFUND_PROCESSED : EventType.REFUND_FAILED, at,
                            new RefundPayload(p, Money.parse(c.args().get("amount"))), now);
                }
            }
            default -> {
                // MERCHANT_CONFIRM, observations and notifications: the recorded result is all there is
            }
        }
        c.breakId().ifPresent(b -> tx.breaks().wake(m, b, now));
    }

    /** Enqueues a PUBLISH_OBSERVATION whose event key and command id are derived from the fact, so it exists once. */
    private void observe(Tx tx, Command source, EventType type, Instant at, EventPayload payload, Instant now) {
        MerchantId m = source.merchantId();
        String subject = switch (payload) {
            case PaymentPayload p -> p.paymentId().value();
            case RefundPayload r -> r.paymentId().value();
            default -> throw new IllegalArgumentException("no observation of " + payload);
        };
        String eventKey = "obs_" + DerivedId.of("observation", m.value(), type.name(), subject).value();
        TreeMap<String, String> args = kv("event_type", type.name(), "event_key", eventKey, "event_time", at.toString(),
                "payment_id", subject);
        switch (payload) {
            case PaymentPayload p -> {
                args.put("order_id", p.orderId().value());
                args.put("amount", p.amount().toString());
            }
            case RefundPayload r -> args.put("amount", r.amount().toString());
            default -> throw new IllegalArgumentException("no observation of " + payload);
        }
        String id = "cmd_" + DerivedId.of("command", "observation", eventKey).value();
        tx.outbox().enqueue(Command.pending(id, m, source.breakId(), CommandKind.PUBLISH_OBSERVATION, args,
                Optional.empty(), now));
    }

    // ---- commands as calls ----

    private static String callId(Command c, String suffix) {
        return c.commandId() + "." + c.attempt() + suffix;
    }

    private static OutboundCall lookupCall(Command c) {
        return new OutboundCall(callId(c, ".lookup"), c.merchantId(), CallTarget.GATEWAY, Optional.empty(),
                new CallRequest.RefundLookup(PaymentId.of(c.args().get("payment_id"))));
    }

    /** The call for a command, as {@code commandId.attempt}, so a late reply still finds its command. */
    public static OutboundCall call(Command c) {
        Map<String, String> a = c.args();
        return switch (c.kind()) {
            case STATUS_QUERY_PAYMENT -> new OutboundCall(callId(c, ""), c.merchantId(), CallTarget.GATEWAY,
                    Optional.empty(), new CallRequest.StatusQuery(PaymentId.of(a.get("payment_id"))));
            case REFUND -> new OutboundCall(callId(c, ""), c.merchantId(), CallTarget.GATEWAY, c.idempotencyKey(),
                    new CallRequest.Refund(PaymentId.of(a.get("payment_id")), OrderId.of(a.get("order_id")),
                            Money.parse(a.get("amount"))));
            case REFUND_LOOKUP -> new OutboundCall(callId(c, ""), c.merchantId(), CallTarget.GATEWAY, Optional.empty(),
                    new CallRequest.RefundLookup(PaymentId.of(a.get("payment_id"))));
            case MERCHANT_CONFIRM -> new OutboundCall(callId(c, ""), c.merchantId(), CallTarget.MERCHANT,
                    c.idempotencyKey(), new CallRequest.MerchantConfirm(OrderId.of(a.get("order_id")),
                            PaymentId.of(a.get("payment_id")), Money.parse(a.get("amount"))));
            case PUBLISH_OBSERVATION -> {
                EventType type = EventType.valueOf(a.get("event_type"));
                PaymentId p = PaymentId.of(a.get("payment_id"));
                EventPayload payload = type == EventType.REFUND_PROCESSED || type == EventType.REFUND_FAILED
                        ? new RefundPayload(p, Money.parse(a.get("amount")))
                        : new PaymentPayload(p, OrderId.of(a.get("order_id")), Money.parse(a.get("amount")));
                yield new OutboundCall(callId(c, ""), c.merchantId(), CallTarget.DRIFTLINE, Optional.empty(),
                        new CallRequest.Observation(type, a.get("event_key"), Instant.parse(a.get("event_time")),
                                payload));
            }
            case ESCALATION_NOTIFY -> new OutboundCall(callId(c, ""), c.merchantId(), CallTarget.OPS, Optional.empty(),
                    new CallRequest.EscalationNotify(c.breakId().orElseThrow(), BreakType.valueOf(a.get("type")),
                            a.get("subject"), a.get("reason")));
            case STATUS_QUERY_ORDER -> throw new IllegalStateException("STATUS_QUERY_ORDER is not used in v1");
        };
    }

    /** Key-value pairs as a sorted map (the core never uses Map.of). */
    private static TreeMap<String, String> kv(String... pairs) {
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(pairs[i], pairs[i + 1]);
        }
        return out;
    }
}
