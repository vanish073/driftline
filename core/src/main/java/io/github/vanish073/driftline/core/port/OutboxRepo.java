package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * The transactional outbox (DESIGN §9, §12): the resolver writes a command in the same transaction as the decision
 * that needs it, and the dispatcher claims, sends and records it. Results are facts, recorded once whatever attempt
 * produced them; the lease only decides who may <em>send</em> (§9.2).
 */
public interface OutboxRepo {

    /** The command kinds of §9.1. */
    enum CommandKind {
        STATUS_QUERY_PAYMENT,
        STATUS_QUERY_ORDER,
        REFUND,
        REFUND_LOOKUP,
        MERCHANT_CONFIRM,
        PUBLISH_OBSERVATION,
        ESCALATION_NOTIFY
    }

    enum CommandStatus {
        /** Waiting to be claimed at {@code nextAttemptAt}. */
        PENDING,
        /** Claimed and (being) sent; claimable again once the lease runs out (a timeout, §9.2 step 4). */
        IN_FLIGHT,
        /** Its result is recorded. Nothing sends it again. */
        DONE
    }

    /**
     * One command. {@code args} and {@code result} are small sorted maps of strings, like break evidence: the call's
     * inputs (payment id, amount, order id) and the facts that came back.
     *
     * @param breakId     the break the command serves; empty for a pending-order probe, which has no break yet
     * @param attempt     how many times it has been claimed
     * @param firstSentAt when it was first claimed to be sent; a REFUND older than the gateway's key TTL is looked up
     *                    before it is sent again (§9.2 step 6)
     *
     * <p>Instants are kept to the microsecond, as PostgreSQL stores them.
     */
    record Command(String commandId, MerchantId merchantId, Optional<String> breakId, CommandKind kind,
                   SortedMap<String, String> args, Optional<String> idempotencyKey, CommandStatus status, int attempt,
                   Optional<String> leaseOwner, Optional<Instant> leaseUntil, Instant nextAttemptAt,
                   Optional<Instant> firstSentAt, Optional<SortedMap<String, String>> result) {

        public Command {
            IdRules.requireToken("commandId", commandId);
            Objects.requireNonNull(merchantId, "merchantId");
            Objects.requireNonNull(breakId, "breakId");
            breakId.ifPresent(b -> IdRules.requireToken("breakId", b));
            Objects.requireNonNull(kind, "kind");
            args = frozen(args);
            Objects.requireNonNull(idempotencyKey, "idempotencyKey");
            idempotencyKey.ifPresent(k -> IdRules.requireKey("idempotencyKey", k));
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(leaseOwner, "leaseOwner");
            leaseUntil = Objects.requireNonNull(leaseUntil, "leaseUntil").map(t -> t.truncatedTo(ChronoUnit.MICROS));
            nextAttemptAt = Objects.requireNonNull(nextAttemptAt, "nextAttemptAt").truncatedTo(ChronoUnit.MICROS);
            firstSentAt = Objects.requireNonNull(firstSentAt, "firstSentAt").map(t -> t.truncatedTo(ChronoUnit.MICROS));
            result = Objects.requireNonNull(result, "result").map(OutboxRepo::frozen);
            if (attempt < 0) {
                throw new IllegalArgumentException("attempt must not be negative");
            }
            if (leaseOwner.isPresent() != leaseUntil.isPresent()) {
                throw new IllegalArgumentException("a lease has both an owner and an end, or neither");
            }
            if ((status == CommandStatus.DONE) != result.isPresent()) {
                throw new IllegalArgumentException("exactly the DONE commands carry a result");
            }
            if (status == CommandStatus.IN_FLIGHT && leaseOwner.isEmpty()) {
                throw new IllegalArgumentException("an IN_FLIGHT command is leased");
            }
            if ((attempt == 0) == firstSentAt.isPresent()) {
                throw new IllegalArgumentException("a command has a first send time exactly when it has been claimed");
            }
        }

        /** A new command, PENDING from {@code at}; {@code breakId} is empty for a pending-order probe (§9.2). */
        public static Command pending(String commandId, MerchantId merchantId, Optional<String> breakId, CommandKind kind,
                                      Map<String, String> args, Optional<String> idempotencyKey, Instant at) {
            return new Command(commandId, merchantId, breakId, kind, new TreeMap<>(args), idempotencyKey,
                    CommandStatus.PENDING, 0, Optional.empty(), Optional.empty(), at, Optional.empty(), Optional.empty());
        }
    }

    private static SortedMap<String, String> frozen(SortedMap<String, String> map) {
        Objects.requireNonNull(map, "map");
        TreeMap<String, String> copy = new TreeMap<>();
        map.forEach((k, v) -> {
            IdRules.requireToken("key", k);
            copy.put(k, Objects.requireNonNull(v, "value"));
        });
        return Collections.unmodifiableSortedMap(copy);
    }

    /**
     * Stores a new command (it must be PENDING with no attempt), unless one with that id exists. A command that names a
     * break needs that break to exist.
     *
     * @throws IllegalArgumentException if it names a break that does not exist
     *
     * @return true if it was stored; false if the id was already there, which changes nothing
     */
    boolean enqueue(Command command);

    Optional<Command> find(String commandId);

    /** The break's commands, by command id. */
    List<Command> forBreak(MerchantId merchantId, String breakId);

    /**
     * Claims up to {@code limit} commands that are due: not DONE, {@code nextAttemptAt ≤ now}, and not leased or leased
     * until before {@code now}; oldest {@code nextAttemptAt} first, then command id. Each becomes IN_FLIGHT, leased to
     * {@code owner} until {@code leaseUntil}, with its attempt counted, {@code firstSentAt} set if it was empty, and
     * {@code nextAttemptAt} set to {@code retryAt} of the command as claimed (when it may be claimed again if no result
     * comes: the dispatcher's backoff, which depends on the attempt). PostgreSQL uses {@code FOR UPDATE SKIP LOCKED},
     * so two dispatchers never claim the same row in one moment.
     *
     * @return the claimed commands as they are now stored
     */
    List<Command> claim(Instant now, String owner, Instant leaseUntil, Function<Command, Instant> retryAt, int limit);

    /**
     * Records the command's result and makes it DONE, unless it is already DONE: the first result wins, whatever
     * attempt or dispatcher produced it.
     *
     * @return true if this call recorded it
     * @throws IllegalArgumentException if there is no such command
     */
    boolean complete(String commandId, Map<String, String> result);

    /**
     * Makes a command that is not DONE PENDING again from {@code nextAttemptAt}, with no lease: an explicit retry.
     *
     * @throws IllegalArgumentException if there is no such command
     */
    void retryAt(String commandId, Instant nextAttemptAt);
}
