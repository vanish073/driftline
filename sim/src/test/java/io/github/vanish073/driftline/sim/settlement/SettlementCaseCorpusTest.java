package io.github.vanish073.driftline.sim.settlement;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.SettlementFileEndPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.Line;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.Match;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileParser;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * The settlement case corpus (SHIFTS Shift 7; DESIGN §6.3–§6.5): {@code core/src/test/resources/settlement-cases}.
 * Each case lists the payments Driftline has seen, its settlement file(s) (inline lines, or a golden Settlement File
 * v1 CSV run through the real parser), the fee schedule, and the <b>expected result</b>: every line's status and the
 * payments it pays, and the exact set of breaks raised. Each case runs through the real {@link Matcher} on the
 * {@link InMemoryStore}: one batch with everything, then (unless the case says the watermark gate has not passed) a
 * tick in the evening of the last settlement date, then any events the case delivers after the solve.
 *
 * <p>Balances are as the ledger reports them: debits positive, credits negative (money held in SUSPENSE is a credit).
 *
 * <p>An AMBIGUOUS line's two witnesses are checked here too: distinct, each a set of the case's successful payments
 * whose expected nets sum to the line's amount (INV-AMBIGUITY-WITNESSED).
 */
class SettlementCaseCorpusTest {

    static final MerchantId M = MerchantId.of("m_0");
    static final YAMLMapper YAML = new YAMLMapper();

    /** The 35 required cases (SHIFTS Shift 7), by number. */
    static final List<String> REQUIRED = List.of("ref_single_exact", "ref_batch_exact",
            "ref_short_itemised_fee_explains", "ref_short_itemised_gst_rounding_explains", "ref_short_unitemised",
            "ref_short_itemised_not_summing", "ref_over_settlement", "ref_unknown_reference", "ref_already_settled",
            "noref_all_candidates_exact", "noref_unique_subset_small", "noref_unique_n40_mitm_boundary",
            "noref_n200_dp_after_gcd", "noref_budget_exceeded", "noref_two_subsets_distinct_amounts",
            "noref_equal_amounts_499xk", "noref_no_solution", "noref_no_candidates", "noref_gcd_reduction",
            "noref_target_not_divisible_by_gcd", "noref_zero_amount_line", "noref_single_candidate_equal",
            "noref_capture_at_ist_cutoff_boundary", "noref_lookback_includes_previous_day",
            "noref_excludes_already_matched", "noref_excludes_failed_payments",
            "multi_line_forced_propagation_resolves_both", "multi_line_propagation_leaves_ambiguous",
            "multi_line_jointly_infeasible", "noref_refund_netted", "mixed_file_ref_and_noref",
            "duplicate_bank_lines_same_amount_same_day", "largest_accepted_amounts", "watermark_gate_not_passed",
            "late_payment_after_inferred_match");

    static Path corpus() throws URISyntaxException {
        URL dir = SettlementCaseCorpusTest.class.getClassLoader().getResource("settlement-cases");
        assertThat(dir).as("the corpus is on the test classpath").isNotNull();
        return Path.of(dir.toURI());
    }

    static List<Path> caseFiles() throws IOException, URISyntaxException {
        try (Stream<Path> files = Files.list(corpus())) {
            return files.filter(p -> p.getFileName().toString().endsWith(".yaml")).sorted().toList();
        }
    }

    @Test
    void everyRequiredCaseIsPresentExactlyOnce() throws IOException, URISyntaxException {
        TreeMap<Integer, String> byNumber = new TreeMap<>();
        for (Path p : caseFiles()) {
            JsonNode c = YAML.readTree(Files.readAllBytes(p));
            if (c.has("case")) {
                String previous = byNumber.put(c.get("case").asInt(), c.get("name").asString());
                assertThat(previous).as("case %d is defined twice", c.get("case").asInt()).isNull();
                assertThat(p.getFileName().toString()).startsWith(String.format("%02d_", c.get("case").asInt()));
            }
        }
        assertThat(byNumber.keySet()).as("cases 1 to 35").hasSize(35).first().isEqualTo(1);
        for (int i = 1; i <= 35; i++) {
            assertThat(byNumber.get(i)).as("case %d", i).isEqualTo(REQUIRED.get(i - 1));
        }
    }

    @TestFactory
    Stream<DynamicTest> cases() throws IOException, URISyntaxException {
        return caseFiles().stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> run(p)));
    }

    // ---- one case ---------------------------------------------------------------------------------------------------

    /** The state of one run. */
    static final class Run {
        final JsonNode c;
        final Path dir;
        final FeeSchedule fees;
        final int cycleDays;
        final InMemoryStore store = new InMemoryStore();
        final List<LogRecord> log = new ArrayList<>();
        final TreeMap<String, Money> gross = new TreeMap<>();
        final TreeSet<String> succeeded = new TreeSet<>();
        final TreeMap<String, Instant> capturedAt = new TreeMap<>();
        Matcher matcher;
        long epoch;
        int pendingFrom;

        Run(JsonNode c, Path dir) {
            this.c = c;
            this.dir = dir;
            JsonNode f = c.path("fees");
            this.fees = new FeeSchedule(f.path("fee_bps").asLong(200), f.path("gst_bps").asLong(1_800));
            this.cycleDays = c.path("cycle_days").asInt(1);
        }

        void append(Source source, EventType type, Instant at, EventPayload payload) {
            String key = "k" + log.size();
            EventEnvelope e = new EventEnvelope(1, M, 0, source, key, type, at, at,
                    Sha256.of(key.getBytes(StandardCharsets.UTF_8)), payload, Optional.empty());
            log.add(new LogRecord(0, log.size(), e));
        }

        void batch(Instant now) {
            List<LogRecord> records = List.copyOf(log.subList(pendingFrom, log.size()));
            pendingFrom = log.size();
            BatchOutcome outcome = store.transact("batch",
                    tx -> matcher.process(new PartitionBatch(0, epoch, now, records), tx));
            matcher.committed(outcome);
        }

        void tick(Instant now) {
            BatchOutcome outcome = store.transact("tick", tx -> matcher.tick(0, epoch, now, tx));
            matcher.committed(outcome);
        }

        /**
         * Plays the gateway's captures API (D72): answers every open PAYMENT_SWEEP with this case's own captures of
         * that day, as of {@code asOf}. The case's payments are the whole world, so a sweep finds nothing new.
         */
        void answerSweeps(Instant asOf) {
            store.transact("sweep", tx -> {
                for (Command c : store.committedCommands()) {
                    if (c.kind() != CommandKind.PAYMENT_SWEEP
                            || tx.outbox().find(c.commandId()).filter(x -> x.status() != CommandStatus.DONE).isEmpty()) {
                        continue;
                    }
                    LocalDate day = LocalDate.parse(c.args().get("captured_on"));
                    TreeMap<String, String> answer = new TreeMap<>();
                    answer.put("as_of", asOf.toString());
                    int i = 0;
                    for (Map.Entry<String, Instant> e : capturedAt.entrySet()) {
                        if (IstCalendar.istDate(e.getValue()).equals(day)) {
                            i++;
                            answer.put("payment_" + i, e.getKey());
                            answer.put("order_" + i, "ord_" + e.getKey());
                            answer.put("amount_" + i, gross.get(e.getKey()).toString());
                            answer.put("at_" + i, e.getValue().toString());
                        }
                    }
                    answer.put("count", Integer.toString(i));
                    tx.outbox().complete(c.commandId(), answer);
                }
                return null;
            });
        }

        /** A successful payment (with its order created and paid, so no order break is raised) or a failed one. */
        void payment(JsonNode p) {
            String id = p.get("id").asString();
            Money amount = p.has("gross_paise") ? Money.ofPaise(p.get("gross_paise").asLong())
                    : Money.parse(p.get("gross").asString());
            Instant at = ist(p.get("captured").asString());
            OrderId order = OrderId.of("ord_" + id);
            gross.put(id, amount);
            boolean failed = "failed".equals(p.path("status").asString("succeeded"));
            if (failed) {
                append(Source.GATEWAY, EventType.PAYMENT_FAILED, at, new PaymentPayload(PaymentId.of(id), order, amount));
                return;
            }
            succeeded.add(id);
            capturedAt.put(id, at);
            append(Source.MERCHANT, EventType.ORDER_CREATED, at.minusSeconds(60),
                    new OrderPayload(order, 1, amount, at.plusSeconds(900), Optional.empty()));
            append(Source.GATEWAY, EventType.PAYMENT_SUCCEEDED, at, new PaymentPayload(PaymentId.of(id), order, amount));
            append(Source.MERCHANT, EventType.ORDER_PAID, at.plusSeconds(5),
                    new OrderPayload(order, 2, amount, at.plusSeconds(900), Optional.of(PaymentId.of(id))));
        }

        /** A settlement file: inline lines, or a golden CSV through the real parser. */
        LocalDate file(JsonNode f) throws IOException {
            if (f.has("csv")) {
                byte[] raw = Files.readAllBytes(dir.resolve(f.get("csv").asString()));
                SettlementFile parsed = SettlementFileParser.parse(M, raw);
                Instant at = IstCalendar.startOfIstDay(parsed.settlementDate()).plusSeconds(36_000);
                for (SettlementLinePayload l : parsed.lines()) {
                    append(Source.BANK, EventType.SETTLEMENT_LINE, at, l);
                }
                append(Source.BANK, EventType.SETTLEMENT_FILE_END, at, new SettlementFileEndPayload(parsed.fileId(),
                        parsed.settlementDate(), parsed.lines().size(), parsed.totalAmount(), Sha256.of(raw)));
                return parsed.settlementDate();
            }
            LocalDate date = LocalDate.parse(f.get("date").asString());
            String fileId = fileId(date);
            Instant at = IstCalendar.startOfIstDay(date).plusSeconds(36_000);
            Money total = Money.ZERO;
            int no = 0;
            for (JsonNode l : f.get("lines")) {
                no++;
                Money amount = Money.parse(l.get("amount").asString());
                total = total.plus(amount);
                append(Source.BANK, EventType.SETTLEMENT_LINE, at, new SettlementLinePayload(fileId, no, date, date,
                        amount, optionalMoney(l, "fee"), optionalMoney(l, "tax"),
                        Optional.ofNullable(l.path("reference").asString(null)),
                        Optional.of(l.path("batch_id").asString("UTR" + date.toString().replace("-", ""))),
                        "case line"));
            }
            append(Source.BANK, EventType.SETTLEMENT_FILE_END, at, new SettlementFileEndPayload(fileId, date, no, total,
                    Sha256.of(fileId.getBytes(StandardCharsets.UTF_8))));
            return date;
        }
    }

    static void run(Path path) throws IOException {
        JsonNode c = YAML.readTree(Files.readAllBytes(path));
        Run r = new Run(c, path.getParent());
        List<LocalDate> dates = new ArrayList<>();
        for (JsonNode f : c.get("files")) {
            dates.add(f.has("csv") ? null : LocalDate.parse(f.get("date").asString()));
        }
        LocalDate first = c.has("first_settlement_date") ? LocalDate.parse(c.get("first_settlement_date").asString())
                : dates.stream().filter(Objects::nonNull).min(LocalDate::compareTo).orElseThrow();
        r.matcher = new Matcher(new MerchantDirectory(List.of(new MerchantTerms(M, 0, r.fees, r.cycleDays, first))));
        r.epoch = r.store.transact("assign", tx -> tx.partitions().acquire(0, "matcher-a.1")).epoch();

        for (JsonNode p : c.path("payments")) {
            r.payment(p);
        }
        LocalDate last = first;
        for (JsonNode f : c.get("files")) {
            LocalDate d = r.file(f);
            last = d.isAfter(last) ? d : last;
        }
        r.batch(IstCalendar.startOfIstDay(last).plusSeconds(37_800));             // 10:30 IST on the last date
        if (!"not_passed".equals(c.path("watermark").asString("passed"))) {
            r.tick(IstCalendar.startOfIstDay(last).plusSeconds(72_000));           // 20:00 IST: idle, gate passed
            r.answerSweeps(IstCalendar.startOfIstDay(last).plusSeconds(72_000));   // the gateway lists the days' captures
            r.tick(IstCalendar.startOfIstDay(last).plusSeconds(72_180));           // 20:03 IST: the solve re-checks
        }
        if (c.has("after_solve")) {
            for (JsonNode p : c.get("after_solve").path("payments")) {
                r.payment(p);
            }
            r.batch(IstCalendar.startOfIstDay(last).plusSeconds(75_600));          // 21:00 IST
        }
        check(r, c.get("expect"));
    }

    static void check(Run r, JsonNode expect) {
        InMemoryStore s = r.store;
        TreeMap<String, Line> lines = new TreeMap<>();
        s.committedLines().forEach(l -> lines.put(l.key(), l));
        TreeMap<String, TreeSet<String>> paid = new TreeMap<>();
        for (Match m : s.committedMatches()) {
            paid.computeIfAbsent(m.fileId() + ":" + m.lineNo(), k -> new TreeSet<>()).add(m.paymentId().value());
        }

        JsonNode expectedLines = expect.get("lines");
        assertThat(new TreeSet<>(expectedLines.propertyNames())).as("every line, and only these, has an expectation")
                .isEqualTo(new TreeSet<>(lines.keySet()));
        for (Map.Entry<String, JsonNode> e : expectedLines.properties()) {
            Line l = lines.get(e.getKey());
            LineStatus status = LineStatus.valueOf(e.getValue().get("status").asString());
            assertThat(l.status()).as("status of %s", e.getKey()).isEqualTo(status);
            if (e.getValue().has("pays")) {
                TreeSet<String> want = new TreeSet<>();
                e.getValue().get("pays").forEach(n -> want.add(n.asString()));
                assertThat(paid.getOrDefault(e.getKey(), new TreeSet<>())).as("payments %s pays", e.getKey())
                        .isEqualTo(want);
            } else {
                assertThat(paid.getOrDefault(e.getKey(), new TreeSet<>())).as("%s pays nothing", e.getKey()).isEmpty();
            }
            if (status == LineStatus.AMBIGUOUS) {
                assertWitnessed(r, l);
            }
            if (status == LineStatus.INFERRED) {
                assertThat(l.candidateHash()).as("%s records its candidate hash", e.getKey()).isPresent();
            }
        }

        TreeMap<String, Break> breaks = new TreeMap<>();
        s.committedBreaks().forEach(b -> breaks.put(b.type() + " " + b.subjectKey(), b));
        TreeSet<String> wantBreaks = new TreeSet<>();
        for (JsonNode b : expect.path("breaks")) {
            String key = b.get("type").asString() + " " + b.get("subject").asString();
            wantBreaks.add(key);
            Break got = breaks.get(key);
            assertThat(got).as("break %s", key).isNotNull();
            if (b.has("reason")) {
                assertThat(got.evidence().get("reason")).as("reason of %s", key).isEqualTo(b.get("reason").asString());
            }
            if (b.has("state")) {
                assertThat(got.state().name()).as("state of %s", key).isEqualTo(b.get("state").asString());
            }
            b.path("evidence").forEachEntry((k, v) ->
                    assertThat(got.evidence().get(k)).as("evidence %s of %s", k, key).isEqualTo(v.asString()));
        }
        assertThat(new TreeSet<>(breaks.keySet())).as("exactly the expected breaks").isEqualTo(wantBreaks);

        expect.path("balances").forEachEntry((account, rupees) -> {
            Money balance = s.transact("read", tx -> tx.ledger().balance(M, AccountCode.valueOf(account)));
            assertThat(balance).as("balance of %s", account).isEqualTo(Money.parseSigned(rupees.asString()));
        });
    }

    /** INV-AMBIGUITY-WITNESSED for one line, against the case's own payments and fee schedule. */
    static void assertWitnessed(Run r, Line l) {
        assertThat(l.witnesses()).as("%s has two witnesses", l.key()).hasSize(2);
        assertThat(l.witnesses().get(0)).isNotEqualTo(l.witnesses().get(1));
        for (List<PaymentId> w : l.witnesses()) {
            Money sum = Money.ZERO;
            for (PaymentId p : w) {
                assertThat(r.succeeded).as("witness %s of %s names a successful payment", w, l.key())
                        .contains(p.value());
                sum = sum.plus(r.fees.expected(r.gross.get(p.value())).net());
            }
            assertThat(new TreeSet<>(w.stream().map(PaymentId::value).toList())).hasSize(w.size());
            assertThat(sum).as("witness %s of %s sums to the line", w, l.key()).isEqualTo(l.amount());
        }
    }

    // ---- helpers ----------------------------------------------------------------------------------------------------

    static String fileId(LocalDate date) {
        return "stl_" + date.toString().replace("-", "");
    }

    /** {@code 2026-11-13T10:00:00} read as IST. */
    static Instant ist(String localDateTime) {
        return LocalDateTime.parse(localDateTime).atZone(IstCalendar.ZONE).toInstant();
    }

    static Optional<Money> optionalMoney(JsonNode node, String field) {
        return node.has(field) ? Optional.of(Money.parse(node.get(field).asString())) : Optional.empty();
    }
}
