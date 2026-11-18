package io.github.vanish073.driftline.store.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The trace context of each break and outbox command (migration V11, D78): how a trace that began at ingest crosses
 * from the matcher to the resolver to the dispatcher, which may be separate processes. Production infrastructure
 * outside the core, like {@link SettlementInbox}: the simulator has no traces, so this is not a Store method. A
 * subject's first context wins; a later save of the same subject changes nothing.
 */
public final class TraceContexts {

    /** {@code brk_…} or {@code cmd_…}. */
    public static final Pattern SUBJECT = Pattern.compile("(brk|cmd)_[A-Za-z0-9_.-]{1,64}");
    /** A W3C traceparent, version 00. */
    public static final Pattern TRACEPARENT = Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

    private final DataSource dataSource;

    public TraceContexts(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Saves each subject's traceparent in one transaction; a subject already saved keeps its first one. */
    public void save(SortedMap<String, String> traceparentBySubject, Instant at) {
        for (Map.Entry<String, String> e : traceparentBySubject.entrySet()) {
            requireSubject(e.getKey());
            if (!TRACEPARENT.matcher(e.getValue()).matches()) {
                throw new IllegalArgumentException("not a version-00 traceparent");
            }
        }
        if (traceparentBySubject.isEmpty()) {
            return;
        }
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO trace_contexts (subject, traceparent, created_at) VALUES (?, ?, ?)
                    ON CONFLICT (subject) DO NOTHING
                    """)) {
                for (Map.Entry<String, String> e : traceparentBySubject.entrySet()) {
                    ps.setString(1, e.getKey());
                    ps.setString(2, e.getValue());
                    ps.setObject(3, OffsetDateTime.ofInstant(at, ZoneOffset.UTC));
                    ps.addBatch();
                }
                ps.executeBatch();
                c.commit();
            } catch (SQLException | RuntimeException failure) {
                try {
                    c.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate("save trace contexts", e);
        }
    }

    /** The saved traceparents of these subjects; a subject with none is absent. */
    public SortedMap<String, String> find(Collection<String> subjects) {
        TreeMap<String, String> out = new TreeMap<>();
        if (subjects.isEmpty()) {
            return out;
        }
        subjects.forEach(TraceContexts::requireSubject);
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT subject, traceparent FROM trace_contexts WHERE subject = ANY (?)")) {
            ps.setArray(1, c.createArrayOf("text", subjects.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getString(2));
                }
            }
            return out;
        } catch (SQLException e) {
            throw SqlErrors.translate("find trace contexts", e);
        }
    }

    private static void requireSubject(String subject) {
        if (!SUBJECT.matcher(subject).matches()) {
            throw new IllegalArgumentException("a trace subject is a break or command id");
        }
    }
}
