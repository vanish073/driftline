-- Shift 7: batch settlement matching (DESIGN §6.4).
--
-- An unreferenced line ends INFERRED (a unique subset of the candidates fits: MATCHED_INFERRED), or AMBIGUOUS,
-- NO_SOLUTION, BUDGET_EXCEEDED or OUT_OF_RANGE (an orphan held in suspense), or ZERO_AMOUNT (rejected and flagged).
-- An INFERRED line records the SHA-256 of the candidate set it was proved over; no other line has one.

ALTER TABLE settlement_lines DROP CONSTRAINT settlement_lines_status_check;
ALTER TABLE settlement_lines ADD CONSTRAINT settlement_lines_status_check CHECK (status IN (
    'MATCHED', 'SHORT_EXPLAINED', 'SHORT_UNEXPLAINED', 'OVER', 'CONFLICT', 'UNATTRIBUTED', 'AWAITING_BATCH',
    'INFERRED', 'AMBIGUOUS', 'NO_SOLUTION', 'BUDGET_EXCEEDED', 'ZERO_AMOUNT', 'OUT_OF_RANGE'));

ALTER TABLE settlement_lines ADD COLUMN candidate_hash text CHECK (candidate_hash ~ '^[0-9a-f]{64}$');
ALTER TABLE settlement_lines ADD CONSTRAINT settlement_lines_inferred_has_hash
    CHECK ((status = 'INFERRED') = (candidate_hash IS NOT NULL));

-- An AMBIGUOUS line keeps its two witnesses: payment ids joined by ',' within a witness and ';' between the two
-- (ids are tokens of [A-Za-z0-9._-], so neither separator can occur in one). No other line has witnesses.
ALTER TABLE settlement_lines ADD COLUMN witnesses text
    CHECK (witnesses ~ '^[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*;[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*$');
ALTER TABLE settlement_lines ADD CONSTRAINT settlement_lines_ambiguous_has_witnesses
    CHECK ((status = 'AMBIGUOUS') = (witnesses IS NOT NULL));

-- A payment settled by an inferred line.
ALTER TABLE settlement_matches DROP CONSTRAINT settlement_matches_how_check;
ALTER TABLE settlement_matches ADD CONSTRAINT settlement_matches_how_check CHECK (how IN (
    'REFERENCED', 'SHORT_EXPLAINED', 'SHORT_PARTIAL', 'OVER_HELD', 'INFERRED'));
