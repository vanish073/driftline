package io.github.vanish073.driftline.sim.world;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Every account in the simulated world: customers, the gateway's float and revenue, and each merchant (DESIGN §11.4).
 * Money only moves between accounts, so the total never changes after setup (INV-WORLD-CONSERVED). That checks the
 * simulator itself as well as Driftline.
 */
public final class FakeBank {

    /** A transfer the source account cannot cover. */
    public static final class InsufficientFunds extends IllegalStateException {
        public InsufficientFunds(String message) {
            super(message);
        }
    }

    private final TreeMap<String, Long> accounts = new TreeMap<>();
    private long opened;

    /** Opens an account with an opening balance (setup only); the opening balance counts towards the total. */
    public void open(String account, long paise) {
        if (paise < 0) {
            throw new IllegalArgumentException("an opening balance must not be negative");
        }
        if (accounts.putIfAbsent(account, paise) != null) {
            throw new IllegalArgumentException("account " + account + " is already open");
        }
        opened = Math.addExact(opened, paise);
    }

    public void transfer(String from, String to, long paise) {
        if (paise <= 0) {
            throw new IllegalArgumentException("a transfer must be positive, got " + paise);
        }
        long source = balance(from);
        balance(to);
        if (source < paise) {
            throw new InsufficientFunds(from + " holds " + source + " paise and cannot pay " + paise);
        }
        accounts.put(from, source - paise);
        accounts.merge(to, paise, Math::addExact);
    }

    public long balance(String account) {
        Long balance = accounts.get(account);
        if (balance == null) {
            throw new IllegalArgumentException("no account " + account);
        }
        return balance;
    }

    /** The sum of every balance now. */
    public long total() {
        long sum = 0;
        for (long balance : accounts.values()) {
            sum = Math.addExact(sum, balance);
        }
        return sum;
    }

    /** The sum of the opening balances: what {@link #total()} must always equal. */
    public long opened() {
        return opened;
    }

    public Map<String, Long> accounts() {
        return Collections.unmodifiableMap(accounts);
    }
}
