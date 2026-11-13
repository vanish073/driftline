package io.github.vanish073.driftline.core.hooks;

/** The named points where the simulator and integration tests crash a component on purpose (DESIGN §9.4). */
public enum CrashPoint {
    CP1("resolver: decided, before commit"),
    CP2("resolver: committed, before any claim"),
    CP3("dispatcher: claim committed, before send"),
    CP4("dispatcher: sent, the gateway applied it, response lost"),
    CP5("dispatcher: result received, before the record commits"),
    CP6("matcher: mid-batch, before commit"),
    CP7("matcher: zombie after reassignment"),
    CP8("settlement worker: mid-emit");

    private final String where;

    CrashPoint(String where) {
        this.where = where;
    }

    public String where() {
        return where;
    }
}
