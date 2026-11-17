package io.github.vanish073.driftline.wire.webhook;

import io.github.vanish073.driftline.wire.json.JsonObject;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The customer's contact details a gateway webhook may carry (DESIGN §14, D77): UPI VPA, mobile number and email.
 * Personal data: it never enters an event, Kafka, the core or the simulator, only the encrypted vault (D77). Each field
 * is validated and normalised (VPA and email lower case, the mobile as its ten digits); a field that does not validate is
 * dropped rather than failing the webhook, since reconciliation does not need it. {@link #toString} never prints a
 * value.
 */
public final class CustomerContact {

    private static final Pattern VPA = Pattern.compile("[a-z0-9._-]{2,64}@[a-z][a-z0-9]{1,31}");
    private static final Pattern PHONE = Pattern.compile("(?:\\+?91)?([6-9][0-9]{9})");
    private static final Pattern EMAIL = Pattern.compile("[a-z0-9._%+-]{1,64}@[a-z0-9-]+(?:\\.[a-z0-9-]+)*\\.[a-z]{2,24}");

    private final Optional<String> vpa;
    private final Optional<String> phone;
    private final Optional<String> email;
    private final int dropped;

    private CustomerContact(Optional<String> vpa, Optional<String> phone, Optional<String> email, int dropped) {
        this.vpa = vpa;
        this.phone = phone;
        this.email = email;
        this.dropped = dropped;
    }

    /** From raw values; each is validated and normalised, and an invalid one is dropped (and counted). */
    public static CustomerContact of(Optional<String> vpa, Optional<String> phone, Optional<String> email) {
        int[] dropped = {0};
        Optional<String> v = vpa.map(x -> x.trim().toLowerCase(Locale.ROOT)).filter(x -> keep(VPA.matcher(x).matches(),
                dropped));
        Optional<String> p = phone.map(x -> x.replace(" ", "").replace("-", "")).flatMap(x -> {
            var m = PHONE.matcher(x);
            return keep(m.matches(), dropped) ? Optional.of(m.group(1)) : Optional.empty();
        });
        Optional<String> e = email.map(x -> x.trim().toLowerCase(Locale.ROOT)).filter(x -> x.length() <= 254
                && keep(EMAIL.matcher(x).matches(), dropped));
        return new CustomerContact(v, p, e, dropped[0]);
    }

    private static boolean keep(boolean valid, int[] dropped) {
        if (!valid) {
            dropped[0]++;
        }
        return valid;
    }

    /** The {@code customer} object of a webhook body. */
    static CustomerContact from(JsonObject customer) {
        return of(Optional.ofNullable(customer.optionalString("vpa")), Optional.ofNullable(customer.optionalString("phone")),
                Optional.ofNullable(customer.optionalString("email")));
    }

    public Optional<String> vpa() {
        return vpa;
    }

    public Optional<String> phone() {
        return phone;
    }

    public Optional<String> email() {
        return email;
    }

    /** How many fields were present but did not validate. */
    public int dropped() {
        return dropped;
    }

    public boolean isEmpty() {
        return vpa.isEmpty() && phone.isEmpty() && email.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CustomerContact c && c.vpa.equals(vpa) && c.phone.equals(phone) && c.email.equals(email);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vpa, phone, email);
    }

    /** Never the values: personal data does not belong in logs or exception messages. */
    @Override
    public String toString() {
        return "CustomerContact[vpa=" + (vpa.isPresent() ? "present" : "absent") + ", phone="
                + (phone.isPresent() ? "present" : "absent") + ", email=" + (email.isPresent() ? "present" : "absent")
                + "]";
    }
}
