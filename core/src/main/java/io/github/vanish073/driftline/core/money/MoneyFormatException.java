package io.github.vanish073.driftline.core.money;

/** Thrown when text is not a valid rupee amount under {@link Money}'s strict grammar. */
public final class MoneyFormatException extends IllegalArgumentException {

    public MoneyFormatException(String text, String reason) {
        super("Invalid amount \"" + text + "\": " + reason);
    }
}
