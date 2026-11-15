package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.core.id.MerchantId;
import java.util.Optional;
import org.springframework.security.core.Authentication;

/**
 * The merchant-binding check of §14, used from {@code @PreAuthorize}: a merchant token may act only on its own
 * merchant's path (a merchant-A token on merchant B is refused, the IDOR test); an ops token is not bound to a merchant
 * and is limited by its scopes alone.
 */
public final class MerchantAccess {

    public boolean allows(Authentication authentication, String merchantId) {
        if (!(authentication instanceof TokenAuthentication token)) {
            return false;
        }
        return token.merchantId().map(m -> m.value().equals(merchantId)).orElse(true);
    }

    /** The merchant a request is limited to: the token's own, or empty for an ops token (all merchants). */
    public static Optional<MerchantId> boundMerchant(Authentication authentication) {
        return authentication instanceof TokenAuthentication token ? token.merchantId() : Optional.empty();
    }
}
