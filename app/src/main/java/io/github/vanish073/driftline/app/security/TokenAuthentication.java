package io.github.vanish073.driftline.app.security;

import io.github.vanish073.driftline.core.id.MerchantId;
import java.io.Serial;
import java.util.List;
import java.util.Optional;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

/**
 * An authenticated API token (§14): its scopes as {@code SCOPE_*} authorities and the merchant it is bound to. An empty
 * merchant is an ops token. The token itself is never kept; only its hash was ever stored.
 */
public final class TokenAuthentication extends AbstractAuthenticationToken {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient Optional<MerchantId> merchantId;
    private final String tokenHashPrefix;

    public TokenAuthentication(Optional<MerchantId> merchantId, List<String> scopes, String tokenHashPrefix) {
        super(scopes.stream().map(s -> new SimpleGrantedAuthority("SCOPE_" + s)).toList());
        this.merchantId = merchantId;
        this.tokenHashPrefix = tokenHashPrefix;
        setAuthenticated(true);
    }

    /** The merchant the token is bound to; empty for an ops token. */
    public Optional<MerchantId> merchantId() {
        return merchantId;
    }

    @Override
    public Object getCredentials() {
        return "";
    }

    /** A short prefix of the token's hash: enough to tell tokens apart in logs, useless to an attacker. */
    @Override
    public Object getPrincipal() {
        return merchantId.map(m -> "merchant:" + m.value()).orElse("ops") + "/" + tokenHashPrefix;
    }
}
