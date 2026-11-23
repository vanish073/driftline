package io.github.vanish073.driftline.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** The profiles of DESIGN §11.15; a {@code nightly} seed itself costs about 40 s, so CI's nightly workflow runs them. */
class ProfileTest {

    @Test
    void theNightlyProfileIsTheDesignsLongCampaign() {
        Profile p = Profile.fromId("nightly");
        assertThat(p).isEqualTo(Profile.NIGHTLY);
        assertThat(p.minMerchants).isEqualTo(5).isEqualTo(p.maxMerchants);
        assertThat(p.minPartitions).isEqualTo(6).isEqualTo(p.maxPartitions);
        assertThat(p.minOrders).isEqualTo(1_800);
        assertThat(p.maxOrders).isEqualTo(2_200);
        assertThat(p.trafficDays).isEqualTo(5);
        assertThat(p.faults).isTrue();
    }

    @Test
    void everyProfileRoundTripsByItsIdAndAnUnknownOneIsRefused() {
        for (Profile p : Profile.values()) {
            assertThat(Profile.fromId(p.id())).isEqualTo(p);
        }
        assertThatThrownBy(() -> Profile.fromId("huge")).hasMessageContaining("calm, pr, nightly");
    }
}
