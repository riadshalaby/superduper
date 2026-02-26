package net.rsworld.superduper.starter;

import static org.assertj.core.api.Assertions.assertThat;

import net.rsworld.superduper.observability.api.ObservabilityComponent;
import net.rsworld.superduper.observability.api.ObservabilitySignal;
import org.junit.jupiter.api.Test;

class ObservabilityPropertiesTest {

    @Test
    void toSettings_usesDefaultMatrix() {
        ObservabilityProperties properties = new ObservabilityProperties();
        var settings = properties.toSettings();

        assertThat(settings.enabled()).isTrue();
        assertThat(settings.logEnabled()).isTrue();
        assertThat(settings.metricsEnabled()).isFalse();
        assertThat(settings.metricsTopicTagEnabled()).isTrue();
        assertThat(settings.metricsExceptionTagEnabled()).isTrue();
        assertThat(settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.LIFECYCLE))
                .isTrue();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS)).isTrue();
        assertThat(settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE))
                .isTrue();
    }

    @Test
    void toSettings_mapsCustomMatrixFlags() {
        ObservabilityProperties properties = new ObservabilityProperties();
        properties.getComponents().setConsumer(false);
        properties.getComponents().setWorker(true);
        properties.getComponents().setMaintenance(false);
        properties.getSignals().setLifecycle(true);
        properties.getSignals().setSuccess(false);
        properties.getSignals().setFailure(true);
        properties.getSignals().setRetry(false);
        properties.getSignals().setTiming(false);
        properties.getOutputs().getLog().setEnabled(false);
        properties.getOutputs().getMetrics().setEnabled(true);
        properties.getMetrics().getTags().setTopic(false);
        properties.getMetrics().getTags().setExceptionTag(false);

        var settings = properties.toSettings();

        assertThat(settings.enabled()).isTrue();
        assertThat(settings.logEnabled()).isFalse();
        assertThat(settings.metricsEnabled()).isTrue();
        assertThat(settings.metricsTopicTagEnabled()).isFalse();
        assertThat(settings.metricsExceptionTagEnabled()).isFalse();
        assertThat(settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.LIFECYCLE))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.LIFECYCLE))
                .isTrue();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.FAILURE))
                .isTrue();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.RETRY))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.TIMING))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE))
                .isFalse();
    }

    @Test
    void toSettings_disablesAllSignals_whenGlobalSwitchIsOff() {
        ObservabilityProperties properties = new ObservabilityProperties();
        properties.setEnabled(false);

        var settings = properties.toSettings();

        assertThat(settings.enabled()).isFalse();
        assertThat(settings.allows(ObservabilityComponent.CONSUMER, ObservabilitySignal.LIFECYCLE))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.WORKER, ObservabilitySignal.SUCCESS))
                .isFalse();
        assertThat(settings.allows(ObservabilityComponent.MAINTENANCE, ObservabilitySignal.FAILURE))
                .isFalse();
    }
}
