package net.rsworld.superduper.starter;

import net.rsworld.superduper.observability.api.ObservabilitySettings;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "superduper.observability")
public class ObservabilityProperties {
    private boolean enabled = true;
    private final Components components = new Components();
    private final Signals signals = new Signals();
    private final Outputs outputs = new Outputs();
    private final Metrics metrics = new Metrics();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Components getComponents() {
        return components;
    }

    public Signals getSignals() {
        return signals;
    }

    public Outputs getOutputs() {
        return outputs;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public ObservabilitySettings toSettings() {
        return new ObservabilitySettings(
                enabled,
                components.consumer,
                components.worker,
                components.maintenance,
                signals.lifecycle,
                signals.success,
                signals.failure,
                signals.retry,
                signals.timing,
                outputs.log.enabled,
                outputs.metrics.enabled,
                metrics.tags.topic,
                metrics.tags.exceptionTag);
    }

    public static class Components {
        private boolean consumer = true;
        private boolean worker = true;
        private boolean maintenance = true;

        public boolean isConsumer() {
            return consumer;
        }

        public void setConsumer(boolean consumer) {
            this.consumer = consumer;
        }

        public boolean isWorker() {
            return worker;
        }

        public void setWorker(boolean worker) {
            this.worker = worker;
        }

        public boolean isMaintenance() {
            return maintenance;
        }

        public void setMaintenance(boolean maintenance) {
            this.maintenance = maintenance;
        }
    }

    public static class Signals {
        private boolean lifecycle = true;
        private boolean success = true;
        private boolean failure = true;
        private boolean retry = true;
        private boolean timing = true;

        public boolean isLifecycle() {
            return lifecycle;
        }

        public void setLifecycle(boolean lifecycle) {
            this.lifecycle = lifecycle;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public boolean isFailure() {
            return failure;
        }

        public void setFailure(boolean failure) {
            this.failure = failure;
        }

        public boolean isRetry() {
            return retry;
        }

        public void setRetry(boolean retry) {
            this.retry = retry;
        }

        public boolean isTiming() {
            return timing;
        }

        public void setTiming(boolean timing) {
            this.timing = timing;
        }
    }

    public static class Outputs {
        private final Log log = new Log();
        private final MetricsOut metrics = new MetricsOut();

        public Log getLog() {
            return log;
        }

        public MetricsOut getMetrics() {
            return metrics;
        }
    }

    public static class Log {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class MetricsOut {
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Metrics {
        private final Tags tags = new Tags();

        public Tags getTags() {
            return tags;
        }
    }

    public static class Tags {
        private boolean topic = true;
        private boolean exceptionTag = true;

        public boolean isTopic() {
            return topic;
        }

        public void setTopic(boolean topic) {
            this.topic = topic;
        }

        public boolean isExceptionTag() {
            return exceptionTag;
        }

        public void setExceptionTag(boolean exceptionTag) {
            this.exceptionTag = exceptionTag;
        }
    }
}
