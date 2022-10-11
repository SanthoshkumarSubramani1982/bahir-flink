package org.apache.flink.streaming.connectors.influxdb.sink2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.influxdb.sink2.writer.InfluxDBSchemaSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink2.writer.InfluxDBWriter;

import java.io.IOException;

public class InfluxDBSink<IN> implements Sink<IN> {

    private final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private final Configuration configuration;

    InfluxDBSink(
            final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer,
            final Configuration configuration) {
        this.influxDBSchemaSerializer = influxDBSchemaSerializer;
        this.configuration = configuration;
    }

    /**
     * Get a influxDBSinkBuilder to build a {@link InfluxDBSink}.
     *
     * @return a InfluxDB sink builder.
     */
    public static <IN> InfluxDBSinkBuilder<IN> builder() {
        return new InfluxDBSinkBuilder<>();
    }


    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) throws IOException {
        final InfluxDBWriter<IN> writer =
                new InfluxDBWriter<>(this.influxDBSchemaSerializer, this.configuration);
        writer.setProcessingTimerService(initContext.getProcessingTimeService());
        return writer;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

}
