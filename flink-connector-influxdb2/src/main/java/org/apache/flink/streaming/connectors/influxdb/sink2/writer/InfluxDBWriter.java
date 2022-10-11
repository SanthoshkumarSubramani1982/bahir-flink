package org.apache.flink.streaming.connectors.influxdb.sink2.writer;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSinkOptions.*;

public class InfluxDBWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBWriter.class);

    private final int bufferSize;
    private final boolean writeCheckpoint;
    private long lastTimestamp = 0;
    private final List<Point> elements;
    private ProcessingTimeService processingTimerService;
    private final InfluxDBSchemaSerializer<IN> schemaSerializer;
    private final InfluxDBClient influxDBClient;

    public InfluxDBWriter(
            final InfluxDBSchemaSerializer<IN> schemaSerializer,
            final Configuration configuration) {
        this.schemaSerializer = schemaSerializer;
        this.bufferSize = configuration.getInteger(WRITE_BUFFER_SIZE);
        this.elements = new ArrayList<>(this.bufferSize);
        this.writeCheckpoint = configuration.getBoolean(WRITE_DATA_POINT_CHECKPOINT);
        this.influxDBClient = getInfluxDBClient(configuration);
    }

    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        this.processingTimerService = processingTimerService;
    }

    /**
     * This method calls the InfluxDB write API whenever the element list reaches the {@link
     * #bufferSize}. It keeps track of the latest timestamp of each element. It compares the latest
     * timestamp with the context.timestamp() and takes the bigger (latest) timestamp.
     *
     * @param in incoming data
     * @param context current Flink context
     * @see org.apache.flink.api.connector.sink2.SinkWriter.Context
     */
    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        if (this.elements.size() == this.bufferSize) {
            LOG.debug("Buffer size reached preparing to write the elements.");
            this.writeCurrentElements();
            this.elements.clear();
        }

        LOG.trace("Adding elements to buffer. Buffer size: {}", this.elements.size());
        this.elements.add(this.schemaSerializer.serialize(in, context));
        if (context.timestamp() != null) {
            this.lastTimestamp = Math.max(this.lastTimestamp, context.timestamp());
        }

    }

    @Override
    public void flush(boolean flush) throws IOException, InterruptedException {
        if (flush) {
            if (this.lastTimestamp == 0) {
                commit(Collections.emptyList());
            }
            final List<Long> lastTimestamp = new ArrayList<>(1);
            lastTimestamp.add(this.lastTimestamp);
            commit(lastTimestamp);
        }
    }

    public List<Long> commit(final List<Long> committables) {
        if (this.writeCheckpoint) {
            LOG.debug("A checkpoint is set.");
            Optional<Long> lastTimestamp = Optional.empty();
            if (committables.size() >= 1) {
                lastTimestamp = Optional.ofNullable(committables.get(committables.size() - 1));
            }
            this.writeCheckpointDataPoint(lastTimestamp);
        }
        return Collections.emptyList();
    }

    private void writeCheckpointDataPoint(final Optional<Long> timestamp) {
        try (final WriteApi writeApi = this.influxDBClient.getWriteApi()) {
            final Point point = new Point("checkpoint");
            point.addField("checkpoint", "flink");
            timestamp.ifPresent(aTime -> point.time(aTime, WritePrecision.NS));
            writeApi.writePoint(point);
            LOG.debug("Checkpoint data point write at {}", point.toLineProtocol());
        }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        LOG.debug(watermark.getTimestamp() + "-->" + lastTimestamp);
        SinkWriter.super.writeWatermark(watermark);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Preparing to write the elements in InfluxDB.");
        this.writeCurrentElements();
        LOG.debug("Closing the writer.");
        this.elements.clear();
    }

    private boolean writeCurrentElements() {
        if (this.elements.isEmpty()) {
            return false;
        }
        try (final WriteApi writeApi = this.influxDBClient.getWriteApi()) {
            writeApi.writePoints(this.elements);
            LOG.debug("Wrote {} data points", this.elements.size());
            this.elements.forEach(e -> LOG.error(e.toLineProtocol()));
            this.elements.clear();
        }
        return true;
    }
}
