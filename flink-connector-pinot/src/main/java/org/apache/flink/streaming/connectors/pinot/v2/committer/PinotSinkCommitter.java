/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot.v2.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.streaming.connectors.pinot.PinotControllerClient;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommittable;
import org.apache.flink.streaming.connectors.pinot.committer.PinotSinkGlobalCommitter;
import org.apache.flink.streaming.connectors.pinot.filesystem.FileSystemAdapter;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Global committer takes committables from {@link org.apache.flink.streaming.connectors.pinot.writer.PinotSinkWriter},
 * generates segments and pushed them to the Pinot controller.
 * Note: We use a custom multithreading approach to parallelize the segment creation and upload to
 * overcome the performance limitations resulting from using a {@link GlobalCommitter} always
 * running at a parallelism of 1.
 */
@Internal
public class PinotSinkCommitter implements Committer<PinotSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PinotSinkCommitter.class);

    private final String pinotControllerHost;
    private final String pinotControllerPort;
    private final String tableName;
    private final SegmentNameGenerator segmentNameGenerator;
    private final FileSystemAdapter fsAdapter;
    private final String timeColumnName;
    private final TimeUnit segmentTimeUnit;
    private final PinotControllerClient pinotControllerClient;
    private final File tempDirectory;
    private final Schema tableSchema;
    private final TableConfig tableConfig;
    private final ExecutorService pool;

    /**
     * @param pinotControllerHost  Host of the Pinot controller
     * @param pinotControllerPort  Port of the Pinot controller
     * @param tableName            Target table's name
     * @param segmentNameGenerator Pinot segment name generator
     * @param tempDirPrefix        Prefix for directory to store temporary files in
     * @param fsAdapter            Adapter for interacting with the shared file system
     * @param timeColumnName       Name of the column containing the timestamp
     * @param segmentTimeUnit      Unit of the time column
     * @param numCommitThreads     Number of threads used to commit the committables
     */
    public PinotSinkCommitter(String pinotControllerHost, String pinotControllerPort,
                              String tableName, SegmentNameGenerator segmentNameGenerator,
                              String tempDirPrefix, FileSystemAdapter fsAdapter,
                              String timeColumnName, TimeUnit segmentTimeUnit,
                              int numCommitThreads) throws IOException {
        this.pinotControllerHost = checkNotNull(pinotControllerHost);
        this.pinotControllerPort = checkNotNull(pinotControllerPort);
        this.tableName = checkNotNull(tableName);
        this.segmentNameGenerator = checkNotNull(segmentNameGenerator);
        this.fsAdapter = checkNotNull(fsAdapter);
        this.timeColumnName = checkNotNull(timeColumnName);
        this.segmentTimeUnit = checkNotNull(segmentTimeUnit);
        this.pinotControllerClient = new PinotControllerClient(pinotControllerHost, pinotControllerPort);

        // Create directory that temporary files will be stored in
//        this.tempDirectory = Files.createTempDirectory(tempDirPrefix).toFile();
        this.tempDirectory = new File(tempDirPrefix);

        // Retrieve the Pinot table schema and the Pinot table config from the Pinot controller
        this.tableSchema = pinotControllerClient.getSchema(tableName);
        this.tableConfig = pinotControllerClient.getTableConfig(tableName);

        // We use a thread pool in order to parallelize the segment creation and segment upload
        checkArgument(numCommitThreads > 0);
        this.pool = Executors.newFixedThreadPool(numCommitThreads);
    }


    @Override
    public void commit(Collection<CommitRequest<PinotSinkCommittable>> collection) throws IOException, InterruptedException {
        Collection<PinotSinkCommittable> committables = collection.stream()
                .map(CommitRequest::getCommittable)
                .collect(Collectors.toList());

        this.commitSink(committables);

    }

    public void commitSink(Collection<PinotSinkCommittable> collection) throws IOException, InterruptedException {
        if (collection.isEmpty()) return;

        // List of failed global committables that can be retried later on
        List<PinotSinkGlobalCommittable> failedCommits = new ArrayList<>();

        PinotSinkGlobalCommittable globalCommittable = this.combine(new ArrayList<>(collection));

        Set<Future<Boolean>> resultFutures = new HashSet<>();
        // Commit all segments in globalCommittable
        for (int sequenceId = 0; sequenceId < globalCommittable.getDataFilePaths().size(); sequenceId++) {
            String dataFilePath = globalCommittable.getDataFilePaths().get(sequenceId);
            // Get segment names with increasing sequenceIds
            String segmentName = getSegmentName(globalCommittable, sequenceId);
            // Segment committer handling the whole commit process for a single segment
            Callable<Boolean> segmentCommitter = new PinotSinkGlobalCommitter.SegmentCommitter(
                    pinotControllerHost, pinotControllerPort, tempDirectory, fsAdapter,
                    dataFilePath, segmentName, tableSchema, tableConfig, timeColumnName,
                    segmentTimeUnit
            );
            // Submits the segment committer to the thread pool
            resultFutures.add(pool.submit(segmentCommitter));
        }

        boolean commitSucceeded = true;
        try {
            for (Future<Boolean> wasSuccessful : resultFutures) {
                // In case any of the segment commits wasn't successful we mark the whole
                // globalCommittable as failed
                if (!wasSuccessful.get()) {
                    commitSucceeded = false;
                    failedCommits.add(globalCommittable);
                    // Once any of the commits failed, we do not need to check the remaining
                    // ones, as we try to commit the globalCommittable next time
                    break;
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            // In case of an exception thrown while accessing commit status, mark the whole
            // globalCommittable as failed
            failedCommits.add(globalCommittable);
            LOG.error("Accessing a SegmentCommitter thread errored with {}", e.getMessage(), e);
        }

        if (commitSucceeded) {
            // If commit succeeded, cleanup the data files stored on the shared file system. In
            // case the commit of at least one of the segments failed, nothing will be cleaned
            // up here to enable retrying failed commits (data files must therefore stay
            // available on the shared filesystem).
            for (String path : globalCommittable.getDataFilePaths()) {
                fsAdapter.deleteFromSharedFileSystem(path);
            }
        }

        if (failedCommits.size() > 0) {
            LOG.error(failedCommits.toString());
        }

    }

    /**
     * Helper method for generating segment names using the segment name generator.
     *
     * @param globalCommittable Global committable the segment name shall be generated from
     * @param sequenceId        Incrementing counter
     * @return generated segment name
     */
    private String getSegmentName(PinotSinkGlobalCommittable globalCommittable, int sequenceId) {
        return segmentNameGenerator.generateSegmentName(sequenceId,
                globalCommittable.getMinTimestamp(), globalCommittable.getMaxTimestamp());
    }

    /**
     * Combines multiple {@link PinotSinkCommittable}s into one {@link PinotSinkGlobalCommittable}
     * by finding the minimum and maximum timestamps from the provided {@link PinotSinkCommittable}s.
     *
     * @param committables Committables created by {@link org.apache.flink.streaming.connectors.pinot.v2.writer.PinotWriter}
     * @return Global committer committable
     */
    public PinotSinkGlobalCommittable combine(List<PinotSinkCommittable> committables) {
        List<String> dataFilePaths = new ArrayList<>();
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;

        // Extract all data file paths and the overall minimum and maximum timestamps
        // from all committables
        for (PinotSinkCommittable committable : committables) {
            dataFilePaths.add(committable.getDataFilePath());
            minTimestamp = Long.min(minTimestamp, committable.getMinTimestamp());
            maxTimestamp = Long.max(maxTimestamp, committable.getMaxTimestamp());
        }

        LOG.debug("Combined {} committables into one global committable", committables.size());
        return new PinotSinkGlobalCommittable(dataFilePaths, minTimestamp, maxTimestamp);
    }

    @Override
    public void close() throws Exception {

    }
}
