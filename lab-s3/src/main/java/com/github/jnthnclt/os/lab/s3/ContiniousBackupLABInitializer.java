package com.github.jnthnclt.os.lab.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.UIO;
import com.github.jnthnclt.os.lab.core.LABEnvironmentBuilder;
import com.github.jnthnclt.os.lab.core.LABHeapPressureBuilder;
import com.github.jnthnclt.os.lab.core.LABIndexProvider;
import com.github.jnthnclt.os.lab.core.LABStats;
import com.github.jnthnclt.os.lab.core.LABValueIndexConfigBuilder;
import com.github.jnthnclt.os.lab.api.ValueIndex;
import com.github.jnthnclt.os.lab.core.api.ValueIndexConfig;
import com.github.jnthnclt.os.lab.core.guts.LABFiles;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ContiniousBackupLABInitializer {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {


        String awsApiKey = "";
        String awsApiSecret = "";
        File root = new File("");
        File restoreRoot = new File("");
        String bucketName = "";
        String keyPrefix = "";

        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsApiKey, awsApiSecret);
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .withRegion(Regions.US_EAST_1)
                .build();


        BackUpper backUpper = new S3BackUpper(s3client, bucketName, keyPrefix);


        long total = 1_000_000;

        FileUtils.cleanDirectory(root);

        LABContinuousS3Backup.BackupPoint<Long> backupPoint = (indexName, batchId) -> {
            System.out.println("Hooray index:" + indexName + " is backed up to:" + batchId);
        };

        LABContinuousS3Backup labContinuousS3Backup = ContiniousBackupLABInitializer.initialize(backUpper,
                backupPoint);

        LABValueIndexConfigBuilder labValueIndexConfigBuilder = new LABValueIndexConfigBuilder("foo");
        labValueIndexConfigBuilder.setSplitWhenValuesAndKeysTotalExceedsNBytes(10 * 1024 * 1024);
        labContinuousS3Backup.open(root, labValueIndexConfigBuilder.build());

        labContinuousS3Backup.start();

        ExecutorService batchFlushers = Executors.newFixedThreadPool(1);

        List<Future<Void>> futures = Lists.newArrayList();
        int batchSize = 10_000;
        for (long i = 0; i < total; i += batchSize) {
            long id = i;


            futures.add(batchFlushers.submit(() -> {
                labContinuousS3Backup.acquireIndex("foo", id, (index) -> {

                    long[] fromToAppend = new long[2];
                    boolean first = true;
                    for (int j = 0; j < batchSize; j++) {
                        long key = id + j;
                        fromToAppend[first ? 0 : 1] = index.append(stream -> {

                                    stream.stream(1, UIO.longBytes(key), System.currentTimeMillis(), false,
                                            System.currentTimeMillis(),
                                            (key + " fluffy bunnies ate my carrots").getBytes());

                                    return true;
                                }, true,
                                new BolBuffer(), new BolBuffer());
                        first = false;
                    }

                    LOG.info("Commit " + (id + batchSize) + "->" + total);
                    index.commit(true, false);
                    Thread.sleep(2000);
                    return fromToAppend;
                });
                return null;
            }));


        }
        System.out.println("Waiting on batches...");
        for (Future<Void> future : futures) {
            future.get();
        }
        System.out.println("Indexing done");

        labContinuousS3Backup.acquireIndex("foo", null, (index) -> {
            index.close(true, true);
            return null;
        });
        System.out.println("Flushing done");


        labContinuousS3Backup.stop();


        backUpper.restore(restoreRoot, "foo");
        System.out.println("Restore index done");


        AtomicLong keyCount = new AtomicLong();
        LABContinuousS3Backup foo2 = ContiniousBackupLABInitializer.initialize(backUpper, backupPoint);
        labValueIndexConfigBuilder = new LABValueIndexConfigBuilder("foo");
        labValueIndexConfigBuilder.setSplitWhenValuesAndKeysTotalExceedsNBytes(10 * 1024 * 1024);

        foo2.open(restoreRoot, labValueIndexConfigBuilder.build());
        foo2.acquireIndex("foo", null, index -> {
            System.out.println(index.count());

            index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
                long k = key.getLong(0);
                keyCount.incrementAndGet();
                if (k % 10_000 == 0) {
                    System.out.println(index1 + " " + k + " " + new String(payload.copy()));
                }
                return true;
            }, true);

            return null;
        });

        System.out.println(keyCount.get());

        System.exit(0);
    }

    public static <I> LABContinuousS3Backup<I> initialize(BackUpper backUpper,
                                                          LABContinuousS3Backup.BackupPoint<I> backupPoint) {

        LABFiles labFiles = new LABFiles();
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats(globalHeapCostInBytes);
        LABHeapPressureBuilder labHeapPressureBuilder = new LABHeapPressureBuilder(globalHeapCostInBytes);
        labHeapPressureBuilder.setMaxHeapPressureInBytes(1024 * 1024 * 32); //??
        LABEnvironmentBuilder labEnvironmentBuilder = new LABEnvironmentBuilder().setLABFiles(labFiles);

        LABIndexProvider<byte[]> indexProvider = new LABIndexProvider<>(stats,
                labHeapPressureBuilder,
                labEnvironmentBuilder);


        return new LABContinuousS3Backup(backUpper, labFiles, indexProvider, backupPoint);
    }

    public interface AcquireIndex<R> {
        long[] index(ValueIndex<byte[]> index) throws Exception;
    }

    public static class LABContinuousS3Backup<I> {

        private static final LABLogger LOG = LABLoggerFactory.getLogger();


        private final BackUpper backUpper;
        private final LABFiles labFiles;
        private final LABIndexProvider<byte[]> indexProvider;
        private final BackupPoint<I> backupPoint;
        private final ExecutorService backupThread = Executors.newSingleThreadExecutor();
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicBoolean stopped = new AtomicBoolean(false);

        private final Map<String, RootAndIndex> indexes = Maps.newConcurrentMap();
        private final Map<String, ConcurrentLinkedQueue<BatchId<I>>> wroteRanges = Maps.newConcurrentMap();
        private final Map<String, TreeRangeSet<Long>> flushedRanges = Maps.newConcurrentMap();

        public LABContinuousS3Backup(BackUpper backUpper,
                                     LABFiles labFiles,
                                     LABIndexProvider<byte[]> indexProvider,
                                     BackupPoint<I> backupPoint) {
            this.backUpper = backUpper;
            this.labFiles = labFiles;
            this.indexProvider = indexProvider;
            this.backupPoint = backupPoint;
        }

        public void open(File indexRoot, ValueIndexConfig indexConfig) throws Exception {
            ValueIndex<byte[]> valueIndex = indexProvider.buildIndex(indexRoot, indexConfig);
            indexes.putIfAbsent(indexConfig.primaryName, new RootAndIndex(indexRoot, valueIndex));
        }

        public interface BackupPoint<I> {
            void backedUp(String indexName, I batchId);
        }


        public void acquireIndex(String indexName, I batchId, AcquireIndex acquireIndex) throws Exception {
            RootAndIndex rootAndIndex = indexes.get(indexName);
            if (batchId != null) {
                long[] fromToVersion = acquireIndex.index(rootAndIndex.index);
                if (fromToVersion != null && fromToVersion[0] < fromToVersion[1]) {
                    ConcurrentLinkedQueue<BatchId<I>> wroteQueue = wroteRanges.computeIfAbsent(indexName,
                            s -> new ConcurrentLinkedQueue<>());
                    Range<Long> range = Range.closed(fromToVersion[0], fromToVersion[1]);
                    LOG.info("Added " + range + " for " + batchId);
                    wroteQueue.add(new BatchId<>(range, indexName, batchId));
                }
            } else {
                acquireIndex.index(rootAndIndex.index);
            }
        }

        private static class RootAndIndex<I> {
            private final File root;
            private final ValueIndex<byte[]> index;

            private RootAndIndex(File root, ValueIndex<byte[]> index) {
                this.root = root;
                this.index = index;
            }

            private String buildKey(File child) {
                return child.getAbsolutePath().substring(root.getAbsolutePath().length() + 1);
            }
        }


        private static class BatchId<I> {
            private final Range<Long> range;
            private final I batchId;
            public String indexName;
            private AtomicBoolean completed = new AtomicBoolean(false);

            private BatchId(Range<Long> range, String indexName, I batchId) {
                this.range = range;
                this.indexName = indexName;
                this.batchId = batchId;
            }

            public void setCompleted() {
                completed.compareAndSet(false, true);
            }

            public boolean isComplete() {
                return completed.get();
            }

        }


        public void start() throws Exception {
            if (running.compareAndSet(false, true)) {
                backupThread.submit(() -> {

                    try {
                        labFiles.take(change -> {

                            if (change == null) {
                                for (RootAndIndex rootAndIndex : indexes.values()) {
                                    if (!rootAndIndex.index.closed()) {
                                        return true;
                                    }
                                }
                                return running.get();
                            }

                            String id = new String(change.labId, StandardCharsets.UTF_8);
                            RootAndIndex rootAndIndex = indexes.get(id);
                            String key = rootAndIndex.buildKey(change.file);
                            if (change.delete) {
                                while (true) {
                                    try {
                                        backUpper.delete(key, change.file);
                                        return true;
                                    } catch (Exception x) {
                                        LOG.error("Failed to remove " + key + " from backup. Will retry in 5 sec",
                                                x);
                                        Thread.sleep(5000);
                                    }
                                }
                            } else {
                                while (true) {
                                    try {
                                        boolean backedUp = false;
                                        if (change.file.exists()) {
                                            try {
                                                LOG.info(
                                                        "Backing up index:" + id
                                                                + " key:" + key + " "
                                                                + UIO.ram(FileUtils.sizeOf(change.file)) + "...");
                                            } catch (IllegalArgumentException ia) {
                                            }
                                            backUpper.backup(key, change.file);
                                            backedUp = true;
                                            LOG.info("Backed up index:" + id
                                                    + " version:[" + change.fromAppendVersion + ".." + change.toAppendVersion + "] key:" + key);

                                        }
                                        if (backedUp && change.fromAppendVersion != -1 && change.toAppendVersion != -1) {

                                            ConcurrentLinkedQueue<BatchId<I>> wrote = wroteRanges.get(id);
                                            if (wrote != null) {

                                                TreeRangeSet<Long> rangeSet = flushedRanges.computeIfAbsent(id,
                                                        s -> TreeRangeSet.create());
                                                rangeSet.add(
                                                        Range.closed(change.fromAppendVersion,
                                                                change.toAppendVersion + 1));


                                                Iterator<BatchId<I>> iterator = wrote.iterator();
                                                while (iterator.hasNext()) {
                                                    BatchId<I> next = iterator.next();
                                                    if (rangeSet.encloses(next.range)) {
                                                        next.setCompleted();
                                                    }
                                                }

                                                List<BatchId<I>> backedUpPoints = Lists.newArrayList();
                                                iterator = wrote.iterator();
                                                while (iterator.hasNext()) {
                                                    BatchId<I> next = iterator.next();
                                                    if (next.isComplete()) {
                                                        backedUpPoints.add(next);
                                                        iterator.remove();
                                                    } else {
                                                        break;
                                                    }
                                                }

                                                LOG.info("Flushed RangeSet:" + rangeSet);
                                                for (BatchId<I> i : backedUpPoints) {
                                                    try {
                                                        this.backupPoint.backedUp(i.indexName, i.batchId);
                                                    } catch (Exception x) {
                                                        LOG.error("Failure while call back point callback.", x);
                                                    }
                                                }
                                            }
                                        }
                                        return true;
                                    } catch (Exception x) {
                                        LOG.error("Failed to backup index:" + id
                                                + " key:" + key
                                                + ". Will retry in 5 sec", x);
                                        Thread.sleep(5000);
                                    }
                                }
                            }
                        });
                    } catch (Exception x) {
                        LOG.error("Unexpected shutdown :(", x);
                    }

                    synchronized (stopped) {
                        if (stopped.compareAndSet(false, true)) {
                            LOG.info("Stopped");
                            stopped.notifyAll();
                        }
                    }
                    return true;
                });
            }
        }


        public void stop() throws Exception {
            if (running.compareAndSet(true, false)) {
                synchronized (stopped) {
                    if (stopped.get() == false) {
                        LOG.info("Waiting for backup service to stop...");
                        stopped.wait();
                    }
                }
                backupThread.shutdown();
                LOG.info("Backup service has stopped");
            }
        }
    }
}
