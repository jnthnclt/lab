package com.github.jnthnclt.os.lab.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class S3BackUpper implements BackUpper {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    private final AmazonS3 s3client;
    private final String bucketName;
    private final String keyPrefix;

    public S3BackUpper(AmazonS3 s3client, String bucketName, String keyPrefix) {
        this.s3client = s3client;
        this.bucketName = bucketName;
        this.keyPrefix = keyPrefix;
    }


    @Override
    public void backup(String key, File file) {
        s3client.putObject(
                bucketName,
                keyPrefix + "/" + key,
                file
        );
    }

    @Override
    public void delete(String key, File file) {
        s3client.deleteObject(bucketName, keyPrefix + "/" + key);
    }

    @Override
    public void restore(File indexRoot, String indexName) throws IOException {

        FileUtils.forceMkdir(indexRoot);
        FileUtils.cleanDirectory(indexRoot);

        String prefix = keyPrefix + "/" + indexName;
        LOG.info("Listing restore prefix " + prefix);
        ObjectListing objectListing = s3client.listObjects(bucketName, prefix);
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            String key = os.getKey();
            S3Object s3object = s3client.getObject(bucketName, key);
            S3ObjectInputStream inputStream = s3object.getObjectContent();
            File destination = new File(indexRoot, key.substring(keyPrefix.length()));
            FileUtils.copyInputStreamToFile(inputStream, destination);
            LOG.info("Restored from backup " + key + " to " + destination);
        }
    }
}
