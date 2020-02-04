package org.superbiz.moviefun.blobstore;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class S3Store implements BlobStore {

    private AmazonS3Client s3Client;
    private String bucket;

    public S3Store(AmazonS3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public void put(Blob blob) throws IOException {
        s3Client.putObject(bucket, blob.name, blob.inputStream, null);
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {
        if (!s3Client.doesObjectExist(bucket, name)) {
            return Optional.empty();
        }

        try (S3Object result = s3Client.getObject(bucket, name)) {
            S3ObjectInputStream content = result.getObjectContent();
            byte[] bytes = IOUtils.toByteArray(content);
            Blob blob = new Blob(result.getKey(),
                    new ByteArrayInputStream(bytes),
                    result.getObjectMetadata().getContentType());
            return Optional.of(blob);
        }
    }

    @Override
    public void deleteAll() {
        List<S3ObjectSummary> summaries = s3Client.listObjects(bucket).getObjectSummaries();
        for (S3ObjectSummary summary : summaries) {
            s3Client.deleteObject(bucket, summary.getKey());
        }
    }
}
