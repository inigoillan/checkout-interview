package com.checkout.count.support;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public final class S3Support {

  private S3Support() {}

  public static S3Client client() {
    return S3Client.builder()
        .endpointOverride(URI.create(Containers.s3Endpoint()))
        .region(Region.of(TestConfigs.REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(TestConfigs.ACCESS_KEY, TestConfigs.SECRET_KEY)))
        .forcePathStyle(true)
        .build();
  }

  public static void createBucket(S3Client s3, String bucket) {
    try {
      s3.createBucket(request -> request.bucket(bucket));
    } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
      // Already created by an earlier test in this JVM.
    }
  }

  public static List<String> keys(S3Client s3, String bucket, String prefix) {
    return s3.listObjectsV2(request -> request.bucket(bucket).prefix(prefix)).contents().stream()
        .map(S3Object::key)
        .toList();
  }

  public static List<GenericRecord> readParquet(S3Client s3, String bucket, List<String> keys, Path dir)
      throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    for (String key : keys) {
      Path local = dir.resolve(UUID.randomUUID() + ".parquet");
      s3.getObject(
          GetObjectRequest.builder().bucket(bucket).key(key).build(),
          ResponseTransformer.toFile(local));
      records.addAll(readLocal(local));
    }
    return records;
  }

  public static long timeWindowMillis(GenericRecord record) {
    Object value = record.get("time_window");
    return value instanceof Instant instant ? instant.toEpochMilli() : (Long) value;
  }

  private static List<GenericRecord> readLocal(Path file) throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(hadoopPath, new Configuration()))
            .build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }
    return records;
  }
}
