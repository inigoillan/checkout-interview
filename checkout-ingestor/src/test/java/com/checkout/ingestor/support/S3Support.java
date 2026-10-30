package com.checkout.ingestor.support;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class S3Support {

  private S3Support() {}

  public static S3Client client() {
    return S3Client.builder()
        .endpointOverride(URI.create(Containers.LOCALSTACK.getEndpoint().toString()))
        .region(Region.of(Containers.LOCALSTACK.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
            Containers.LOCALSTACK.getAccessKey(), Containers.LOCALSTACK.getSecretKey())))
        .forcePathStyle(true)
        .build();
  }

  public static void createBucket(S3Client s3, String bucket) {
    s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
  }

  public static List<String> parquetKeys(S3Client s3, String bucket, String prefix) {
    List<String> keys = new ArrayList<>();
    for (S3Object object : s3.listObjectsV2Paginator(
        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()).contents()) {
      if (object.key().endsWith(".parquet")) {
        keys.add(object.key());
      }
    }
    return keys;
  }

  public static List<GenericRecord> readRecords(S3Client s3, String bucket, List<String> keys) throws Exception {
    List<GenericRecord> records = new ArrayList<>();
    for (String key : keys) {
      records.addAll(readParquet(s3, bucket, key));
    }
    return records;
  }

  private static List<GenericRecord> readParquet(S3Client s3, String bucket, String key) throws Exception {
    Path dir = Files.createTempDirectory("ingestor-test-");
    Path local = dir.resolve("object.parquet");
    try {
      s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(), ResponseTransformer.toFile(local));
      List<GenericRecord> records = new ArrayList<>();
      try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
          new org.apache.hadoop.fs.Path(local.toUri())).build()) {
        GenericRecord record;
        while ((record = reader.read()) != null) {
          records.add(record);
        }
      }
      return records;
    } finally {
      Files.deleteIfExists(local);
      Files.deleteIfExists(dir);
    }
  }
}
