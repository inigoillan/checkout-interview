package com.checkout.count.sink;

import com.checkout.count.config.CountConfig.Location;
import org.apache.flink.core.fs.Path;

final class S3Paths {

  private S3Paths() {}

  static Path of(Location location) {
    return new Path("s3://" + location.bucket() + "/" + location.prefix());
  }
}
