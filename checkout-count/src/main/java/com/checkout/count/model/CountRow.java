package com.checkout.count.model;

import java.util.Objects;

/** One output row: the number of view events for a postcode in a one-minute window. */
public class CountRow {

  public long timeWindow;
  public String postcode;
  public int count;

  public CountRow() {}

  public CountRow(long timeWindow, String postcode, int count) {
    this.timeWindow = timeWindow;
    this.postcode = postcode;
    this.count = count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CountRow other)) {
      return false;
    }
    return timeWindow == other.timeWindow
        && count == other.count
        && Objects.equals(postcode, other.postcode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeWindow, postcode, count);
  }

  @Override
  public String toString() {
    return "CountRow[timeWindow=" + timeWindow + ", postcode=" + postcode + ", count=" + count + "]";
  }
}
