/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.range.RangeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeTracker} for non-negative positions within a {@link Stream}.
 *
 * <p>The BigQuery storage read API provides server-side liquid sharding rather than static
 * partitioning, so the length of a read stream isn't known in some cases until the end of the
 * stream is reached. Streams for which the end offfset is unknown (unbounded streams) are
 * represented by a stop offset value of {@link #OFFSET_INFINITY}.
 *
 * <p>Unbounded streams can't be split into offset ranges since rows aren't assigned offsets by the
 * server until the offset is reached by a reader. (In other words, you can't start reading at
 * stream S0 offset 1000 without first starting a read stream at stream S0 offset 0 and reading 1000
 * rows.) For this reason, unbounded ranges can't be split with calls to {@link
 * #trySplitAtPosition(StreamPosition)}. Instead, the BigQuery storage read API supports splitting
 * an existing, non-finalized stream into a pair of child streams which maintain the property that,
 * by reading from offset 0 in the primary child to the end of the stream and then reading from
 * offset 0 in the residual stream to the end of the stream, the caller will observe the same data
 * (rows) in the same order as when reading from offset 0 in the parent stream to the end of the
 * stream.
 *
 * <p>Since RPCs are not allowed from within {@link BoundedReader#splitAtFraction(double)}, invoking
 * a server- side split is a two-part operation. First, from within an arbitrary thread context, the
 * caller updates the state of the tracker to indicate that a split operation is <i>pending</i>,
 * causing any subsequent calls to {@link #tryReturnRecordAt(boolean, StreamPosition)} will fail.
 * When the reader thread (e.g. the thread which calls {@link BoundedReader#start()} and {@link
 * BoundedReader#advance()} attempts to return a record and observes the failure, it then performs
 * the split RPC call and updates the state of the tracker to reflect the split results through a
 * call to the {@link #resolvePendingSplit(Stream, long, long)} method.
 *
 * <p>A pending split can be resolved either by updating the stream context of the tracker or by
 * updating the offset range of the tracker to set a {@link #stopOffset} value which is not {@link
 * #OFFSET_INFINITY}. When the range becomes bounded, the range tracker behaves like a standard
 * {@link org.apache.beam.sdk.io.range.OffsetRangeTracker}, allowing the range to be subdivided into
 * sub-ranges by {@link #trySplitAtPosition(StreamPosition)}.
 */
public class StreamOffsetRangeTracker implements RangeTracker<StreamPosition> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamOffsetRangeTracker.class);

  /**
   * Offset corresponding to infinity. This can only be used as the upper-bound of a range, and
   * indicates reading all of the records until the end without specifying exactly what the end is.
   */
  public static final long OFFSET_INFINITY = Long.MAX_VALUE;

  private Stream stream;
  private long startOffset;
  private long stopOffset;
  private long lastRecordStart = -1L;
  private boolean splitPending;
  private boolean done = false;

  public StreamOffsetRangeTracker(
      Stream stream, long startOffset, long stopOffset, boolean splitPending) {
    this.stream = checkNotNull(stream, "stream");
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    this.splitPending = splitPending;
  }

  private StreamOffsetRangeTracker() {}

  public synchronized boolean isStarted() {
    return lastRecordStart != -1L || done;
  }

  public synchronized boolean isDone() {
    return done;
  }

  @Override
  public synchronized StreamPosition getStartPosition() {
    return StreamPosition.newBuilder().setStream(stream).setOffset(startOffset).build();
  }

  public synchronized long getStartOffset() {
    return startOffset;
  }

  @Override
  public synchronized StreamPosition getStopPosition() {
    return StreamPosition.newBuilder().setStream(stream).setOffset(stopOffset).build();
  }

  public synchronized long getStopOffset() {
    return stopOffset;
  }

  @Override
  public boolean tryReturnRecordAt(boolean isAtSplitPoint, StreamPosition recordStart) {
    checkArgument(isAtSplitPoint, "All stream offsets must be split points");
    return tryReturnRecordAt(recordStart);
  }

  private synchronized boolean tryReturnRecordAt(StreamPosition recordStart) {
    if (!stream.equals(recordStart.getStream())) {
      throw new IllegalStateException(
          String.format(
              "Trying to return a record in stream context [%s] "
                  + "which is not the current stream context [%s]",
              recordStart.getStream(), stream));
    }

    if (recordStart.getOffset() < startOffset) {
      throw new IllegalStateException(
          String.format(
              "Trying to return a record at offset [%d] which is before the range start [%d]",
              recordStart.getOffset(), startOffset));
    }

    if (recordStart.getOffset() <= lastRecordStart) {
      throw new IllegalStateException(
          String.format(
              "Trying to return a record at offset [%d] "
                  + "which is at or before the offset of the last returned record [%d]",
              recordStart.getOffset(), lastRecordStart));
    }

    if (splitPending) {
      LOG.info("Refusing to return record at {}; split pending", recordStart);
      return false;
    }

    lastRecordStart = recordStart.getOffset();
    if (lastRecordStart >= stopOffset) {
      done = true;
      return false;
    }

    return true;
  }

  @Override
  public synchronized boolean trySplitAtPosition(StreamPosition splitOffset) {
    if (stopOffset == OFFSET_INFINITY) {
      LOG.info("Refusing to split {} at {}; stop position unspecified", this, splitOffset);
      return false;
    }

    if (!isStarted()) {
      LOG.info("Refusing to split {} at {}; unstarted", this, splitOffset);
      return false;
    }

    if (!stream.equals(splitOffset.getStream())) {
      LOG.info("Refusing to split {} at {}; stream context mismatch", this, splitOffset);
      return false;
    }

    if (splitOffset.getOffset() < startOffset || splitOffset.getOffset() >= stopOffset) {
      LOG.info("Refusing to split {} at {}; proposed split offset out of range", this, splitOffset);
      return false;
    }

    if (splitOffset.getOffset() <= lastRecordStart) {
      LOG.info("Refusing to split {} at {}; already past proposed split offset", this, splitOffset);
      return false;
    }

    checkState(!splitPending, "A split should never be pending for a bounded range");
    LOG.info("Agreeing to split {} at {}", this, splitOffset);
    this.stopOffset = splitOffset.getOffset();
    return true;
  }

  /**
   * Atomically updates the internal state to indicate that a server-side split operation is
   * pending. Any calls to {@link #tryReturnRecordAt(boolean, StreamPosition)} will fail until the
   * split is resolved through a call to {@link #resolvePendingSplit(Stream, long, long)}.
   */
  public synchronized boolean tryMarkPendingSplit(StreamPosition currentOffset) {
    if (stopOffset != OFFSET_INFINITY) {
      LOG.info("Refusing pending split for {} at {}: stop position specified", this, currentOffset);
      return false;
    }

    if (!isStarted()) {
      LOG.info("Refusing pending split for {} at {}: unstarted", this, currentOffset);
      return false;
    }

    if (!stream.equals(currentOffset.getStream())) {
      LOG.info("Refusing pending split for {} at {}: stream context mismatch", this, currentOffset);
      return false;
    }

    if (currentOffset.getOffset() < startOffset || currentOffset.getOffset() >= stopOffset) {
      LOG.info(
          "Refusing pending split of {} at {}; current offset out of range", this, currentOffset);
      return false;
    }

    if (currentOffset.getOffset() != lastRecordStart) {
      LOG.info("Refusing pending split of {} at {}; current offset outdated", this, currentOffset);
      return false;
    }

    if (splitPending) {
      LOG.info("Refusing pending split of {} at {}; split already pending", this, currentOffset);
      return false;
    }

    LOG.info("Agreeing pending split of {} at {}", this, currentOffset);
    splitPending = true;
    return true;
  }

  /**
   * Atomically updates the internal state to reflect the results of a server-side split operation.
   *
   * <p>This method may not update both the stream context and the offset range for the tracker
   * except in the case where {@link #isStarted()} is false.
   */
  public synchronized void resolvePendingSplit(Stream stream, long startOffset, long stopOffset) {

    checkState(
        this.stopOffset == OFFSET_INFINITY,
        "We should never attempt to resolve a server-side split operation for a bounded range");

    checkState(
        splitPending,
        "We should never attempt to resolve a server-side split without a pending split");

    if (isStarted()) {

      //
      // The current reader must be the primary, so we should never try to change both the stream
      // context and the range boundaries at the same time.
      //

      checkState(
          this.stream.equals(stream)
              || (startOffset == this.startOffset && stopOffset == OFFSET_INFINITY),
          "We should never attempt to update both the stream context and the offset range "
              + "for a started range tracker");
    }

    LOG.debug(
        "Resolving pending split of {} to stream {}, offset range [{}, {})",
        this,
        stream,
        startOffset,
        stopOffset);
    this.stream = checkNotNull(stream, "stream");
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    this.splitPending = false;
  }

  public synchronized StreamPosition getPositionForLastRecordStart() {
    return StreamPosition.newBuilder().setStream(stream).setOffset(lastRecordStart).build();
  }

  public synchronized StreamPosition getPositionForFractionConsumed(double fraction) {

    checkState(
        stopOffset != OFFSET_INFINITY,
        "We should never attempt getPositionForFractionConsumed on an unbounded range");

    long splitOffset = (long) Math.floor(startOffset + fraction * (stopOffset - startOffset));
    return StreamPosition.newBuilder().setStream(stream).setOffset(splitOffset).build();
  }

  @Override
  public synchronized double getFractionConsumed() {
    return getOrEstimateFractionConsumed(OFFSET_INFINITY);
  }

  public synchronized double getOrEstimateFractionConsumed(long estimatedStopOffset) {
    if (!isStarted()) {
      return 0.0;
    } else if (isDone()) {
      return 1.0;
    } else if (stopOffset != OFFSET_INFINITY) {
      return Math.min(1.0, 1.0 * (lastRecordStart - startOffset) / (stopOffset - startOffset));
    } else if (estimatedStopOffset != OFFSET_INFINITY) {
      return Math.min(
          1.0, 1.0 * (lastRecordStart - startOffset) / (estimatedStopOffset - startOffset));
    } else {
      return 0.0;
    }
  }

  public synchronized boolean markDone() {
    done = true;
    return false;
  }

  @Override
  public String toString() {
    String stop = (stopOffset == OFFSET_INFINITY) ? "infinity" : String.valueOf(stopOffset);
    if (lastRecordStart >= 0) {
      return String.format(
          "<stream %s at offset %d in range [%d, %s)>",
          stream.getName(), lastRecordStart, startOffset, stop);
    } else {
      return String.format(
          "<stream %s (unstarted) in range [%d, %s)>", stream.getName(), startOffset, stop);
    }
  }

  @VisibleForTesting
  StreamOffsetRangeTracker copy() {
    StreamOffsetRangeTracker copy = new StreamOffsetRangeTracker();
    synchronized (this) {
      synchronized (copy) {
        copy.stream = this.stream;
        copy.startOffset = this.startOffset;
        copy.stopOffset = this.stopOffset;
        copy.lastRecordStart = this.lastRecordStart;
        copy.splitPending = this.splitPending;
        copy.done = this.done;
        return copy;
      }
    }
  }
}
