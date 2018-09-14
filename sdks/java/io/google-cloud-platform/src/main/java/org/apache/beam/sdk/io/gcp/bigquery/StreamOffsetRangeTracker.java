package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.io.range.RangeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeTracker} for non-negative offsets within a BigQuery {@link Stream} context.
 *
 * <p>The BigQuery read API provides server-side liquid sharding rather than static partitioning, so
 * the length of a stream isn't known in some cases until the end of the stream is reached. Streams
 * for which the end offset is unknown (unbounded streams) are represented by a stop offset value of
 * {@link #OFFSET_INFINITY}.
 *
 * <p>Unbounded streams can't be split into offset ranges since offsets aren't assigned on the
 * server until reached by a reader. For this reason, an unbounded range tracker can't be split with
 * a call to {@link #trySplitAtPosition(StreamPosition)}. Instead, the BigQuery read API supports
 * splitting an existing, non-finalized stream on the server. However, since RPCs are not allowed
 * from within {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader#splitAtFraction(double)},
 * invoking a server-side split is a two-part operation. First, from {@code splitAtFraction}, the
 * caller updates the tracker state to indicate that a split is <i>pending</i>. When this state
 * transition is invoked, any subsequent calls to {@link #tryReturnRecordAt(StreamPosition)} will
 * fail until the split is subsequently <i>resolved</i> from within
 * {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader#start()} or
 * {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader#advance()} through a call to
 * {@link #resolvePendingSplit(Stream, long, long)}, which is permitted to update both the stream
 * context and the offset range for the tracker.
 *
 * <p>Once the offset range of the tracker becomes bounded (e.g. the stop offset is updated to a
 * value other than {@link #OFFSET_INFINITY}), then the range tracker behaves like a standard
 * {@link org.apache.beam.sdk.io.range.OffsetRangeTracker}. The offset range can be subdivided into
 * sub-ranges by {@link #trySplitAtPosition(StreamPosition)}, and any calls to
 * {@link #tryMarkPendingSplit(StreamPosition)} are rejected.
 */
public class StreamOffsetRangeTracker implements RangeTracker<StreamPosition> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamOffsetRangeTracker.class);

  /**
   * Offset corresponding to infinity. This can only be used as the upper bound of a range, and
   * indicates reading all of the records until the end of the range without specifying the exact
   * offset at which the range ends.
   *
   * <p>Unbounded ranges (where the stop offset is {@code OFFSET_INFINITY}) cannot be split with
   * {@link #trySplitAtPosition(Object)} since the underlying streams can't be partitioned into
   * offset ranges.
   *
   * <p>Bounded ranges (where the stop offset is a non-{@code OFFSET_INFINITY} value) cannot be
   * split with {@link #tryMarkPendingSplit(StreamPosition)}.
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

  private StreamOffsetRangeTracker() { }

  public synchronized boolean isStarted() {
    return (lastRecordStart != -1) || done;
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
    return tryReturnRecordAt(checkNotNull(recordStart));
  }

  private synchronized boolean tryReturnRecordAt(StreamPosition recordStart) {
    if (!stream.equals(recordStart.getStream())) {
      throw new IllegalStateException(String.format(
          "Trying to return record [starting at %s] which is in the wrong stream context [%s]",
          recordStart, stream));
    }

    if (recordStart.getOffset() < startOffset) {
      throw new IllegalStateException(String.format(
          "Trying to return record [starting at %s] which is before the range start [%d]",
          recordStart, startOffset));
    }

    if (recordStart.getOffset() <= lastRecordStart) {
      throw new IllegalStateException(String.format(
          "Trying to return record [starting at %s] "
              + "which is at or before the last-returned record [starting at %d]",
          recordStart, lastRecordStart));
    }

    if (splitPending) {
      LOG.debug("Refusing to return record at {} for {}; split pending", recordStart, this);
      return false;
    }

    lastRecordStart = recordStart.getOffset();

    if (recordStart.getOffset() >= stopOffset) {
      done = true;
      return false;
    }

    return true;
  }

  @Override
  public synchronized boolean trySplitAtPosition(StreamPosition splitOffset) {
    if (stopOffset == OFFSET_INFINITY) {
      LOG.debug("Refusing to split {} at {}; stop position unspecified", this, splitOffset);
      return false;
    }

    if (!isStarted()) {
      LOG.debug("Refusing to split {} at {}; unstarted", this, splitOffset);
      return false;
    }

    if (!stream.equals(splitOffset.getStream())) {
      LOG.debug("Refusing to split {} at {}; stream context mismatch", this, splitOffset);
      return false;
    }

    if (splitOffset.getOffset() < startOffset || splitOffset.getOffset() >= stopOffset) {
      LOG.debug(
          "Refusing to split {} at {}; proposed split position out of range", this, splitOffset);
      return false;
    }

    if (splitOffset.getOffset() <= lastRecordStart) {
      LOG.debug(
          "Refusing to split {} at {}; already past proposed split position", this, splitOffset);
      return false;
    }

    checkState(!splitPending, "A split should never be pending for a bounded range");

    LOG.debug("Agreeing to split {} at {}", this, splitOffset);
    this.stopOffset = splitOffset.getOffset();
    return true;
  }

  /**
   * Atomically updates the internal state to indicate that a server-side split operation is
   * pending. Any calls to {@link #tryReturnRecordAt(StreamPosition)} will fail until the split is
   * resolved by {@link #resolvePendingSplit(Stream, long, long)}.
   */
  public synchronized boolean tryMarkPendingSplit(StreamPosition currentOffset) {
    if (stopOffset != OFFSET_INFINITY) {
      LOG.debug("Refusing pending split for {} at {}; stop position known", this, currentOffset);
      return false;
    }

    if (!isStarted()) {
      LOG.debug("Refusing pending split for {} at {}; unstarted", this, currentOffset);
      return false;
    }

    if (!stream.equals(currentOffset.getStream())) {
      LOG.debug(
          "Refusing pending split for {} at {}; stream context mismatch", this, currentOffset);
      return false;
    }

    if (currentOffset.getOffset() != lastRecordStart) {
      LOG.debug(
          "Refusing pending split for {} at {}; current offset out of date", this, currentOffset);
      return false;
    }

    if (splitPending) {
      LOG.debug("Refusing pending split for {} at {}; split already pending", this, currentOffset);
      return false;
    }

    LOG.debug("Acknowledging pending split of {} at {}", this, currentOffset);
    splitPending = true;
    return true;
  }

  /**
   * Atomically updates the internal state to reflect the results of a server-side split operation.
   *
   * <p>This method may not update both the stream context and the offset range for the range
   * tracker except in the case where {@link #isStarted()} is false.
   */
  public synchronized void resolvePendingSplit(Stream stream, long startOffset, long stopOffset) {
    checkState(
        this.stopOffset == OFFSET_INFINITY,
        "A server-side split operation should never be attempted for a bounded range");

    checkState(
        splitPending,
        "Attempting to resolve a pending split operation for a tracker with no split pending");

    if (isStarted()) {
      // We're updating the current range to be the primary, so we should never change both the
      // stream context and the range boundaries at the same time.
      checkState(
          this.stream.equals(stream)
              || (this.startOffset == startOffset && stopOffset == OFFSET_INFINITY),
          "Attempting to update both stream context %s and offset range [%d, %d) for %s",
          stream, startOffset, stopOffset, this);
    }

    LOG.debug(
        "Resolving pending split of {} to stream {}, offset range [{}, {})",
        this, stream, startOffset, stopOffset);

    this.stream = checkNotNull(stream, "stream");
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    this.splitPending = false;
  }

  public synchronized StreamPosition getPositionForLastRecordStart() {
    return StreamPosition.newBuilder().setStream(stream).setOffset(lastRecordStart).build();
  }

  public synchronized StreamPosition getPositionForFractionConsumed(double fraction) {
    // It's OK to throw in this case since there's never a transition from a bounded range to an
    // unbounded range (callers will never read the stop offset of the range as something other than
    // OFFSET_INFINITY and then
    if (stopOffset == OFFSET_INFINITY) {
      throw new IllegalStateException(String.format(
          "getPositionForFractionConsumed is not applicable to an unbounded range: %s", this));
    }

    long splitOffset = (long) Math.floor(startOffset + fraction * (stopOffset - startOffset));
    return StreamPosition.newBuilder().setStream(stream).setOffset(splitOffset).build();
  }

  public synchronized double getFractionConsumed() {
    if (!isStarted()) {
      return 0.0;
    } else if (isDone()) {
      return 1.0;
    } else if (stopOffset == OFFSET_INFINITY) {
      return 0.0;
    } else if (lastRecordStart >= stopOffset) {
      return 1.0;
    } else {
      return Math.min(1.0, 1.0 * (lastRecordStart - startOffset) / (stopOffset - startOffset));
    }
  }

  public synchronized Double getOrEstimateFractionConsumed(long estimatedStopOffset) {
    if (!isStarted()) {
      return 0.0;
    } else if (isDone()) {
      return 1.0;
    } else if (stopOffset != OFFSET_INFINITY) {
      return Math.min(1.0, 1.0 * (lastRecordStart - startOffset) / (stopOffset - startOffset));
    } else if (estimatedStopOffset != OFFSET_INFINITY) {
      return Math.min(1.0,
          1.0 * (lastRecordStart - startOffset) / (estimatedStopOffset - startOffset));
    } else {
      return null;
    }
  }

  public synchronized boolean markDone() {
    done = true;
    return false;
  }

  @Override
  public synchronized String toString() {
    String stopString = (stopOffset == OFFSET_INFINITY) ? "infinity" : String.valueOf(stopOffset);
    if (lastRecordStart >= 0) {
      return String.format(
          "<stream %s at [starting at %d] of offset range [%d, %s)>",
          stream, lastRecordStart, startOffset, stopString);
    } else {
      return String.format(
          "<stream %s unstarted in offset range [%d, %s)>", stream, startOffset, stopString);
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
