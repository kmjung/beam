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
 * partitioning of the underlying table data, so the length of a stream isn't known in some cases
 * until the end of the stream is reached. Streams for which the end offset is unknown (unbounded
 * ranges) are represented by a {@link #stopOffset} value of {@link #OFFSET_INFINITY}.
 *
 * <p>Unbounded streams can't be split into offset ranges since data isn't assigned to an offset by
 * the server until the offset is reached by a read stream; in other words, offset 1000 isn't valid
 * for a particular read stream until a reader has read to offset 1000 starting at the beginning of
 * the stream. For this reason, an unbounded range can't be split with a call to
 * {@link #trySplitAtPosition(StreamPosition)}. Instead, the BigQuery storage read API supports
 * splitting an existing, non-finalized stream into two child streams which, when read sequentially
 * from start to end, will return the same data as would be returned by reading the parent stream
 * from start to end. However, since RPCs are not allowed from within
 * {@link BoundedReader#splitAtFraction(double)}, performing a server-side split is a two-part
 * operation. First, from {@code splitAtFraction} on an arbitrary thread context, the caller
 * attempts to update the state of the tracker to indicate that a split is <i>pending</i> through a
 * call to {@link #tryMarkSplitPending(StreamPosition)}. When this state transition occurs, any
 * subsequent calls to {@link #tryReturnRecordAt(StreamPosition)} will fail. When the reader thread
 * (e.g. which calls {@link BoundedReader#start()} and {@link BoundedReader#advance()}) observes
 * such a failure, it then performs the split RPC and updates the tracker context to reflect the
 * results through a call to {@link #resolvePendingSplit(Stream, long, long)}.
 *
 * <p>At some point -- either by reading to the end of the stream, or as the result of a split
 * operation -- the length of a portion of the underlying stream becomes known to the reader, and
 * the range becomes bounded (e.g. the stop offset is updated to some value other than
 * {@link #OFFSET_INFINITY}). When this occurs, the range tracker behaves like a standard
 * {@link org.apache.beam.sdk.io.range.OffsetRangeTracker} and allows the underlying range to be
 * subdivided into sub-ranges by {@link #trySplitAtPosition(StreamPosition)}. Any subsequent calls
 * to {@link #tryMarkSplitPending(StreamPosition)} are rejected.
 */
public class StreamOffsetRangeTracker implements RangeTracker<StreamPosition> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamOffsetRangeTracker.class);

  private Stream streamContext;
  private long startOffset;
  private long stopOffset;
  private boolean splitPending;
  private long lastRecordStart = -1L;
  private boolean done = false;

  /**
   * Offset corresponding to infinity. This can only be used as the upper bound of a range, and
   * indicates reading all of the records in the stream without specifying the offset of the stream
   * end.
   */
  public static final long OFFSET_INFINITY = Long.MAX_VALUE;

  /**
   * Creates a {@link StreamOffsetRangeTracker} for the specified range.
   */
  public StreamOffsetRangeTracker(
      Stream streamContext, long startOffset, long stopOffset, boolean splitPending) {
    this.streamContext = checkNotNull(streamContext, "streamContext");
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    this.splitPending = splitPending;
  }

  private StreamOffsetRangeTracker() { }

  public synchronized boolean isStarted() {
    return lastRecordStart != -1 || done;
  }

  public synchronized boolean isDone() {
    return done;
  }

  @Override
  public StreamPosition getStartPosition() {
    return StreamPosition.newBuilder().setStream(streamContext).setOffset(startOffset).build();
  }

  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public StreamPosition getStopPosition() {
    return StreamPosition.newBuilder().setStream(streamContext).setOffset(stopOffset).build();
  }

  public long getStopOffset() {
    return stopOffset;
  }

  @Override
  public boolean tryReturnRecordAt(boolean isAtSplitPoint, StreamPosition recordStart) {
    checkArgument(isAtSplitPoint, "Every record in a stream must be a split point");
    return tryReturnRecordAt(recordStart);
  }

  private synchronized boolean tryReturnRecordAt(StreamPosition recordStart) {
    if (!streamContext.equals(recordStart.getStream())) {
      throw new IllegalStateException(
          String.format(
              "Trying to return a record in stream context [%s] "
                  + "which is not the current context [%s]",
              recordStart.getStream(), streamContext));
    }

    if (recordStart.getOffset() < startOffset) {
      throw new IllegalStateException(
          String.format(
              "Trying to return a record in stream context [%s] "
                  + "whose offset [%d] is before the start offset [%d]",
              streamContext, recordStart.getOffset(), startOffset));
    }

    if (recordStart.getOffset() <= lastRecordStart) {
      throw new IllegalStateException(
          String.format(
              "Trying to return a record in stream context [%s] "
                  + "whose offset [%d] is at or before the offset of the last returned record [%d]",
              streamContext, recordStart.getOffset(), lastRecordStart));
    }

    if (splitPending) {
      LOG.debug("Refusing to return record at {}; split pending", recordStart);
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
  public synchronized boolean trySplitAtPosition(StreamPosition splitPosition) {
    if (stopOffset == OFFSET_INFINITY) {
      LOG.warn("Refusing to split {} at {}: stop offset unspecified", this, splitPosition);
      return false;
    }

    checkState(!splitPending, "Split should never be pending for bounded ranges");

    if (!isStarted()) {
      LOG.info("Refusing to split {} at {}: unstarted", this, splitPosition);
      return false;
    }

    if (!streamContext.equals(splitPosition.getStream())) {
      LOG.info("Refusing to split {} at {}: stream context mismatch", this, splitPosition);
      return false;
    }

    if (splitPosition.getOffset() < startOffset || splitPosition.getOffset() >= stopOffset) {
      LOG.info(
          "Refusing to split {} at {}: proposed split offset out of range", this, splitPosition);
      return false;
    }

    if (splitPosition.getOffset() <= lastRecordStart) {
      LOG.info(
          "Refusing to split {} at {}: already past proposed split offset", this, splitPosition);
      return false;
    }

    LOG.info("Agreeing to split {} at {}", this, splitPosition);
    this.stopOffset = splitPosition.getOffset();
    return true;
  }

  /**
   * Atomically updates the internal state to indicate that the reader should perform a server-side
   * split of the underlying stream. Any calls to {@link #tryReturnRecordAt(StreamPosition)} will
   * fail until the pending split is resolved through a call to
   * {@link #resolvePendingSplit(Stream, long, long)}.
   *
   * @param currentPosition The offset of the last returned record.
   */
  public synchronized boolean tryMarkSplitPending(StreamPosition currentPosition) {
    if (stopOffset != OFFSET_INFINITY) {
      LOG.warn("Refusing pending split of {} at {}: stop offset specified", this, currentPosition);
      return false;
    }

    if (!isStarted()) {
      LOG.info("Refusing pending split of {} at {}: unstarted", this, currentPosition);
      return false;
    }

    if (!streamContext.equals(currentPosition.getStream())) {
      LOG.info(
          "Refusing pending split of {} at {}: stream context mismatch", this, currentPosition);
      return false;
    }

    if (currentPosition.getOffset() != lastRecordStart) {
      LOG.info(
          "Refusing pending split of {} at {}: current position mismatch", this, currentPosition);
      return false;
    }

    if (splitPending) {
      LOG.info("Refusing pending split of {} at {}: split already pending", this, currentPosition);
      return false;
    }

    LOG.info("Acknowledging pending split of {} at {}", this, currentPosition);
    splitPending = true;
    return true;
  }

  /**
   * Atomically updates the internal state to reflect the result of a server-side split operation.
   *
   * <p>This method may not modify both the stream context and the offset range at the same time
   * except in the case where {@link #isStarted()} is false.
   */
  public synchronized void resolvePendingSplit(
      Stream streamContext, long startOffset, long stopOffset) {
    if (!splitPending) {
      throw new IllegalStateException(
          String.format(
              "Attempting to resolve a pending split [%s, %d, %d] "
                  + "against a range tracker with no split pending",
              streamContext, startOffset, stopOffset, this));
    }

    checkState(
        this.stopOffset == OFFSET_INFINITY,
        "Server-side splits can only be performed against unbounded ranges");

    if (isStarted()) {
      // The only case in which a server-side split operation may cause both the stream context and
      // offset range bounds to change is at initial reader creation time, when the underlying
      // source object represents the residual child of the named stream; otherwise, a split
      // operation can only update the stream context or the offset range.
      if (!this.streamContext.equals(streamContext)
          && (startOffset != this.startOffset || stopOffset != OFFSET_INFINITY)) {
        throw new IllegalStateException(
            String.format(
                "Attempting to update both the stream context [%s] and the offset range [%d, %d) "
                    + "when resolving a pending split for [%s]",
                streamContext, startOffset, stopOffset, this));
      }
    }

    LOG.info(
        "Resolving pending split of {} to stream {}, offset [{}, {})",
        this, streamContext, startOffset, stopOffset);

    this.streamContext = streamContext;
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    splitPending = false;
  }

  public synchronized StreamPosition getPositionForFractionConsumed(double fraction) {
    if (stopOffset == OFFSET_INFINITY) {
      throw new IllegalArgumentException(
          String.format(
              "getPositionForFractionConsumed is not applicable to an unbounded range: %s",
              this));
    }

    long offset = (long) Math.floor(startOffset + fraction * (stopOffset - startOffset));
    return StreamPosition.newBuilder().setStream(streamContext).setOffset(offset).build();
  }

  public synchronized StreamPosition getPositionForLastRecordStart() {
    return StreamPosition.newBuilder().setStream(streamContext).setOffset(lastRecordStart).build();
  }

  @Override
  public synchronized double getFractionConsumed() {
    return getOrEstimateFractionConsumed(OFFSET_INFINITY);
  }

  public synchronized Double getOrEstimateFractionConsumed(long estimatedStopOffset) {
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
          "<at offset %d in stream %s range [%d, %s)>",
          lastRecordStart, streamContext.getName(), startOffset, stopString);
    } else {
      return String.format(
          "<unstarted in stream %s range [%d, %s)>",
          streamContext.getName(), startOffset, stopString);
    }
  }

  @VisibleForTesting
  StreamOffsetRangeTracker copy() {
    StreamOffsetRangeTracker result = new StreamOffsetRangeTracker();
    synchronized (this) {
      synchronized (result) {
        result.streamContext = streamContext;
        result.startOffset = startOffset;
        result.stopOffset = stopOffset;
        result.splitPending = splitPending;
        result.lastRecordStart = lastRecordStart;
        result.done = done;
      }
    }
    return result;
  }
}
