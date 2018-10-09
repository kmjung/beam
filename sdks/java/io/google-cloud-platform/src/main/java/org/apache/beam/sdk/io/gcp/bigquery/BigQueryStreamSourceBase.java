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

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1alpha1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Javadoc comment. */
@Experimental(Kind.SOURCE_SINK)
public abstract class BigQueryStreamSourceBase<InputT, OutputT> extends BoundedSource<OutputT> {

  /** A value representing the split disposition of the source. */
  enum SplitDisposition {
    SELF,
    PRIMARY,
    RESIDUAL,
  }

  private final ReadSession readSession;
  private final Stream stream;
  private final long startOffset;
  private final long stopOffset;
  private final SplitDisposition splitDisposition;
  private final StreamPosition splitPosition;
  private final SerializableFunction<InputT, OutputT> parseFn;
  private final Coder<OutputT> outputCoder;
  private final BigQueryServices bqServices;

  protected BigQueryStreamSourceBase(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      @Nullable StreamPosition splitPosition,
      SerializableFunction<InputT, OutputT> parseFn,
      Coder<OutputT> outputCoder,
      BigQueryServices bqServices) {
    this.readSession = checkNotNull(readSession, "readSession");
    this.stream = checkNotNull(stream, "stream");
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    this.splitDisposition = checkNotNull(splitDisposition, "splitDisposition");
    this.splitPosition = splitPosition;
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
  }

  @VisibleForTesting
  ReadSession getReadSession() {
    return readSession;
  }

  @VisibleForTesting
  Stream getStream() {
    return stream;
  }

  @VisibleForTesting
  long getStartOffset() {
    return startOffset;
  }

  @VisibleForTesting
  long getStopOffset() {
    return stopOffset;
  }

  @VisibleForTesting
  SplitDisposition getSplitDisposition() {
    return splitDisposition;
  }

  @VisibleForTesting
  @Nullable
  StreamPosition getSplitPosition() {
    return splitPosition;
  }

  @VisibleForTesting
  BigQueryServices getBigQueryServices() {
    return bqServices;
  }

  @VisibleForTesting
  SerializableFunction<InputT, OutputT> getParseFn() {
    return parseFn;
  }

  @Override
  public void validate() {
    checkState(startOffset >= 0, "startOffset [%s] must be non-negative", startOffset);
    checkState(stopOffset >= 0, "stopOffset [%s] must be non-negative", stopOffset);
    checkState(
        stopOffset >= startOffset,
        "stopOffset [%s] cannot be less than startOffset [%s]",
        stopOffset,
        startOffset);
    if (splitDisposition == SplitDisposition.SELF) {
      checkState(
          splitPosition == null, "Split position cannot be specified when disposition is SELF");
    } else {
      checkState(
          splitPosition != null, "Split position must be specified when disposition is not SELF");
    }
  }

  @Override
  public Coder<OutputT> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("readSession", readSession.getName()).withLabel("Read session"));
    builder.add(DisplayData.item("stream", stream.getName()).withLabel("Stream"));
  }

  @Override
  public List<? extends BigQueryStreamSourceBase<InputT, OutputT>> split(
      long desiredBundleSize, PipelineOptions options) throws Exception {
    // Stream sources cannot be split until a reader is created.
    return ImmutableList.of(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    // We can't estimate the size of a single read stream due to server-side liquid sharding.
    return 0L;
  }

  protected abstract BigQueryStreamSourceBase<InputT, OutputT> newChildSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      StreamPosition splitPosition,
      SerializableFunction<InputT, OutputT> parseFn,
      Coder<OutputT> outputCoder,
      BigQueryServices bqServices);

  private BigQueryStreamSourceBase<InputT, OutputT> forChildStream(
      Stream childStream, long startOffset, long stopOffset) {
    return newChildSource(
        this.readSession,
        childStream,
        startOffset,
        stopOffset,
        SplitDisposition.SELF,
        null,
        this.parseFn,
        this.outputCoder,
        this.bqServices);
  }

  private BigQueryStreamSourceBase<InputT, OutputT> forSplit(
      StreamPosition splitPosition, SplitDisposition splitDisposition) {
    return newChildSource(
        this.readSession,
        this.stream,
        this.startOffset,
        this.stopOffset,
        splitDisposition,
        splitPosition,
        this.parseFn,
        this.outputCoder,
        this.bqServices);
  }

  private BigQueryStreamSourceBase<InputT, OutputT> forSubrange(long startOffset, long stopOffset) {
    return newChildSource(
        this.readSession,
        this.stream,
        startOffset,
        stopOffset,
        SplitDisposition.SELF,
        null,
        this.parseFn,
        this.outputCoder,
        this.bqServices);
  }

  /** A Javadoc comment. */
  @Experimental(Kind.SOURCE_SINK)
  public abstract static class BigQueryStreamReaderBase<InputT, OutputT>
      extends BoundedSource.BoundedReader<OutputT> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStreamReaderBase.class);

    private final StreamOffsetRangeTracker rangeTracker;
    private final BigQueryStorageClient client;

    @GuardedBy("this")
    private BigQueryStreamSourceBase<InputT, OutputT> currentSource;

    // This value can be read from any thread context, but is only written in the context of the
    // reader thread. The "volatile" keyword prevents read tearing.
    private volatile long estimatedStreamLength = StreamOffsetRangeTracker.OFFSET_INFINITY;

    // These values can only be accessed in the context of the reader thread.
    private ServerStream<ReadRowsResponse> responseStream;
    private Iterator<ReadRowsResponse> responseIterator;
    private StreamPosition currentPosition;

    BigQueryStreamReaderBase(
        BigQueryStreamSourceBase<InputT, OutputT> source, BigQueryOptions options)
        throws IOException {
      this.currentSource = source;
      this.client = source.bqServices.getStorageClient(options);
      this.rangeTracker =
          new StreamOffsetRangeTracker(
              source.stream,
              source.startOffset,
              source.stopOffset,
              source.splitDisposition != SplitDisposition.SELF);
    }

    @Override
    public final boolean start() throws IOException {
      return (startImpl() && rangeTracker.tryReturnRecordAt(true, currentPosition))
          || rangeTracker.markDone();
    }

    protected boolean startImpl() throws IOException {
      BigQueryStreamSourceBase<InputT, OutputT> source = getCurrentSource();
      LOG.info("Starting reader from source {}", source);
      if (source.splitDisposition == SplitDisposition.SELF) {
        currentPosition = rangeTracker.getStartPosition();
      } else {
        currentPosition = processSplit(source.splitPosition, source.splitDisposition);
      }

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseStream = client.readRowsCallable().call(request);
      responseIterator = responseStream.iterator();
      return readNextRecord();
    }

    @Override
    public final boolean advance() throws IOException {
      currentPosition =
          currentPosition.toBuilder().setOffset(currentPosition.getOffset() + 1).build();
      while (true) {
        if (!readNextRecord()) {
          return rangeTracker.markDone();
        }

        if (rangeTracker.tryReturnRecordAt(true, currentPosition)) {
          return true;
        }

        if (rangeTracker.isDone()) {
          return false;
        }

        //
        // At this point, the range tracker has returned false for an attempt to return a record at
        // a particular offset but is not done, which means a split must be pending. Invalidate the
        // read stream, including any state associated with the
        //

        invalidateStream();

        StreamPosition splitPosition =
            StreamPosition.newBuilder()
                .setStream(currentPosition.getStream())
                .setOffset(currentPosition.getOffset() - 1)
                .build();

        currentPosition = processSplit(splitPosition, SplitDisposition.PRIMARY);
        ReadRowsRequest request =
            ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
        responseStream = client.readRowsCallable().call(request);
        estimatedStreamLength = StreamOffsetRangeTracker.OFFSET_INFINITY;
        responseIterator = responseStream.iterator();
      }
    }

    private StreamPosition processSplit(
        StreamPosition splitPosition, SplitDisposition splitDisposition) {
      checkArgument(splitDisposition != SplitDisposition.SELF);
      SplitReadStreamResponse response = client.splitReadStream(splitPosition.getStream());

      checkState(
          response.hasPrimaryStream(),
          "Invalid split response [%s] missing primary stream",
          response);

      checkState(
          response.hasRemainderStream(),
          "Invalid split response [%s] missing remainder stream",
          response);

      Stream newStream;
      long newStartOffset;
      long newStopOffset;
      StreamPosition newPosition;

      if (response.getPrimaryStream().getRowCount() > splitPosition.getOffset()) {

        //
        // The split position is contained within the primary stream, so the primary source is the
        // primary stream ID with the same range offsets as the parent source, and the residual
        // source is the entirety of the remainder stream.
        //

        if (splitDisposition == SplitDisposition.PRIMARY) {
          newStream = response.getPrimaryStream();
          newStartOffset = rangeTracker.getStartOffset();
          newStopOffset = rangeTracker.getStopOffset();
          newPosition =
              StreamPosition.newBuilder()
                  .setStream(response.getPrimaryStream())
                  .setOffset(splitPosition.getOffset() + 1)
                  .build();
        } else {
          newStream = response.getRemainderStream();
          newStartOffset = 0L;
          newStopOffset = StreamOffsetRangeTracker.OFFSET_INFINITY;
          newPosition = StreamPosition.newBuilder().setStream(newStream).setOffset(0L).build();
        }

      } else {

        //
        // The split position is contained within the remainder stream, so the primary source is the
        // parent stream with the offset range [initial offset, split position + 1) -- +1 since the
        // upper bound of the offset range is exclusive -- and the residual source is the remainder
        // stream with the offset range [split position - primary stream length + 1, infinity).
        //
        // N.B. This means that the next call to start() or advance() on the primary reader will
        //      return false.
        //

        if (splitDisposition == SplitDisposition.PRIMARY) {
          newStream = splitPosition.getStream();
          newStartOffset = rangeTracker.getStartOffset();
          newStopOffset = splitPosition.getOffset() + 1;
          newPosition =
              StreamPosition.newBuilder().setStream(newStream).setOffset(newStopOffset).build();
        } else {
          newStream = response.getRemainderStream();
          newStartOffset =
              splitPosition.getOffset() - response.getPrimaryStream().getRowCount() + 1;
          newStopOffset = StreamOffsetRangeTracker.OFFSET_INFINITY;
          newPosition =
              StreamPosition.newBuilder().setStream(newStream).setOffset(newStartOffset).build();
        }
      }

      //
      // We're in a "split pending" state, so any attempt to return a record, split the range
      // tracker at an offset, or mark a server-side split as pending has been rejected. This also
      // means that the current source has not been updated, and can be modified with proper
      // synchronization.
      //

      BigQueryStreamSourceBase<InputT, OutputT> newSource =
          getCurrentSource().forChildStream(newStream, newStartOffset, newStopOffset);

      synchronized (this) {
        this.currentSource = newSource;
      }

      rangeTracker.resolvePendingSplit(newStream, newStartOffset, newStopOffset);
      return newPosition;
    }

    protected ReadRowsResponse readNextStreamResponse() {
      if (responseIterator == null || !responseIterator.hasNext()) {
        return null;
      }

      ReadRowsResponse nextResponse = responseIterator.next();
      if (nextResponse.hasStatus()) {
        estimatedStreamLength = nextResponse.getStatus().getTotalEstimatedRows();
      }

      return nextResponse;
    }

    public abstract boolean readNextRecord() throws IOException;

    public void invalidateStream() {
      if (responseStream != null) {
        responseStream.cancel();
      }
    }

    @Override
    public void close() {
      invalidateStream();
      client.close();
    }

    @Override
    public Double getFractionConsumed() {
      return rangeTracker.getOrEstimateFractionConsumed(estimatedStreamLength);
    }

    @Override
    public synchronized BigQueryStreamSourceBase<InputT, OutputT> getCurrentSource() {
      return currentSource;
    }

    @Override
    public synchronized BigQueryStreamSourceBase<InputT, OutputT> splitAtFraction(double fraction) {
      BigQueryStreamSourceBase<InputT, OutputT> source = getCurrentSource();
      if (source.splitDisposition != SplitDisposition.SELF) {
        return null;
      }

      BigQueryStreamSourceBase<InputT, OutputT> primary, residual;
      if (source.stopOffset == StreamOffsetRangeTracker.OFFSET_INFINITY) {
        StreamPosition currentPosition = rangeTracker.getPositionForLastRecordStart();
        primary = source.forSplit(currentPosition, SplitDisposition.PRIMARY);
        residual = source.forSplit(currentPosition, SplitDisposition.RESIDUAL);
        if (!rangeTracker.tryMarkPendingSplit(currentPosition)) {
          return null;
        }
      } else {
        StreamPosition splitPosition = rangeTracker.getPositionForFractionConsumed(fraction);
        primary = source.forSubrange(source.startOffset, splitPosition.getOffset());
        residual = source.forSubrange(splitPosition.getOffset(), source.stopOffset);
        if (!rangeTracker.trySplitAtPosition(splitPosition)) {
          return null;
        }
      }

      this.currentSource = primary;
      return residual;
    }
  }
}
