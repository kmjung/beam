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

import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.ServerStream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.TableReadService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A source to read from a BigQuery storage read stream. */
@Experimental(Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

  /** A value representing the split disposition of the source. */
  enum SplitDisposition {
    /** The current source represents the specified stream. */
    SELF,

    /** The current source represents the primary child of the specified stream. */
    PRIMARY,

    /** The current source represents the residual child of the specified stream. */
    RESIDUAL,
  }

  static <T> BigQueryStorageStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSource<>(
        readSession,
        stream,
        0,
        StreamOffsetRangeTracker.OFFSET_INFINITY,
        SplitDisposition.SELF,
        null,
        parseFn,
        outputCoder,
        bqServices);
  }

  private final ReadSession readSession;
  private final Stream stream;
  private final long startOffset;
  private final long stopOffset;
  private final SplitDisposition splitDisposition;
  private final StreamPosition splitPosition;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private BigQueryStorageStreamSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      StreamPosition splitPosition,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
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
  StreamPosition getSplitPosition() {
    return splitPosition;
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
      checkState(splitPosition == null, "Split position cannot be specified with disposition SELF");
    } else {
      checkState(
          splitPosition != null, "Split position must be specified when disposition is not SELF");
    }
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("readSession", readSession.getName()).withLabel("Read session"));
    builder.add(DisplayData.item("stream", stream.getName()).withLabel("Stream"));
  }

  @Override
  public List<? extends BigQueryStorageStreamSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    return ImmutableList.of(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0L;
  }

  @Override
  public BigQueryStorageStreamReader<T> createReader(PipelineOptions options) throws IOException {
    return new BigQueryStorageStreamReader<>(this, options.as(BigQueryOptions.class));
  }

  private BigQueryStorageStreamSource<T> forChildStream(
      StreamPosition splitPosition, SplitDisposition disposition) {
    checkArgument(disposition != SplitDisposition.SELF);

    checkState(
        splitDisposition == SplitDisposition.SELF,
        "Attempting to split a source whose disposition [%s] is not SELF",
        splitDisposition);

    checkState(
        stopOffset == StreamOffsetRangeTracker.OFFSET_INFINITY,
        "Attempting to create a split child source from a bounded range [%s]",
        this);

    return new BigQueryStorageStreamSource<>(
        readSession,
        stream,
        startOffset,
        stopOffset,
        disposition,
        splitPosition,
        parseFn,
        outputCoder,
        bqServices);
  }

  private BigQueryStorageStreamSource<T> forSubrange(long start, long stop) {
    checkArgument(
        start >= startOffset,
        "Start offset of the subrange [%s] "
            + "cannot be larger than the start offset of the parent range [%s]",
        start,
        startOffset);

    checkArgument(
        stop <= stopOffset,
        "Stop offset of the subrange [%s] "
            + "cannot be larger than the start offset of the parent range [%s]",
        stop,
        stopOffset);

    checkState(
        splitDisposition == SplitDisposition.SELF,
        "Attempting to split a source whose disposition [%s] is not SELF",
        splitDisposition);

    checkState(
        stopOffset != StreamOffsetRangeTracker.OFFSET_INFINITY,
        "Attempting to create a subrange from an unbounded range [%s]",
        this);

    return new BigQueryStorageStreamSource<>(
        readSession,
        stream,
        start,
        stop,
        SplitDisposition.SELF,
        null,
        parseFn,
        outputCoder,
        bqServices);
  }

  /** A {@link BoundedSource.BoundedReader} to read from a {@link BigQueryStorageStreamSource}. */
  @Experimental(Kind.SOURCE_SINK)
  public static class BigQueryStorageStreamReader<T> extends BoundedSource.BoundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamReader.class);

    private final TableReadService client;
    private final SerializableFunction<SchemaAndRowProto, T> parseFn;
    private final StructType schema;
    private final StreamOffsetRangeTracker rangeTracker;

    @GuardedBy("this")
    private BigQueryStorageStreamSource<T> currentSource;

    // This value can be read from any thread, although it is only written by the reader thread. The
    // "volatile" keyword prevents read and write tearing.
    private volatile long estimatedStreamLength = StreamOffsetRangeTracker.OFFSET_INFINITY;

    // These values can only be accessed by the reader thread.
    private ServerStream<ReadRowsResponse> responseStream;
    private Iterator<ReadRowsResponse> responseIterator;
    private Iterator<Row> rowIterator;
    private StreamPosition currentPosition;
    private T currentRecord;

    BigQueryStorageStreamReader(BigQueryStorageStreamSource<T> source, BigQueryOptions options)
        throws IOException {
      this.currentSource = source;
      this.client = source.bqServices.getTableReadService(options);
      this.parseFn = source.parseFn;
      this.schema = source.readSession.getProjectedSchema();
      this.rangeTracker =
          new StreamOffsetRangeTracker(
              source.stream,
              source.startOffset,
              source.stopOffset,
              source.splitDisposition != SplitDisposition.SELF);
    }

    @Override
    public boolean start() throws IOException {
      return (startImpl() && rangeTracker.tryReturnRecordAt(true, currentPosition))
          || rangeTracker.markDone();
    }

    private boolean startImpl() {
      BigQueryStorageStreamSource<T> source = getCurrentSource();
      LOG.info("Starting reader {} from source {}", this, source);
      if (source.splitDisposition == SplitDisposition.SELF) {
        currentPosition = rangeTracker.getStartPosition();
      } else {
        currentPosition = processSplit(source.splitPosition, source.splitDisposition);
      }

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseStream = client.readRows(request);
      responseIterator = responseStream.iterator();
      rowIterator = null;
      return readNextRow();
    }

    @Override
    public boolean advance() throws IOException {

      currentPosition =
          currentPosition.toBuilder().setOffset(currentPosition.getOffset() + 1).build();

      while (true) {
        if (!readNextRow()) {
          return rangeTracker.markDone();
        }

        if (rangeTracker.tryReturnRecordAt(true, currentPosition)) {
          return true;
        }

        if (rangeTracker.isDone()) {
          return false;
        }

        if (responseStream != null) {
          responseStream.cancel();
          responseStream = null;
        }

        StreamPosition splitPosition =
            StreamPosition.newBuilder()
                .setStream(currentPosition.getStream())
                .setOffset(currentPosition.getOffset() - 1)
                .build();

        currentPosition = processSplit(splitPosition, SplitDisposition.PRIMARY);

        ReadRowsRequest request =
            ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
        responseStream = client.readRows(request);
        responseIterator = responseStream.iterator();
        rowIterator = null;
        estimatedStreamLength = StreamOffsetRangeTracker.OFFSET_INFINITY;
      }
    }

    private StreamPosition processSplit(
        StreamPosition splitPosition, SplitDisposition disposition) {
      checkArgument(disposition != SplitDisposition.SELF);

      SplitReadStreamRequest request =
          SplitReadStreamRequest.newBuilder().setOriginalStream(splitPosition.getStream()).build();
      SplitReadStreamResponse response = client.splitReadStream(request);

      checkState(
          response.hasPrimaryStream(),
          "Invalid split response [%s] missing primary stream",
          response);
      checkState(
          response.hasRemainderStream(),
          "Invalid split response [%s] missing remainder stream",
          response);

      if (response.getPrimaryStream().getRowCount() > splitPosition.getOffset()) {

        //
        // The split position is contained within the primary stream, so the primary source is the
        // primary stream ID with the same offset range as the parent source and the residual source
        // is the entirety of the remainder stream.
        //
        // We're in a "split pending" state, so any attempt to return a record, split the range
        // tracker at an offset, or mark a server-side split as pending has been rejected. This
        // also means that the current source has not been modified, and can be updated with proper
        // synchronization.
        //

        if (disposition == SplitDisposition.PRIMARY) {

          BigQueryStorageStreamSource<T> newSource =
              new BigQueryStorageStreamSource<>(
                  currentSource.readSession,
                  response.getPrimaryStream(),
                  currentSource.startOffset,
                  currentSource.stopOffset,
                  SplitDisposition.SELF,
                  null,
                  currentSource.parseFn,
                  currentSource.outputCoder,
                  currentSource.bqServices);

          synchronized (this) {
            this.currentSource = newSource;
          }

          rangeTracker.resolvePendingSplit(
              response.getPrimaryStream(),
              rangeTracker.getStartOffset(),
              rangeTracker.getStopOffset());

          return StreamPosition.newBuilder()
              .setStream(response.getPrimaryStream())
              .setOffset(splitPosition.getOffset() + 1)
              .build();

        } else {

          BigQueryStorageStreamSource<T> newSource =
              new BigQueryStorageStreamSource<>(
                  currentSource.readSession,
                  response.getRemainderStream(),
                  0,
                  StreamOffsetRangeTracker.OFFSET_INFINITY,
                  SplitDisposition.SELF,
                  null,
                  currentSource.parseFn,
                  currentSource.outputCoder,
                  currentSource.bqServices);

          synchronized (this) {
            this.currentSource = newSource;
          }

          rangeTracker.resolvePendingSplit(
              response.getRemainderStream(), 0, StreamOffsetRangeTracker.OFFSET_INFINITY);

          return rangeTracker.getStartPosition();
        }

      } else {

        //
        // The split position is contained within the remainder stream, so the primary source is the
        // parent stream ID with the offset range [initial offset, split position + 1) -- +1 since
        // the upper bound of the offset range is exclusive -- and the residual source is the
        // remainder stream from offset [split position - primary stream length + 1, infinity).
        //
        // N.B. This means that the next call to start() or advance() on the primary reader will
        //      return false.
        //
        // We're in a "split pending" state, so any attempt to return a record, split the range
        // tracker at an offset, or mark a server-side split as pending has been rejected. This
        // also means that the current source has not been modified, and can be updated with proper
        // synchronization.
        //

        if (disposition == SplitDisposition.PRIMARY) {

          BigQueryStorageStreamSource<T> newSource =
              new BigQueryStorageStreamSource<>(
                  currentSource.readSession,
                  currentSource.stream,
                  currentSource.startOffset,
                  splitPosition.getOffset() + 1,
                  SplitDisposition.SELF,
                  null,
                  currentSource.parseFn,
                  currentSource.outputCoder,
                  currentSource.bqServices);

          synchronized (this) {
            this.currentSource = newSource;
          }

          rangeTracker.resolvePendingSplit(
              splitPosition.getStream(),
              rangeTracker.getStartOffset(),
              splitPosition.getOffset() + 1);

          return rangeTracker.getStopPosition();

        } else {

          BigQueryStorageStreamSource<T> newSource =
              new BigQueryStorageStreamSource<>(
                  currentSource.readSession,
                  response.getRemainderStream(),
                  splitPosition.getOffset() - response.getPrimaryStream().getRowCount() + 1,
                  StreamOffsetRangeTracker.OFFSET_INFINITY,
                  SplitDisposition.SELF,
                  null,
                  currentSource.parseFn,
                  currentSource.outputCoder,
                  currentSource.bqServices);

          synchronized (this) {
            this.currentSource = newSource;
          }

          rangeTracker.resolvePendingSplit(
              response.getRemainderStream(),
              splitPosition.getOffset() - response.getPrimaryStream().getRowCount() + 1,
              StreamOffsetRangeTracker.OFFSET_INFINITY);

          return rangeTracker.getStartPosition();
        }
      }
    }

    private boolean readNextRow() {
      while (rowIterator == null || !rowIterator.hasNext()) {
        if (!responseIterator.hasNext()) {
          responseStream = null;
          return false;
        }

        ReadRowsResponse nextResponse = responseIterator.next();
        rowIterator = nextResponse.getRowsList().iterator();
        if (nextResponse.hasStatus()) {
          estimatedStreamLength = nextResponse.getStatus().getTotalEstimatedRows();
        }
      }

      Row nextRow = rowIterator.next();
      currentRecord = parseFn.apply(new SchemaAndRowProto(schema, nextRow));
      return true;
    }

    @Override
    public T getCurrent() {
      return currentRecord;
    }

    @Override
    public void close() {
      if (responseStream != null) {
        responseStream.cancel();
      }
      client.close();
    }

    @Override
    public Double getFractionConsumed() {
      return rangeTracker.getOrEstimateFractionConsumed(estimatedStreamLength);
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return currentSource;
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> splitAtFraction(double fraction) {
      if (currentSource.splitDisposition != SplitDisposition.SELF) {
        return null;
      }

      if (currentSource.stopOffset == StreamOffsetRangeTracker.OFFSET_INFINITY) {
        StreamPosition currentPosition = rangeTracker.getPositionForLastRecordStart();
        BigQueryStorageStreamSource<T> primary =
            currentSource.forChildStream(currentPosition, SplitDisposition.PRIMARY);
        BigQueryStorageStreamSource<T> residual =
            currentSource.forChildStream(currentPosition, SplitDisposition.RESIDUAL);
        if (!rangeTracker.tryMarkPendingSplit(currentPosition)) {
          return null;
        }
        currentSource = primary;
        return residual;
      } else {
        StreamPosition splitPosition = rangeTracker.getPositionForFractionConsumed(fraction);
        BigQueryStorageStreamSource<T> primary =
            currentSource.forSubrange(currentSource.startOffset, splitPosition.getOffset());
        BigQueryStorageStreamSource<T> residual =
            currentSource.forSubrange(splitPosition.getOffset(), currentSource.stopOffset);
        if (!rangeTracker.trySplitAtPosition(splitPosition)) {
          return null;
        }
        currentSource = primary;
        return residual;
      }
    }
  }
}
