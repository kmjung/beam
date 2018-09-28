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

import com.google.cloud.bigquery.storage.v1alpha1.Storage;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.TableReadService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.ItemSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Do not use in pipelines directly. Most users should use {@link BigQueryIO} instead.
 *
 * <p>A {@link BoundedSource} for reading from BigQuery storage read streams.
 *
 * TODO(kmj): Add more comments here.
 *
 * @param <T> The type of records returned by this source.
 */
@Experimental(Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

  /**
   * Represents the split disposition of the current source.
   */
  public enum SplitDisposition {
    /**
     * The current source represents the specified stream.
     */
    SELF,

    /**
     * The current source represents the primary child of the specified stream.
     */
    PRIMARY,

    /**
     * The current source represents the residual child of the specified stream.
     */
    RESIDUAL,
  }

  public static <T> BigQueryStorageStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSource<>(
        readSession,
        stream,
        SplitDisposition.SELF,
        null,
        0,
        Long.MAX_VALUE,
        parseFn,
        outputCoder,
        bqServices);
  }

  private final ReadSession readSession;
  private final Stream stream;
  private final SplitDisposition splitDisposition;
  @Nullable
  private final StreamPosition splitPosition;
  private final long startOffset;
  private final long stopOffset;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  @VisibleForTesting
  BigQueryStorageStreamSource(
      ReadSession readSession,
      Stream stream,
      SplitDisposition splitDisposition,
      StreamPosition splitPosition,
      long startOffset,
      long stopOffset,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.readSession = checkNotNull(readSession, "readSession");
    this.stream = checkNotNull(stream, "stream");
    this.splitDisposition = checkNotNull(splitDisposition, "splitDisposition");
    this.splitPosition = splitPosition;
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
  }

  @Override
  public void validate() {
    checkState(
        startOffset >= 0,
        "Start offset must be non-negative");

    checkState(
        stopOffset >= 0,
        "Stop offset must be non-negative");

    checkState(
        startOffset <= stopOffset,
        "Start offset may not be greater than stop offset");

    checkState(
        (splitDisposition == SplitDisposition.SELF)
            ? splitPosition == null : splitPosition != null,
        "A split position cannot be specified when the split disposition is SELF "
            + "and must be specified otherwise");
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
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

  public static class BigQueryStorageStreamReader<T> extends BoundedSource.BoundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamReader.class);

    private final ReadSession readSession;
    private final SerializableFunction<SchemaAndRowProto, T> parseFn;
    private final Coder<T> outputCoder;
    private final BigQueryServices bqServices;
    private final TableReadService tableReadService;
    private final StreamOffsetRangeTracker rangeTracker;

    @GuardedBy("this")
    private BigQueryStorageStreamSource<T> currentSource;

    // This variable is written from within start() or advance() and may be read from
    // getFractionConsumed(). The "volatile" keyword provides guarantees against read and write
    // tearing.
    private volatile long estimatedStreamLength = Long.MAX_VALUE;

    // These variables may not be accessed from within splitAtFraction(), getFractionConsumed(),
    // getCurrentSource(), getSplitPointsConsumed(), or getSplitPointsRemaining().
    private StreamPosition currentPosition;
    private Iterator<ReadRowsResponse> responseIterator;
    private Iterator<Row> rowIterator;
    private T currentRecord;

    private BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, BigQueryOptions options) throws IOException {
      this.readSession = source.readSession;
      this.parseFn = source.parseFn;
      this.outputCoder = source.outputCoder;
      this.bqServices = source.bqServices;
      this.tableReadService = bqServices.getTableReadService(options);
      this.rangeTracker =
          new StreamOffsetRangeTracker(
              source.stream, source.startOffset, source.stopOffset,
              (source.splitDisposition != SplitDisposition.SELF));
      this.currentSource = source;
    }

    @Override
    public boolean start() throws IOException {
      return startImpl() && rangeTracker.tryReturnRecordAt(true, currentPosition)
          || rangeTracker.markDone();
    }

    private boolean startImpl() throws IOException {
      BigQueryStorageStreamSource<T> source = getCurrentSource();
      LOG.info("Starting reader {} from source {}", this, source);
      if (source.splitDisposition == SplitDisposition.SELF) {
        currentPosition = rangeTracker.getStartPosition();
      } else {
        processSplit(source.splitPosition, source.splitDisposition);
      }

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseIterator = tableReadService.readRows(request);
      rowIterator = null;
      return advanceImpl();
    }

    @Override
    public boolean advance() throws IOException {
      while (true) {
        if (!advanceImpl()) {
          rangeTracker.markDone();
          return false;
        }

        currentPosition =
            currentPosition.toBuilder().setOffset(currentPosition.getOffset() + 1).build();

        if (rangeTracker.tryReturnRecordAt(true, currentPosition)) {
          return true;
        }

        if (rangeTracker.isDone()) {
          return false;
        }

        // At this point, we've attempted to return a record in the context of the current stream
        // and were rejected by the range tracker because a split is pending. Perform the split
        // call, resolve the pending split, and open a new stream.

        StreamPosition splitPosition =
            StreamPosition.newBuilder().setStream(currentPosition.getStream())
                .setOffset(currentPosition.getOffset() - 1).build();

        processSplit(splitPosition, SplitDisposition.PRIMARY);

        ReadRowsRequest request =
            ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
        responseIterator = tableReadService.readRows(request);
        rowIterator = null;
      }
    }

    private boolean advanceImpl() {
      while (rowIterator == null || !rowIterator.hasNext()) {
        if (!responseIterator.hasNext()) {
          return false;
        }

        ReadRowsResponse nextResponse = responseIterator.next();
        rowIterator = nextResponse.getRowsList().iterator();
        if (nextResponse.hasStatus()) {
          estimatedStreamLength = nextResponse.getStatus().getTotalEstimatedRows();
        }
      }

      Row currentRow = rowIterator.next();
      currentRecord = parseFn.apply(
          new SchemaAndRowProto(readSession.getProjectedSchema(), currentRow));
      return true;
    }

    private void processSplit(StreamPosition splitPosition, SplitDisposition splitDisposition) {
      checkArgument(splitDisposition != SplitDisposition.SELF);

      SplitReadStreamRequest splitRequest =
          SplitReadStreamRequest.newBuilder().setOriginalStream(splitPosition.getStream()).build();
      SplitReadStreamResponse splitResponse = tableReadService.splitReadStream(splitRequest);
      if (splitResponse.getPrimaryStream().getRowCount() > splitPosition.getOffset()) {

        //
        // The split position is contained within the primary stream, so the primary source
        // represents the original unbounded offset range in the primary child stream, and the
        // residual source represents the entirety of the residual child stream.
        //

        if (splitDisposition == SplitDisposition.PRIMARY) {
          rangeTracker.resolvePendingSplit(
              splitResponse.getPrimaryStream(), rangeTracker.getStartOffset(),
              rangeTracker.getStopOffset());
          currentPosition =
              StreamPosition.newBuilder().setStream(splitResponse.getPrimaryStream())
                  .setOffset(splitPosition.getOffset() + 1).build();
        } else {
          rangeTracker.resolvePendingSplit(splitResponse.getRemainderStream(), 0, Long.MAX_VALUE);
          currentPosition = rangeTracker.getStartPosition();
        }

      } else {

        //
        // The split position is contained within the residual stream, so the primary source
        // represents the bounded offset range from [start offset, split offset + 1) within the
        // original stream, and the residual source represents the unbounded offset range starting
        // at offset [split offset - primary stream length + 1] within the residual child stream.
        //
        // N.B. This means that the next call to start() or advance() on the primary stream will
        //      return false.
        //

        if (splitDisposition == SplitDisposition.PRIMARY) {
          rangeTracker.resolvePendingSplit(
              splitPosition.getStream(), rangeTracker.getStartOffset(),
              splitPosition.getOffset() + 1);
          currentPosition = rangeTracker.getStopPosition();
        } else {
          rangeTracker.resolvePendingSplit(
              splitResponse.getRemainderStream(),
              splitPosition.getOffset() - splitResponse.getPrimaryStream().getRowCount() + 1,
              Long.MAX_VALUE);
          currentPosition = rangeTracker.getStartPosition();
        }
      }

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseIterator = tableReadService.readRows(request);
      estimatedStreamLength = Long.MAX_VALUE;
      rowIterator = null;
    }

    @Override
    public T getCurrent() {
      return currentRecord;
    }

    @Override
    public void close() {
      // TODO(kmj): Do nothing for now. Eventually, close the read stream if still open.
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
      
    }
  }
}

// /**
//  * A {@link BoundedSource} to read from existing read streams using the BigQuery parallel read API.
//  */
// @Experimental(Experimental.Kind.SOURCE_SINK)
// public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {
//
//   private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamSource.class);
//
//   static <T> BigQueryStorageStreamSource<T> create(
//       BigQueryServices bqServices,
//       SerializableFunction<SchemaAndRowProto, T> parseFn,
//       Coder<T> coder,
//       Storage.ReadSession readSession,
//       Storage.StreamPosition streamPosition,
//       Long readSizeBytes) {
//     return new BigQueryStorageStreamSource<>(
//         bqServices,
//         parseFn,
//         coder,
//         readSession,
//         streamPosition,
//         readSizeBytes);
//   }
//
//   private final BigQueryServices bqServices;
//   private final SerializableFunction<SchemaAndRowProto, T> parseFn;
//   private final Coder<T> coder;
//   private final Storage.ReadSession readSession;
//   private final Storage.StreamPosition streamPosition;
//   private final Long readSizeBytes;
//
//   BigQueryStorageStreamSource(
//       BigQueryServices bqServices,
//       SerializableFunction<SchemaAndRowProto, T> parseFn,
//       Coder<T> coder,
//       Storage.ReadSession readSession,
//       Storage.StreamPosition streamPosition,
//       Long readSizeBytes) {
//     this.bqServices = checkNotNull(bqServices, "bqServices");
//     this.parseFn = checkNotNull(parseFn, "parseFn");
//     this.coder = checkNotNull(coder, "coder");
//     this.readSession = checkNotNull(readSession, "readSession");
//     this.streamPosition = checkNotNull(streamPosition, "streamPosition");
//     this.readSizeBytes = readSizeBytes;
//   }
//
//   @Override
//   public List<? extends BoundedSource<T>> split(
//       long desiredBundleSizeBytes, PipelineOptions options) {
//     // Stream source objects can't (currently) be split.
//     return ImmutableList.of(this);
//   }
//
//   @Override
//   public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
//     return readSizeBytes != null ? readSizeBytes : 0;
//   }
//
//   @Override
//   public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
//     return new BigQueryStorageStreamReader<>(
//         readSession,
//         streamPosition,
//         parseFn,
//         this,
//         bqServices,
//         pipelineOptions.as(BigQueryOptions.class));
//   }
//
//   @Override
//   public Coder<T> getOutputCoder() {
//     return coder;
//   }
//
//   /**
//    * Iterates over all rows assigned to a particular reader in a read session.
//    */
//   @Experimental(Experimental.Kind.SOURCE_SINK)
//   static class BigQueryStorageStreamReader<T> extends BoundedReader<T> {
//
//     private Storage.ReadSession readSession;
//     private final BigQueryServices client;
//     private Iterator<Storage.ReadRowsResponse> responseIterator;
//     private Iterator<Row> rowIterator;
//     private final Storage.ReadRowsRequest request;
//     private SerializableFunction<SchemaAndRowProto, T> parseFn;
//     private BoundedSource<T> source;
//     private BigQueryOptions options;
//     private Row currentRow;
//
//     BigQueryStorageStreamReader(
//         Storage.ReadSession readSession,
//         Storage.StreamPosition streamPosition,
//         SerializableFunction<SchemaAndRowProto, T> parseFn,
//         BoundedSource<T> source,
//         BigQueryServices client,
//         BigQueryOptions options) {
//       this.readSession = checkNotNull(readSession, "readSession");
//       this.client = checkNotNull(client, "client");
//       this.parseFn = checkNotNull(parseFn, "parseFn");
//       this.source = checkNotNull(source, "source");
//       this.options = options;
//
//       this.request = Storage.ReadRowsRequest.newBuilder()
//           .setReadPosition(streamPosition)
//           .build();
//     }
//
//     /**
//      * Empty operation, the table is already open for read.
//      * @throws IOException on failure
//      */
//     @Override
//     public boolean start() throws IOException {
//       responseIterator = client.getTableReadService(options).readRows(request);
//       return advance();
//     }
//
//     @Override
//     public boolean advance() {
//       if (rowIterator == null || !rowIterator.hasNext()) {
//         if (!responseIterator.hasNext()) {
//           currentRow = null;
//           return false;
//         }
//         rowIterator = responseIterator.next().getRowsList().iterator();
//       }
//       currentRow = rowIterator.next();
//       return true;
//     }
//
//     @Override
//     public T getCurrent() {
//       if (currentRow == null) {
//         return null;
//       }
//       return parseFn.apply(new SchemaAndRowProto(readSession.getProjectedSchema(), currentRow));
//     }
//
//     @Override
//     public void close() {
//       // Do nothing.
//     }
//
//     @Override
//     public BoundedSource<T> getCurrentSource() {
//       return source;
//     }
//   }
// }
