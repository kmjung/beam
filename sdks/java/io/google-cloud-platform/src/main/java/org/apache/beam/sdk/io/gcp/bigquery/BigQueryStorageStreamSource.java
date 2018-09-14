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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental(Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

  public enum SplitDisposition {
    SELF,
    PRIMARY,
    RESIDUAL,
  }

  static <T> BigQueryStorageStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSource<>(
        readSession, stream, SplitDisposition.SELF, null, 0, Long.MAX_VALUE, parseFn, outputCoder,
        bqServices);
  }

  private final ReadSession readSession;
  private final Stream stream;
  private final SplitDisposition disposition;
  @Nullable private final StreamPosition splitPosition;
  private final long startOffset;
  private final long endOffset;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  @VisibleForTesting
  BigQueryStorageStreamSource(
      ReadSession readSession,
      Stream stream,
      SplitDisposition disposition,
      StreamPosition splitPosition,
      long startOffset,
      long endOffset,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.readSession = checkNotNull(readSession, "readSession");
    this.stream = checkNotNull(stream, "stream");
    this.disposition = checkNotNull(disposition, "disposition");
    this.splitPosition = splitPosition;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getEndOffset() {
    return endOffset;
  }

  @Override
  public void validate() {

    checkState(
        (disposition == SplitDisposition.SELF) ? splitPosition == null : splitPosition != null,
        "A split position cannot be specified when the split disposition is SELF "
            + "and must be specified otherwise");

    checkState(startOffset >= 0, "startOffset must be non-negative");
    checkState(endOffset >= 0, "endOffset must be non-negative");
    checkState(startOffset <= endOffset, "startOffset must not be greater than endOffset");
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    // TODO(kmj): Populate display data.
  }

  @Override
  public List<? extends BigQueryStorageStreamSource<T>> split(
      long desiredBundleSize, PipelineOptions options) throws Exception {
    // A stream source can't be split before reading due to server-side liquid sharding.
    return ImmutableList.of(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    // The estimated size of a stream source can't be estimated due to server-side liquid sharding.
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
    private final BigQueryStorageClient client;
    private final StreamOffsetRangeTracker rangeTracker;

    @GuardedBy("this")
    private BigQueryStorageStreamSource<T> currentSource;

    // This variable is written from within start() or advance() and may be read from
    // getFractionConsumed(). The "volatile" keyword provides guarantees against read tearing.
    private volatile long estimatedStreamLength = Long.MAX_VALUE;

    // These variables may not be accessed from within splitAtFraction(), getFractionConsumed(),
    // getCurrentSource(), getSplitPointsConsumed(), or getSplitPointsRemaining().
    private StreamPosition currentPosition;
    private ServerStream<ReadRowsResponse> responseStream;
    private Iterator<ReadRowsResponse> responseIterator;
    private Iterator<Row> rowIterator;
    private T currentRecord;

    private BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, BigQueryOptions options) throws IOException {
      this.readSession = source.readSession;
      this.parseFn = source.parseFn;
      this.outputCoder = source.outputCoder;
      this.bqServices = source.bqServices;
      this.client = bqServices.newStorageClient(options);
      this.rangeTracker =
          new StreamOffsetRangeTracker(source.stream, source.startOffset, source.endOffset,
              source.disposition != SplitDisposition.SELF);

      this.currentSource = source;
    }

    public boolean start() throws IOException {
      return startImpl() && rangeTracker.tryReturnRecordAt(true, currentPosition)
          || rangeTracker.markDone();
    }

    private boolean startImpl() {
      BigQueryStorageStreamSource<T> source = getCurrentSource();
      LOG.debug("Starting reader {} for source {}", this, source);
      if (source.disposition == SplitDisposition.SELF) {
        currentPosition = StreamPosition.newBuilder(rangeTracker.getStartPosition()).build();
      } else {
        processSplit(source.splitPosition, source.disposition);
      }

      ReadRowsRequest readRowsRequest =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseStream = client.readRowsCallable().call(readRowsRequest);
      responseIterator = responseStream.iterator();
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
          return true;
        }

        // At this point, we have attempted to return a record in the context of the current stream
        // and were rejected by the range tracker because a split is pending. Perform the split
        // call, resolve the pending split, and open a new stream.

        if (responseStream != null) {
          responseStream.cancel();
          responseStream = null;
        }

        StreamPosition splitPosition =
            StreamPosition.newBuilder(currentPosition).setOffset(currentPosition.getOffset() - 1)
                .build();

        processSplit(splitPosition, SplitDisposition.PRIMARY);

        ReadRowsRequest readRowsRequest =
            ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
        responseStream = client.readRowsCallable().call(readRowsRequest);
        responseIterator = responseStream.iterator();
      }
    }

    private void processSplit(StreamPosition splitPosition, SplitDisposition disposition) {
      checkArgument(disposition != SplitDisposition.SELF, "disposition");

      SplitReadStreamRequest splitRequest =
          SplitReadStreamRequest.newBuilder().setOriginalStream(splitPosition.getStream()).build();
      SplitReadStreamResponse splitResponse = client.splitReadStream(splitRequest);
      if (splitResponse.getPrimaryStream().getRowCount() > splitPosition.getOffset()) {

        //
        // The split position is contained within the primary stream, so the primary source is the
        // primary stream ID with the same offset range as the parent source and the residual source
        // is the entirety of the remainder stream.
        //

        if (disposition == SplitDisposition.PRIMARY) {
          rangeTracker.resolvePendingSplit(
              splitResponse.getPrimaryStream(), rangeTracker.getStartOffset(),
              rangeTracker.getStopOffset());
          currentPosition =
              StreamPosition.newBuilder().setStream(splitResponse.getPrimaryStream())
                  .setOffset(splitPosition.getOffset() + 1).build();
        } else {
          rangeTracker.resolvePendingSplit(splitResponse.getRemainderStream(), 0, Long.MAX_VALUE);
          currentPosition = StreamPosition.newBuilder(rangeTracker.getStartPosition()).build();
        }

      } else {

        //
        // The split position is contained within the remainder stream, so the primary source is the
        // parent stream ID with offset range [initial offset, split position + 1) -- +1 since the
        // upper bound of the offset range isn't inclusive -- and the residual source is the
        // remainder stream from offset (split position - primary stream length + 1) to the end.
        //
        // N.B. This means that the next attempt to call start() or advance() on the primary stream
        //      will result in a result of false.
        //

        if (disposition == SplitDisposition.PRIMARY) {
          rangeTracker.resolvePendingSplit(
              splitPosition.getStream(), rangeTracker.getStartOffset(),
              splitPosition.getOffset() + 1);
          currentPosition = StreamPosition.newBuilder(rangeTracker.getStopPosition()).build();
        } else {
          rangeTracker.resolvePendingSplit(
              splitResponse.getRemainderStream(),
              splitPosition.getOffset() - splitResponse.getPrimaryStream().getRowCount() + 1,
              Long.MAX_VALUE);
          currentPosition = StreamPosition.newBuilder(rangeTracker.getStartPosition()).build();
        }
      }

      ReadRowsRequest readRowsRequest =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseStream = client.readRowsCallable().call(readRowsRequest);
      responseIterator = responseStream.iterator();
      estimatedStreamLength = Long.MAX_VALUE;
      rowIterator = null;
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
      currentRecord =
          parseFn.apply(new SchemaAndRowProto(readSession.getProjectedSchema(), currentRow));
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
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return currentSource;
    }

    @Override
    public Double getFractionConsumed() {
      return rangeTracker.getOrEstimateFractionConsumed(estimatedStreamLength);
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> splitAtFraction(double fraction) {
      LOG.debug("Proposing to split range tracker {} at fraction {}", rangeTracker, fraction);
      if (rangeTracker.getStopPosition().getOffset() == Long.MAX_VALUE) {
        // TODO(kmj): Is this a race condition?
        StreamPosition splitPosition = rangeTracker.getPositionForLastRecordStart();
        BigQueryStorageStreamSource<T> primary = new BigQueryStorageStreamSource<>(
            readSession, splitPosition.getStream(), SplitDisposition.PRIMARY, splitPosition,
            rangeTracker.getStartOffset(), rangeTracker.getStopOffset(), parseFn, outputCoder,
            bqServices);
        BigQueryStorageStreamSource<T> residual = new BigQueryStorageStreamSource<>(
            readSession, splitPosition.getStream(), SplitDisposition.RESIDUAL, splitPosition,
            rangeTracker.getStartOffset(), rangeTracker.getStopOffset(), parseFn, outputCoder,
            bqServices);
        if (!rangeTracker.tryMarkPendingSplit(splitPosition)) {
          return null;
        }
        this.currentSource = primary;
        return residual;
      } else {
        StreamPosition splitPosition = rangeTracker.getPositionForFractionConsumed(fraction);
        BigQueryStorageStreamSource<T> primary = new BigQueryStorageStreamSource<>(
            readSession, splitPosition.getStream(), SplitDisposition.SELF, null,
            rangeTracker.getStartOffset(), splitPosition.getOffset(), parseFn, outputCoder,
            bqServices);
        BigQueryStorageStreamSource<T> residual = new BigQueryStorageStreamSource<>(
            readSession, splitPosition.getStream(), SplitDisposition.SELF, null,
            splitPosition.getOffset(), rangeTracker.getStopOffset(), parseFn, outputCoder,
            bqServices);
        if (!rangeTracker.trySplitAtPosition(splitPosition)) {
          return null;
        }
        this.currentSource = primary;
        return residual;
      }
    }
  }
}

// /**
//  * A {@link BoundedSource} to read from existing read streams using the BigQuery parallel read API.
//  */
// @Experimental(Experimental.Kind.SOURCE_SINK)
// public class BigQueryStorageStreamSource<T> extends OffsetBasedSource<T> {
//
//   static <T> BigQueryStorageStreamSource<T> create(
//       ReadSession readSession,
//       Stream stream,
//       SerializableFunction<SchemaAndRowProto, T> parseFn,
//       Coder<T> outputCoder,
//       BigQueryServices bqServices) {
//     return new BigQueryStorageStreamSource<>(
//         readSession, stream, 0, OffsetRangeTracker.OFFSET_INFINITY, parseFn, outputCoder,
//         bqServices);
//   }
//
//   private final ReadSession readSession;
//   private final Stream stream;
//   private final SerializableFunction<SchemaAndRowProto, T> parseFn;
//   private final Coder<T> outputCoder;
//   private final BigQueryServices bqServices;
//
//   @VisibleForTesting
//   BigQueryStorageStreamSource(
//       ReadSession readSession,
//       Stream stream,
//       long startOffset,
//       long endOffset,
//       SerializableFunction<SchemaAndRowProto, T> parseFn,
//       Coder<T> outputCoder,
//       BigQueryServices bqServices) {
//     super(startOffset, endOffset, 0);
//     this.readSession = checkNotNull(readSession, "readSession");
//     this.stream = checkNotNull(stream, "stream");
//     this.parseFn = checkNotNull(parseFn, "parseFn");
//     this.outputCoder = checkNotNull(outputCoder, "outputCoder");
//     this.bqServices = checkNotNull(bqServices, "bqServices");
//   }
//
//   private ReadSession getReadSession() {
//     return readSession;
//   }
//
//   private Stream getStream() {
//     return stream;
//   }
//
//   private SerializableFunction<SchemaAndRowProto, T> getParseFn() {
//     return parseFn;
//   }
//
//   private BigQueryServices getBigQueryServices() {
//     return bqServices;
//   }
//
//   @Override
//   public String toString() {
//     return readSession.getName() + ":" + stream.getName() + super.toString();
//   }
//
//   @Override
//   public long getMaxEndOffset(PipelineOptions options) {
//     return Long.MAX_VALUE;
//   }
//
//   @Override
//   public Coder<T> getOutputCoder() {
//     return outputCoder;
//   }
//
//   @Override
//   public List<OffsetBasedSource<T>> split(
//       long desiredBundleSizeBytes, PipelineOptions options) {
//     // Stream source objects can't be split further due to liquid sharding.
//     return ImmutableList.of(this);
//   }
//
//   @Override
//   public OffsetBasedSource<T> createSourceForSubrange(long startOffset, long endOffset) {
//     // This method is (currently) unreachable.
//     return null;
//   }
//
//   /**
//    * A {@code Block} represents a block of records that can be read.
//    *
//    * <p>This interface is taken directly from {@link org.apache.beam.sdk.io.BlockBasedSource}, but
//    * we can't use that type directly here since we don't subclass BlockBasedSource.
//    */
//   @Experimental(Kind.SOURCE_SINK)
//   protected abstract static class Block<T> {
//     /**
//      * Returns the current record.
//      */
//     public abstract T getCurrentRecord();
//
//     /**
//      * Reads the next record from the block and returns true iff one exists.
//      */
//     public abstract boolean readNextRecord() throws IOException;
//
//     /**
//      * Returns the fraction of the block already consumed, if possible, as a value in
//      * {@code [0, 1]}. The value should not include the current record. Successive results from this
//      * method must be monotonically increasing.
//      *
//      * <p>If it is not possible to compute the fraction of the block consumed -- for example, if the
//      * total number of records in the block is unknown -- then this method may return zero.
//      */
//     public abstract double getFractionOfBlockConsumed();
//   }
//
//   @Experimental(Kind.SOURCE_SINK)
//   static class RowProtoBlock<T> extends Block<T> {
//     // The schema for the read session.
//     private final StructType schema;
//
//     // The parse function to create output type objects.
//     private final SerializableFunction<SchemaAndRowProto, T> parseFn;
//
//     // The data to process.
//     private final List<Row> data;
//
//     // The current record in the block. Initialize in readNextRecord.
//     @Nullable private T currentRecord;
//
//     // The index of the current record in the block.
//     private int currentRecordIndex = 0;
//
//     RowProtoBlock(
//         StructType schema, SerializableFunction<SchemaAndRowProto, T> parseFn, List<Row> data) {
//       this.schema = checkNotNull(schema, "schema");
//       this.parseFn = checkNotNull(parseFn, "parseFn");
//       this.data = checkNotNull(data, "data");
//     }
//
//     @Override
//     public T getCurrentRecord() {
//       return currentRecord;
//     }
//
//     @Override
//     public boolean readNextRecord() {
//       if (currentRecordIndex >= data.size()) {
//         return false;
//       }
//       Row currentRow = data.get(currentRecordIndex);
//       currentRecord = parseFn.apply(new SchemaAndRowProto(schema, currentRow));
//       currentRecordIndex++;
//       return true;
//     }
//
//     @Override
//     public double getFractionOfBlockConsumed() {
//       return ((double) currentRecordIndex) / data.size();
//     }
//   }
//
//   /**
//    * An {@link OffsetBasedReader} which reads from a {@link ServerStream}.
//    *
//    * <p>This class is analogous to {@link org.apache.beam.sdk.io.FileBasedSource.FileBasedReader},
//    * but it uses a server stream instead of a file. It might be possible to combine the
//    * implementations refactoring -- can the server stream be represented as a
//    * {@link java.nio.channels.ReadableByteChannel}, maybe? -- but I don't know if this is desirable.
//    */
//   abstract static class ServerStreamBasedReader<ResponseT, OutputT>
//       extends OffsetBasedReader<OutputT> {
//     // Initialized in startImpl.
//     ServerStream<ResponseT> stream;
//
//     protected ServerStreamBasedReader(OffsetBasedSource<OutputT> source) {
//       super(source);
//     }
//
//     @Override
//     protected final boolean startImpl() throws IOException {
//       initializeServerStream();
//       return advanceImpl();
//     }
//
//     @Override
//     protected final boolean advanceImpl() throws IOException {
//       return readNextRecord();
//     }
//
//     @Override
//     public void close() {
//       if (stream != null) {
//         stream.cancel();
//       }
//     }
//
//     @Override
//     public boolean allowsDynamicSplitting() {
//       // Don't support dynamic splitting (for now).
//       return false;
//     }
//
//     /**
//      * Initializes the server stream associated with the current reader.
//      */
//     protected abstract ServerStream<ResponseT> initializeServerStream() throws IOException;
//
//     /**
//      * Reads the next record from the server stream provided by {@link #initializeServerStream()}.
//      * Methods {@link #getCurrent()}, {@link #getCurrentOffset()}, and {@link #isAtSplitPoint()}
//      * should return the corresponding information abou tthe record read by the last invocation of
//      * this method.
//      */
//     protected abstract boolean readNextRecord() throws IOException;
//   }
//
//   /**
//    * A {@link ServerStreamBasedReader} which processes response messages containing many individual
//    * rows, possibly requiring decoding as a chunk.
//    *
//    * <p>This interface is taken directly from {@link org.apache.beam.sdk.io.BlockBasedSource}, but
//    * we can't use that type directly here since we don't subclass BlockBasedSource.
//    */
//   @Experimental(Kind.SOURCE_SINK)
//   abstract static class BlockBasedReader<ResponseT, OutputT>
//       extends ServerStreamBasedReader<ResponseT, OutputT> {
//     private boolean atSplitPoint;
//
//     protected BlockBasedReader(OffsetBasedSource<OutputT> source) {
//       super(source);
//     }
//
//     @Override
//     public final OutputT getCurrent() throws NoSuchElementException {
//       Block<OutputT> currentBlock = getCurrentBlock();
//       if (currentBlock == null) {
//         throw new NoSuchElementException(
//             "No block has been successfully read from " + getCurrentSource());
//       }
//       return currentBlock.getCurrentRecord();
//     }
//
//     @Override
//     protected boolean isAtSplitPoint() {
//       return atSplitPoint;
//     }
//
//     @Override
//     protected final boolean readNextRecord() throws IOException {
//       atSplitPoint = false;
//       while (getCurrentBlock() == null || !getCurrentBlock().readNextRecord()) {
//         if (!readNextBlock()) {
//           return false;
//         }
//         // The first record in a block is a split point.
//         atSplitPoint = true;
//       }
//       return true;
//     }
//
//     @Override
//     @Nullable
//     public Double getFractionConsumed() {
//       if (!isStarted()) {
//         return 0.0;
//       }
//       if (isDone()) {
//         return 1.0;
//       }
//       OffsetBasedSource<OutputT> source = getCurrentSource();
//       if (source.getEndOffset() == Long.MAX_VALUE) {
//         return null;
//       }
//
//       long currentBlockOffset = getCurrentBlockOffset();
//       long startOffset = source.getStartOffset();
//       long endOffset = source.getEndOffset();
//       double fractionAtBlockStart =
//           ((double) (currentBlockOffset - startOffset)) / (endOffset - startOffset);
//       double fractionAtBlockEnd =
//           ((double) (currentBlockOffset + getCurrentBlockSize() - startOffset)
//               / (endOffset - startOffset));
//       double blockFraction = getCurrentBlock().getFractionOfBlockConsumed();
//       return Math.min(
//           1.0,
//           fractionAtBlockStart + blockFraction * (fractionAtBlockEnd - fractionAtBlockStart));
//     }
//
//     @Override
//     protected long getCurrentOffset() {
//       return getCurrentBlockOffset();
//     }
//
//     /**
//      * Read the next block from the input.
//      */
//     public abstract boolean readNextBlock() throws IOException;
//
//     /**
//      * Returns the current block (the block that was read by the last successful call to
//      * {@link #readNextBlock()}. May return null initially, or if no block has been successfully
//      * read.
//      */
//     @Nullable
//     public abstract Block<OutputT> getCurrentBlock();
//
//     /**
//      * Returns the size of the current block in bytes. May return {@code 0} if the size of the
//      * current block is unknown.
//      */
//     public abstract long getCurrentBlockSize();
//
//     /**
//      * Returns the largest offset such that starting to read from that offset includes the current
//      * block.
//      */
//     public abstract long getCurrentBlockOffset();
//   }
//
//   @Override
//   public OffsetBasedReader<T> createReader(PipelineOptions options) {
//     return new BigQueryStorageStreamReader<>(this, options);
//   }
//
//   /**
//    * A {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader} to read from a BigQuery storage
//    * stream source.
//    */
//   @Experimental(Kind.SOURCE_SINK)
//   static class BigQueryStorageStreamReader<T> extends BlockBasedReader<ReadRowsResponse, T> {
//     private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamReader.class);
//
//     // The schema for the source (e.g. for the underlying read session).
//     private final StructType schema;
//
//     // The parse function for the source.
//     private final SerializableFunction<SchemaAndRowProto, T> parseFn;
//
//     // BigQuery services for the source.
//     private final BigQueryServices bqServices;
//
//     // BigQuery options for the source.
//     private final BigQueryOptions bqOptions;
//
//     // An iterator over the server stream responses.
//     private Iterator<ReadRowsResponse> responseIterator;
//
//     // The current block. Initialized in readNextRecord.
//     @Nullable private RowProtoBlock<T> currentBlock;
//
//     // A lock used to synchronize block offsets.
//     private final Object progressLock = new Object();
//
//     // Offset of the current block.
//     @GuardedBy("progressLock")
//     private long currentBlockOffset = 0;
//
//     // Size of the current block.
//     @GuardedBy("progressLock")
//     private long currentBlockSizeBytes = 0;
//
//     // Estimated length of the current stream, as provided by the server.
//     private long estimatedStreamLength = Long.MAX_VALUE;
//
//     public BigQueryStorageStreamReader(
//         BigQueryStorageStreamSource<T> source, PipelineOptions options) {
//       super(source);
//       this.schema = source.getReadSession().getProjectedSchema();
//       this.parseFn = source.getParseFn();
//       this.bqServices = source.getBigQueryServices();
//       this.bqOptions = options.as(BigQueryOptions.class);
//     }
//
//     @Override
//     public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
//       return (BigQueryStorageStreamSource<T>) super.getCurrentSource();
//     }
//
//     @Override
//     public ServerStream<ReadRowsResponse> initializeServerStream() throws IOException {
//       BigQueryStorageStreamSource<T> source = getCurrentSource();
//       LOG.info("Initializing a server stream for source " + source);
//
//       ReadRowsRequest request = ReadRowsRequest.newBuilder()
//           .setReadPosition(StreamPosition.newBuilder()
//               .setStream(source.getStream())
//               .setOffset(source.getStartOffset()))
//           .build();
//
//       ServerStream<ReadRowsResponse> stream =
//           bqServices.newStorageClient(bqOptions).readRowsCallable().call(request);
//       responseIterator = stream.iterator();
//
//       synchronized (progressLock) {
//         currentBlockOffset = source.getStartOffset();
//       }
//
//       return stream;
//     }
//
//     @Override
//     public boolean readNextBlock() {
//       long startOfNextBlock;
//       synchronized (progressLock) {
//         startOfNextBlock = currentBlockOffset + currentBlockSizeBytes;
//       }
//
//       if (!responseIterator.hasNext()) {
//         return false;
//       }
//
//       ReadRowsResponse nextResponse = responseIterator.next();
//       currentBlock = new RowProtoBlock<>(schema, parseFn, nextResponse.getRowsList());
//       if (nextResponse.hasStatus()) {
//         estimatedStreamLength = nextResponse.getStatus().getTotalEstimatedRows();
//       }
//
//       synchronized (progressLock) {
//         currentBlockOffset = startOfNextBlock;
//         currentBlockSizeBytes = nextResponse.getRowsCount();
//       }
//
//       return true;
//     }
//
//     @Override
//     public RowProtoBlock<T> getCurrentBlock() {
//       return currentBlock;
//     }
//
//     @Override
//     public long getCurrentBlockOffset() {
//       synchronized (progressLock) {
//         return currentBlockOffset;
//       }
//     }
//
//     @Override
//     public long getCurrentBlockSize() {
//       synchronized (progressLock) {
//         return currentBlockSizeBytes;
//       }
//     }
//
//     @Override
//     @Nullable
//     public Double getFractionConsumed() {
//       if (!isStarted()) {
//         return 0.0;
//       }
//
//       if (isDone()) {
//         return 1.0;
//       }
//
//       BigQueryStorageStreamSource<T> source = getCurrentSource();
//       long endOffset = source.getEndOffset();
//       if (endOffset != Long.MAX_VALUE) {
//         // The actual end offset of the stream is known; compute the fraction consumed using the
//         // actual offset.
//         return computeFractionConsumed(source.getStartOffset(), endOffset);
//       }
//
//       endOffset = estimatedStreamLength;
//       if (endOffset != Long.MAX_VALUE) {
//         // The actual end offset of the stream is not known, but the server has provided us with a
//         // rough estimate. Compute the fraction consumed using the estimated length.
//         return computeFractionConsumed(source.getStartOffset(), endOffset);
//       }
//
//       return null;
//     }
//
//     private double computeFractionConsumed(long startOffset, long endOffset) {
//       long currentBlockOffset = getCurrentBlockOffset();
//       double fractionAtBlockStart =
//           ((double) (currentBlockOffset - startOffset)) / (endOffset - startOffset);
//       double fractionAtBlockEnd =
//           ((double) (currentBlockOffset + getCurrentBlockSize() - startOffset)
//               / (endOffset - startOffset));
//       double blockFraction = getCurrentBlock().getFractionOfBlockConsumed();
//       return Math.min(
//           1.0,
//           fractionAtBlockStart + blockFraction * (fractionAtBlockEnd - fractionAtBlockStart));
//     }
//   }
// }
