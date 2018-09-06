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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.range.OffsetRangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BoundedSource} to read from existing read streams using the BigQuery parallel read API.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends OffsetBasedSource<T> {

  static <T> BigQueryStorageStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSource<>(
        readSession, stream, 0, OffsetRangeTracker.OFFSET_INFINITY, parseFn, outputCoder,
        bqServices);
  }

  private final ReadSession readSession;
  private final Stream stream;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private BigQueryStorageStreamSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long endOffset,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    super(startOffset, endOffset, 0);
    this.readSession = checkNotNull(readSession, "readSession");
    this.stream = checkNotNull(stream, "stream");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
  }

  private ReadSession getReadSession() {
    return readSession;
  }

  private Stream getStream() {
    return stream;
  }

  private SerializableFunction<SchemaAndRowProto, T> getParseFn() {
    return parseFn;
  }

  private BigQueryServices getBigQueryServices() {
    return bqServices;
  }

  @Override
  public String toString() {
    return readSession.getName() + ":" + stream.getName() + super.toString();
  }

  @Override
  public long getMaxEndOffset(PipelineOptions options) {
    return Long.MAX_VALUE;
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public List<OffsetBasedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    // Stream source objects can't be split further due to liquid sharding.
    return ImmutableList.of(this);
  }

  @Override
  public OffsetBasedSource<T> createSourceForSubrange(long startOffset, long endOffset) {
    // This method is (currently) unreachable.
    return null;
  }

  /**
   * A {@code Block} represents a block of records that can be read.
   *
   * <p>This interface is taken directly from {@link org.apache.beam.sdk.io.BlockBasedSource}, but
   * we can't use that type directly here since we don't subclass BlockBasedSource.
   */
  @Experimental(Kind.SOURCE_SINK)
  protected abstract static class Block<T> {
    /**
     * Returns the current record.
     */
    public abstract T getCurrentRecord();

    /**
     * Reads the next record from the block and returns true iff one exists.
     */
    public abstract boolean readNextRecord() throws IOException;

    /**
     * Returns the fraction of the block already consumed, if possible, as a value in
     * {@code [0, 1]}. The value should not include the current record. Successive results from this
     * method must be monotonically increasing.
     *
     * <p>If it is not possible to compute the fraction of the block consumed -- for example, if the
     * total number of records in the block is unknown -- then this method may return zero.
     */
    public abstract double getFractionOfBlockConsumed();
  }

  @Experimental(Kind.SOURCE_SINK)
  static class RowProtoBlock<T> extends Block<T> {
    // The schema for the read session.
    private final StructType schema;

    // The parse function to create output type objects.
    private final SerializableFunction<SchemaAndRowProto, T> parseFn;

    // The data to process.
    private final List<Row> data;

    // The current record in the block. Initialize in readNextRecord.
    @Nullable private T currentRecord;

    // The index of the current record in the block.
    private int currentRecordIndex = 0;

    RowProtoBlock(
        StructType schema, SerializableFunction<SchemaAndRowProto, T> parseFn, List<Row> data) {
      this.schema = checkNotNull(schema, "schema");
      this.parseFn = checkNotNull(parseFn, "parseFn");
      this.data = checkNotNull(data, "data");
    }

    @Override
    public T getCurrentRecord() {
      return currentRecord;
    }

    @Override
    public boolean readNextRecord() {
      if (currentRecordIndex >= data.size()) {
        return false;
      }
      Row currentRow = data.get(currentRecordIndex);
      currentRecord = parseFn.apply(new SchemaAndRowProto(schema, currentRow));
      currentRecordIndex++;
      return true;
    }

    @Override
    public double getFractionOfBlockConsumed() {
      return ((double) currentRecordIndex) / data.size();
    }
  }

  /**
   * An {@link OffsetBasedReader} which reads from a {@link ServerStream}.
   *
   * <p>This class is analogous to {@link org.apache.beam.sdk.io.FileBasedSource.FileBasedReader},
   * but it uses a server stream instead of a file. It might be possible to combine the
   * implementations refactoring -- can the server stream be represented as a
   * {@link java.nio.channels.ReadableByteChannel}, maybe? -- but I don't know if this is desirable.
   */
  abstract static class ServerStreamBasedReader<ResponseT, OutputT>
      extends OffsetBasedReader<OutputT> {
    // Initialized in startImpl.
    ServerStream<ResponseT> stream;

    protected ServerStreamBasedReader(OffsetBasedSource<OutputT> source) {
      super(source);
    }

    @Override
    protected final boolean startImpl() throws IOException {
      initializeServerStream();
      return advanceImpl();
    }

    @Override
    protected final boolean advanceImpl() throws IOException {
      return readNextRecord();
    }

    @Override
    public void close() {
      if (stream != null) {
        stream.cancel();
      }
    }

    @Override
    public boolean allowsDynamicSplitting() {
      // Don't support dynamic splitting (for now).
      return false;
    }

    /**
     * Initializes the server stream associated with the current reader.
     */
    protected abstract ServerStream<ResponseT> initializeServerStream() throws IOException;

    /**
     * Reads the next record from the server stream provided by {@link #initializeServerStream()}.
     * Methods {@link #getCurrent()}, {@link #getCurrentOffset()}, and {@link #isAtSplitPoint()}
     * should return the corresponding information abou tthe record read by the last invocation of
     * this method.
     */
    protected abstract boolean readNextRecord() throws IOException;
  }

  /**
   * A {@link ServerStreamBasedReader} which processes response messages containing many individual
   * rows, possibly requiring decoding as a chunk.
   *
   * <p>This interface is taken directly from {@link org.apache.beam.sdk.io.BlockBasedSource}, but
   * we can't use that type directly here since we don't subclass BlockBasedSource.
   */
  @Experimental(Kind.SOURCE_SINK)
  abstract static class BlockBasedReader<ResponseT, OutputT>
      extends ServerStreamBasedReader<ResponseT, OutputT> {
    private boolean atSplitPoint;

    protected BlockBasedReader(OffsetBasedSource<OutputT> source) {
      super(source);
    }

    @Override
    public final OutputT getCurrent() throws NoSuchElementException {
      Block<OutputT> currentBlock = getCurrentBlock();
      if (currentBlock == null) {
        throw new NoSuchElementException(
            "No block has been successfully read from " + getCurrentSource());
      }
      return currentBlock.getCurrentRecord();
    }

    @Override
    protected boolean isAtSplitPoint() {
      return atSplitPoint;
    }

    @Override
    protected final boolean readNextRecord() throws IOException {
      atSplitPoint = false;
      while (getCurrentBlock() == null || !getCurrentBlock().readNextRecord()) {
        if (!readNextBlock()) {
          return false;
        }
        // The first record in a block is a split point.
        atSplitPoint = true;
      }
      return true;
    }

    @Override
    @Nullable
    public Double getFractionConsumed() {
      if (!isStarted()) {
        return 0.0;
      }
      if (isDone()) {
        return 1.0;
      }
      OffsetBasedSource<OutputT> source = getCurrentSource();
      if (source.getEndOffset() == Long.MAX_VALUE) {
        return null;
      }

      long currentBlockOffset = getCurrentBlockOffset();
      long startOffset = source.getStartOffset();
      long endOffset = source.getEndOffset();
      double fractionAtBlockStart =
          ((double) (currentBlockOffset - startOffset)) / (endOffset - startOffset);
      double fractionAtBlockEnd =
          ((double) (currentBlockOffset + getCurrentBlockSize() - startOffset)
              / (endOffset - startOffset));
      double blockFraction = getCurrentBlock().getFractionOfBlockConsumed();
      return Math.min(
          1.0,
          fractionAtBlockStart + blockFraction * (fractionAtBlockEnd - fractionAtBlockStart));
    }

    @Override
    protected long getCurrentOffset() {
      return getCurrentBlockOffset();
    }

    /**
     * Read the next block from the input.
     */
    public abstract boolean readNextBlock() throws IOException;

    /**
     * Returns the current block (the block that was read by the last successful call to
     * {@link #readNextBlock()}. May return null initially, or if no block has been successfully
     * read.
     */
    @Nullable
    public abstract Block<OutputT> getCurrentBlock();

    /**
     * Returns the size of the current block in bytes. May return {@code 0} if the size of the
     * current block is unknown.
     */
    public abstract long getCurrentBlockSize();

    /**
     * Returns the largest offset such that starting to read from that offset includes the current
     * block.
     */
    public abstract long getCurrentBlockOffset();
  }

  @Override
  public OffsetBasedReader<T> createReader(PipelineOptions options) {
    return new BigQueryStorageStreamReader<>(this, options);
  }

  /**
   * A {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader} to read from a BigQuery storage
   * stream source.
   */
  @Experimental(Kind.SOURCE_SINK)
  static class BigQueryStorageStreamReader<T> extends BlockBasedReader<ReadRowsResponse, T> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamReader.class);

    // The schema for the source (e.g. for the underlying read session).
    private final StructType schema;

    // The parse function for the source.
    private final SerializableFunction<SchemaAndRowProto, T> parseFn;

    // BigQuery services for the source.
    private final BigQueryServices bqServices;

    // BigQuery options for the source.
    private final BigQueryOptions bqOptions;

    // An iterator over the server stream responses.
    private Iterator<ReadRowsResponse> responseIterator;

    // The current block. Initialized in readNextRecord.
    @Nullable private RowProtoBlock<T> currentBlock;

    // A lock used to synchronize block offsets.
    private final Object progressLock = new Object();

    // Offset of the current block.
    @GuardedBy("progressLock")
    private long currentBlockOffset = 0;

    // Size of the current block.
    @GuardedBy("progressLock")
    private long currentBlockSizeBytes = 0;

    public BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, PipelineOptions options) {
      super(source);
      this.schema = source.getReadSession().getProjectedSchema();
      this.parseFn = source.getParseFn();
      this.bqServices = source.getBigQueryServices();
      this.bqOptions = options.as(BigQueryOptions.class);
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return (BigQueryStorageStreamSource<T>) super.getCurrentSource();
    }

    @Override
    public ServerStream<ReadRowsResponse> initializeServerStream() throws IOException {
      BigQueryStorageStreamSource<T> source = getCurrentSource();
      LOG.info("Initializing a server stream for source " + source);

      ReadRowsRequest request = ReadRowsRequest.newBuilder()
          .setReadPosition(StreamPosition.newBuilder()
              .setStream(source.getStream())
              .setOffset(source.getStartOffset()))
          .build();

      responseIterator = bqServices.getTableReadService(bqOptions).readRows(request);
      // TODO: Convert TableReadService to return a GAX ServerStream object.
      return null;
    }

    @Override
    public boolean readNextBlock() {
      long startOfNextBlock;
      synchronized (progressLock) {
        startOfNextBlock = currentBlockOffset + currentBlockSizeBytes;
      }

      if (!responseIterator.hasNext()) {
        return false;
      }

      ReadRowsResponse nextResponse = responseIterator.next();
      currentBlock = new RowProtoBlock<>(schema, parseFn, nextResponse.getRowsList());
      // TODO: Update the estimated row count for the stream.

      synchronized (progressLock) {
        currentBlockOffset = startOfNextBlock;
        currentBlockSizeBytes = nextResponse.getRowsCount();
      }

      return true;
    }

    @Override
    public RowProtoBlock<T> getCurrentBlock() {
      return currentBlock;
    }

    @Override
    public long getCurrentBlockOffset() {
      synchronized (progressLock) {
        return currentBlockOffset;
      }
    }

    @Override
    public long getCurrentBlockSize() {
      synchronized (progressLock) {
        return currentBlockSizeBytes;
      }
    }
  }
}
