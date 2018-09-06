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

import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
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
  public List<BigQueryStorageStreamSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    // Stream source objects can't be split further due to liquid sharding.
    return ImmutableList.of(this);
  }

  @Override
  public BigQueryStorageStreamSource<T> createSourceForSubrange(long startOffset, long endOffset) {
    // This method is unreachable.
    return null;
  }

  @Override
  public BigQueryStorageStreamReader<T> createReader(PipelineOptions options) {
    return new BigQueryStorageStreamReader<>(this, options);
  }

  /**
   * An {@link org.apache.beam.sdk.io.OffsetBasedSource.OffsetBasedReader} to read from a BigQuery
   * storage stream source.
   */
  public static class BigQueryStorageStreamReader<T> extends OffsetBasedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamReader.class);
    private final BigQueryOptions bqOptions;
    private final SerializableFunction<SchemaAndRowProto, T> parseFn;

    // A lock used to synchronize record offsets.
    private final Object progressLock = new Object();

    // Offset of the current record.
    @GuardedBy("progressLock")
    private long currentRecordOffset = 0;

    private Iterator<ReadRowsResponse> responseIterator;
    private Iterator<Row> recordIterator;
    private T currentRecord;

    public BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, PipelineOptions options) {
      super(source);
      bqOptions = options.as(BigQueryOptions.class);
      parseFn = source.getParseFn();
    }

    public long getCurrentOffset() {
      synchronized (progressLock) {
        return currentRecordOffset;
      }
    }

    public boolean startImpl() throws IOException {
      BigQueryStorageStreamSource<T> source = getCurrentSource();
      LOG.info("Starting reader for BQ stream source " + source);

      ReadRowsRequest request = ReadRowsRequest.newBuilder()
          .setReadPosition(StreamPosition.newBuilder()
              .setStream(source.getStream())
              .setOffset(source.getStartOffset()))
          .build();

      responseIterator =
          source.getBigQueryServices().getTableReadService(bqOptions).readRows(request);

      if (!readNextRecord()) {
        return false;
      }

      synchronized (progressLock) {
        currentRecordOffset = source.getStartOffset();
      }

      return true;
    }

    public boolean advanceImpl() {
      if (!readNextRecord()) {
        return false;
      }

      synchronized (progressLock) {
        ++currentRecordOffset;
      }

      return true;
    }

    private boolean readNextRecord() {
      while (recordIterator == null || !recordIterator.hasNext()) {
        if (!responseIterator.hasNext()) {
          currentRecord = null;
          return false;
        }
        recordIterator = responseIterator.next().getRowsList().iterator();
      }

      Row currentRow = recordIterator.next();
      currentRecord = parseFn.apply(new SchemaAndRowProto(
          getCurrentSource().getReadSession().getProjectedSchema(), currentRow));
      return true;
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return (BigQueryStorageStreamSource<T>) super.getCurrentSource();
    }

    @Override
    public boolean allowsDynamicSplitting() {
      // Don't support dynamic split attempts (for now).
      return false;
    }

    @Override
    public T getCurrent() {
      return currentRecord;
    }

    @Override
    public void close() {
      // Do nothing (for now).
    }
  }
}
