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
import com.google.cloud.bigquery.storage.v1alpha1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1alpha1.Storage;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BoundedSource} to read from existing read streams using the BigQuery parallel read API.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

  static <T> BigQueryStorageStreamSource<T> create(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      Storage.ReadSession readSession,
      Storage.StreamPosition streamPosition,
      Long readSizeBytes) {
    return new BigQueryStorageStreamSource<>(
        bqServices, parseFn, coder, readSession, streamPosition, readSizeBytes);
  }

  private final BigQueryServices bqServices;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> coder;
  private final Storage.ReadSession readSession;
  private final Storage.StreamPosition streamPosition;
  private final Long readSizeBytes;

  BigQueryStorageStreamSource(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      Storage.ReadSession readSession,
      Storage.StreamPosition streamPosition,
      Long readSizeBytes) {
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.coder = checkNotNull(coder, "coder");
    this.readSession = checkNotNull(readSession, "readSession");
    this.streamPosition = checkNotNull(streamPosition, "streamPosition");
    this.readSizeBytes = readSizeBytes;
  }

  @Override
  public List<? extends BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    // Stream source objects can't (currently) be split.
    return ImmutableList.of(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
    return readSizeBytes != null ? readSizeBytes : 0;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions pipelineOptions) throws IOException {
    return new BigQueryStorageStreamReader<>(this, pipelineOptions.as(BigQueryOptions.class));
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }

  /** Iterates over all rows assigned to a particular reader in a read session. */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  static class BigQueryStorageStreamReader<T> extends BoundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamReader.class);

    private final BigQueryStorageClient client;
    private final SerializableFunction<SchemaAndRowProto, T> parseFn;
    private final StructType schema;
    private final StreamOffsetRangeTracker rangeTracker;

    @GuardedBy("this")
    private BigQueryStorageStreamSource<T> currentSource;

    // This value can be read from any thread, although it is only written by the reader thread. The
    // "volatile" keyword prevents read and write tearing.
    private volatile long estimatedStreamLength = StreamOffsetRangeTracker.OFFSET_INFINITY;

    // These objects can only be accessed by the reader thread.
    private ServerStream<ReadRowsResponse> responseStream;
    private Iterator<ReadRowsResponse> responseIterator;
    private Iterator<Row> rowIterator;
    private StreamPosition currentPosition;
    private T currentRecord;

    BigQueryStorageStreamReader(BigQueryStorageStreamSource<T> source, BigQueryOptions options)
        throws IOException {
      this.currentSource = source;
      this.client = source.bqServices.getStorageClient(options);
      this.parseFn = source.parseFn;
      this.schema = source.readSession.getProjectedSchema();
      this.rangeTracker =
          new StreamOffsetRangeTracker(
              source.streamPosition.getStream(),
              0,
              StreamOffsetRangeTracker.OFFSET_INFINITY,
              false);
    }

    @Override
    public boolean start() throws IOException {
      return (startImpl() && rangeTracker.tryReturnRecordAt(true, currentPosition))
          || rangeTracker.markDone();
    }

    private boolean startImpl() {
      BigQueryStorageStreamSource<T> source = getCurrentSource();
      LOG.info("Starting reader {} from source {}", this, source);
      currentPosition = rangeTracker.getStartPosition();
      ReadRowsRequest request =
          ReadRowsRequest.newBuilder().setReadPosition(currentPosition).build();
      responseStream = client.readRowsCallable().call(request);
      responseIterator = responseStream.iterator();
      return readNextRow();
    }

    @Override
    public boolean advance() {
      return (advanceImpl() && rangeTracker.tryReturnRecordAt(true, currentPosition))
          || rangeTracker.markDone();
    }

    private boolean advanceImpl() {
      currentPosition =
          currentPosition.toBuilder().setOffset(currentPosition.getOffset() + 1).build();
      return readNextRow();
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
  }
}
