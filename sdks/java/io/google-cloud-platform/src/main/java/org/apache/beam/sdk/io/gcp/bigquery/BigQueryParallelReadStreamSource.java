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

import com.google.cloud.bigquery.storage.v1alpha1.Storage;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
public class BigQueryParallelReadStreamSource<T> extends BoundedSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryParallelReadStreamSource.class);

  static <T> BigQueryParallelReadStreamSource<T> create(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      Storage.ReadSession readSession,
      Storage.StreamPosition streamPosition,
      Long readSizeBytes) {
    return new BigQueryParallelReadStreamSource<>(
        bqServices,
        parseFn,
        coder,
        readSession,
        streamPosition,
        readSizeBytes);
  }

  private final BigQueryServices bqServices;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> coder;
  private final Storage.ReadSession readSession;
  private final Storage.StreamPosition streamPosition;
  private final Long readSizeBytes;

  BigQueryParallelReadStreamSource(
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
  public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
    return new BigQueryParallelReadStreamReader<>(
        readSession,
        streamPosition,
        parseFn,
        this,
        bqServices,
        pipelineOptions.as(BigQueryOptions.class));
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }

  /**
   * Iterates over all rows assigned to a particular reader in a read session.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  static class BigQueryParallelReadStreamReader<T> extends BoundedReader<T> {

    private Storage.ReadSession readSession;
    private final BigQueryServices client;
    private Iterator<Storage.ReadRowsResponse> responseIterator;
    private Iterator<Row> rowIterator;
    private final Storage.ReadRowsRequest request;
    private SerializableFunction<SchemaAndRowProto, T> parseFn;
    private BoundedSource<T> source;
    private BigQueryOptions options;
    private Row currentRow;

    BigQueryParallelReadStreamReader(
        Storage.ReadSession readSession,
        Storage.StreamPosition streamPosition,
        SerializableFunction<SchemaAndRowProto, T> parseFn,
        BoundedSource<T> source,
        BigQueryServices client,
        BigQueryOptions options) {
      this.readSession = checkNotNull(readSession, "readSession");
      this.client = checkNotNull(client, "client");
      this.parseFn = checkNotNull(parseFn, "parseFn");
      this.source = checkNotNull(source, "source");
      this.options = options;

      this.request = Storage.ReadRowsRequest.newBuilder()
          .setReadPosition(streamPosition)
          .build();
    }

    /**
     * Empty operation, the table is already open for read.
     * @throws IOException on failure
     */
    @Override
    public boolean start() throws IOException {
      responseIterator = client.getTableReadService(options).readRows(request);
      return advance();
    }

    @Override
    public boolean advance() {
      if (rowIterator == null || !rowIterator.hasNext()) {
        if (!responseIterator.hasNext()) {
          currentRow = null;
          return false;
        }
        rowIterator = responseIterator.next().getRowsList().iterator();
      }
      currentRow = rowIterator.next();
      return true;
    }

    @Override
    public T getCurrent() {
      if (currentRow == null) {
        return null;
      }
      return parseFn.apply(new SchemaAndRowProto(readSession.getProjectedSchema(), currentRow));
    }

    @Override
    public void close() {
      // Do nothing.
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }
  }
}
