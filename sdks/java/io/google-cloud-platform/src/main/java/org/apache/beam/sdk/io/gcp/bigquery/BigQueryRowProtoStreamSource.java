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

import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A source representing reading from a stream in {@link Row} format using the BigQuery storage read
 * API.
 */
@Experimental(Kind.SOURCE_SINK)
class BigQueryRowProtoStreamSource<T> extends BigQueryStreamSourceBase<SchemaAndRowProto, T> {

  static <T> BigQueryRowProtoStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryRowProtoStreamSource<>(
        readSession,
        stream,
        0L,
        StreamOffsetRangeTracker.OFFSET_INFINITY,
        SplitDisposition.SELF,
        null,
        parseFn,
        outputCoder,
        bqServices);
  }

  private BigQueryRowProtoStreamSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      @Nullable StreamPosition splitPosition,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    super(
        readSession,
        stream,
        startOffset,
        stopOffset,
        splitDisposition,
        splitPosition,
        parseFn,
        outputCoder,
        bqServices);
  }

  @Override
  protected BigQueryRowProtoStreamSource<T> newChildSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      @Nullable StreamPosition splitPosition,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryRowProtoStreamSource<>(
        readSession,
        stream,
        startOffset,
        stopOffset,
        splitDisposition,
        splitPosition,
        parseFn,
        outputCoder,
        bqServices);
  }

  @Override
  public BigQueryRowProtoStreamReader<T> createReader(PipelineOptions options) throws IOException {
    return new BigQueryRowProtoStreamReader<>(this, options.as(BigQueryOptions.class));
  }

  /**
   * A reader for reading from a stream in {@link Row} format using the BigQuery storage read API.
   */
  @Experimental(Kind.SOURCE_SINK)
  static class BigQueryRowProtoStreamReader<T>
      extends BigQueryStreamReaderBase<SchemaAndRowProto, T> {

    private final SerializableFunction<SchemaAndRowProto, T> parseFn;
    private final StructType schema;

    // These objects can only be accessed in the context of the reader thread.
    private Iterator<Row> rowIterator;
    private T currentRecord;

    private BigQueryRowProtoStreamReader(
        BigQueryRowProtoStreamSource<T> source, BigQueryOptions options) throws IOException {
      super(source, options);
      this.parseFn = source.getParseFn();
      this.schema = source.getReadSession().getProjectedSchema();
    }

    @Override
    protected boolean readNextRow() throws IOException {
      while (rowIterator == null || !rowIterator.hasNext()) {
        ReadRowsResponse nextResponse = readNextStreamResponse();
        if (nextResponse == null) {
          return false;
        }

        rowIterator = nextResponse.getRowsList().iterator();
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
    public void invalidateStream() {
      super.invalidateStream();
      rowIterator = null;
    }
  }
}
