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
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** A Javadoc comment. */
@Experimental(Kind.SOURCE_SINK)
public class BigQueryAvroStreamSource<T> extends BigQueryStreamSourceBase<SchemaAndRecord, T> {

  static <T> BigQueryAvroStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryAvroStreamSource<>(
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

  private BigQueryAvroStreamSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      @Nullable StreamPosition splitPosition,
      SerializableFunction<SchemaAndRecord, T> parseFn,
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
  protected BigQueryAvroStreamSource<T> newChildSource(
      ReadSession readSession,
      Stream stream,
      long startOffset,
      long stopOffset,
      SplitDisposition splitDisposition,
      StreamPosition splitPosition,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryAvroStreamSource<>(
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
  public BigQueryAvroStreamReader<T> createReader(PipelineOptions options) throws IOException {
    return new BigQueryAvroStreamReader<>(this, options.as(BigQueryOptions.class));
  }

  /** A Javadoc comment. */
  @Experimental(Kind.SOURCE_SINK)
  public static class BigQueryAvroStreamReader<T>
      extends BigQueryStreamReaderBase<SchemaAndRecord, T> {

    private final SerializableFunction<SchemaAndRecord, T> parseFn;
    private final DatumReader<GenericRecord> datumReader;

    // These values can only be accessed in the context of the reader thread.
    private BinaryDecoder decoder;
    private GenericRecord nextRecord;
    private T currentRecord;

    private BigQueryAvroStreamReader(BigQueryAvroStreamSource<T> source, BigQueryOptions options)
        throws IOException {
      super(source, options);
      this.parseFn = source.getParseFn();
      this.datumReader =
          new GenericDatumReader<>(
              new Schema.Parser().parse(source.getReadSession().getAvroSchema().getJsonSchema()));
    }

    @Override
    public boolean readNextRecord() throws IOException {
      while (decoder == null || decoder.isEnd()) {
        ReadRowsResponse nextResponse = readNextStreamResponse();
        if (nextResponse == null) {
          return false;
        }

        decoder =
            DecoderFactory.get()
                .binaryDecoder(
                    nextResponse.getAvroRows().getSerializedBinaryRows().toByteArray(), decoder);
      }

      nextRecord = datumReader.read(nextRecord, decoder);
      currentRecord = parseFn.apply(new SchemaAndRecord(nextRecord, null));
      return true;
    }

    @Override
    public T getCurrent() {
      return currentRecord;
    }

    @Override
    public void invalidateStream() {
      super.invalidateStream();
      decoder = null;
    }
  }
}
