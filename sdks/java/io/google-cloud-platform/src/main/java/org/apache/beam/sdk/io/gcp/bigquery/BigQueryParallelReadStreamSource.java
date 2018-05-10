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

import com.google.cloud.bigquery.v3.ParallelRead;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BoundedSource} to read from existing read streams using the BigQuery parallel read API.
 */
public class BigQueryParallelReadStreamSource<T> extends BoundedSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryParallelReadStreamSource.class);

  static <T> BigQueryParallelReadStreamSource<T> create(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryIO.ReadSessionOptions readSessionOptions,
      ParallelRead.Session readSession,
      ParallelRead.ReadLocation readLocation,
      Long readSizeBytes) {
    return new BigQueryParallelReadStreamSource<>(
        bqServices,
        parseFn,
        coder,
        readSessionOptions,
        readSession,
        readLocation,
        readSizeBytes);
  }

  private final BigQueryServices bqServices;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> coder;
  private final BigQueryIO.ReadSessionOptions readSessionOptions;
  private final ParallelRead.Session readSession;
  private final ParallelRead.ReadLocation readLocation;
  private final Long readSizeBytes;

  BigQueryParallelReadStreamSource(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryIO.ReadSessionOptions readSessionOptions,
      ParallelRead.Session readSession,
      ParallelRead.ReadLocation readLocation,
      Long readSizeBytes) {
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.coder = checkNotNull(coder, "coder");
    this.readSessionOptions = readSessionOptions;
    this.readSession = checkNotNull(readSession, "readSession");
    this.readLocation = checkNotNull(readLocation, "readLocation");
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
        readLocation,
        (readSessionOptions != null && readSessionOptions.getRowBatchSize() != null)
            ? readSessionOptions.getRowBatchSize()
            : 1000,
        parseFn,
        this,
        bqServices,
        pipelineOptions.as(BigQueryOptions.class));
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }
}
