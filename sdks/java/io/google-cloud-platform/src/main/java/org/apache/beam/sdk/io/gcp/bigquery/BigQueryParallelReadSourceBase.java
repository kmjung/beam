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

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.v3.ParallelRead;
import com.google.cloud.bigquery.v3.ReadOptions;
import com.google.cloud.bigquery.v3.TableReferenceProto;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * An abstract {@link BoundedSource} to read data from BigQuery using the parallel read API.
 */
abstract class BigQueryParallelReadSourceBase<T> extends BoundedSource<T> {

  /**
   * The maximum number of readers which will be requested when creating a BigQuery read session as
   * part of a split operation, regardless of the desired bundle size.
   */
  private static final int MAX_SPLIT_COUNT = 10000;

  /**
   * The minimum number of readers which will be requested when creating a BigQuery read session as
   * part of a split operation, regardless of the desired bundle size. Note that the source may
   * still be split into fewer than ten component sources depending on the number of read locations
   * returned by the server at session creation time.
   */
  private static final int MIN_SPLIT_COUNT = 10;

  protected final BigQueryServices bqServices;

  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> coder;
  private final BigQueryIO.ReadSessionOptions readSessionOptions;

  BigQueryParallelReadSourceBase(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryIO.ReadSessionOptions readSessionOptions) {
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.coder = checkNotNull(coder, "coder");
    this.readSessionOptions = readSessionOptions;
  }

  protected abstract TableReference getEffectiveTableReference(BigQueryOptions bqOptions)
      throws Exception;

  @Override
  public List<BoundedSource<T>> split(long desiredBundleSizeBytes, PipelineOptions options)
      throws Exception {

    TableReference tableReference = getEffectiveTableReference(options.as(BigQueryOptions.class));

    ParallelRead.CreateSessionRequest.Builder requestBuilder =
        ParallelRead.CreateSessionRequest.newBuilder()
            .setTableReference(TableReferenceProto.TableReference.newBuilder()
                .setProjectId(tableReference.getProjectId())
                .setDatasetId(tableReference.getDatasetId())
                .setTableId(tableReference.getTableId()));

    long tableSizeBytes = getEstimatedSizeBytes(options);
    int readerCount = 0;
    if (desiredBundleSizeBytes > 0) {
      readerCount = (tableSizeBytes / desiredBundleSizeBytes) > MAX_SPLIT_COUNT
          ? MAX_SPLIT_COUNT
          : (int) (tableSizeBytes / desiredBundleSizeBytes);
    }

    if (readerCount < MIN_SPLIT_COUNT) {
      readerCount = MIN_SPLIT_COUNT;
    }

    requestBuilder.setReaderCount(readerCount);

    if (readSessionOptions != null) {
      ReadOptions.TableReadOptions.Builder readOptionsBuilder = null;
      String sqlFilter = readSessionOptions.getSqlFilter();
      if (!Strings.isNullOrEmpty(sqlFilter)) {
        readOptionsBuilder = ReadOptions.TableReadOptions.newBuilder().setSqlFilter(sqlFilter);
      }

      List<String> selectedFields = readSessionOptions.getSelectedFields();
      if (selectedFields != null && !selectedFields.isEmpty()) {
        if (readOptionsBuilder == null) {
          readOptionsBuilder = ReadOptions.TableReadOptions.newBuilder();
        }
        for (String selectedField : selectedFields) {
          readOptionsBuilder.addSelectedFields(selectedField);
        }
      }

      if (readOptionsBuilder != null) {
        requestBuilder.setReadOptions(readOptionsBuilder);
      }
    }

    ParallelRead.CreateSessionRequest createSessionRequest = requestBuilder.build();
    ParallelRead.Session readSession = bqServices
        .getTableReadService(options.as(BigQueryOptions.class))
        .createSession(createSessionRequest);
    if (readSession.getInitialReadLocationsCount() == 0) {
      return ImmutableList.of();
    }

    Long readSizeBytes = tableSizeBytes / readSession.getInitialReadLocationsCount();
    List<BoundedSource<T>> sources = new ArrayList<>(readSession.getInitialReadLocationsCount());
    for (ParallelRead.ReadLocation readLocation : readSession.getInitialReadLocationsList()) {
      sources.add(new BigQueryParallelReadStreamSource<>(
          bqServices,
          parseFn,
          coder,
          readSessionOptions,
          readSession,
          readLocation,
          readSizeBytes));
    }

    return ImmutableList.copyOf(sources);
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) {
    throw new UnsupportedOperationException("BigQuery source must be split before being read");
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }
}
