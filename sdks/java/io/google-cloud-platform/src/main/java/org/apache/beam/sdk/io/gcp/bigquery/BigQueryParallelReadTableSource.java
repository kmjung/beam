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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.v3.ParallelRead.CreateSessionRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadLocation;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import com.google.cloud.bigquery.v3.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.v3.TableReferenceProto;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A {@link BoundedSource} for reading BigQuery tables using the BigQuery parallel read API.
 */
class BigQueryParallelReadTableSource<T> extends BoundedSource<T> {

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

  private final ValueProvider<String> jsonTableRefProvider;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Coder<T> coder;
  private final BigQueryServices bqServices;
  private final Session readSession;
  private final ReadLocation readLocation;
  private final ReadSessionOptions readSessionOptions;
  private final Long readSizeBytes;

  private transient Long cachedReadSizeBytes;

  /**
   * This method creates a new {@link BigQueryParallelReadTableSource} with no initial read session
   * or read location.
   */
  public static <T> BigQueryParallelReadTableSource<T> create(
      ValueProvider<TableReference> tableRefProvider,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryServices bqServices,
      ReadSessionOptions readSessionOptions) {
    return new BigQueryParallelReadTableSource<>(
        NestedValueProvider.of(
            checkNotNull(tableRefProvider, "tableRefProvider"),
            new TableRefToJson()),
        parseFn,
        coder,
        bqServices,
        readSessionOptions,
        null,
        null,
        null);
  }

  private BigQueryParallelReadTableSource(
      ValueProvider<String> jsonTableRefProvider,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryServices bqServices,
      ReadSessionOptions readSessionOptions,
      Session readSession,
      ReadLocation readLocation,
      Long readSizeBytes) {
    this.jsonTableRefProvider = checkNotNull(jsonTableRefProvider, "jsonTableRefProvider");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.coder = checkNotNull(coder, "coder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.readSessionOptions = readSessionOptions;
    this.readSession = readSession;
    this.readLocation = readLocation;
    this.readSizeBytes = readSizeBytes;
  }

  /**
   * Gets the estimated number of bytes returned by the current source. For sources created by a
   * split() operation, this is a fraction of the total table size.
   */
  @Override
  public synchronized long getEstimatedSizeBytes(PipelineOptions options)
      throws IOException, InterruptedException {
    if (readSizeBytes != null) {
      return readSizeBytes;
    }

    if (cachedReadSizeBytes == null) {
      TableReference tableReference = BigQueryIO.JSON_FACTORY.fromString(
          jsonTableRefProvider.get(), TableReference.class);
      Table table = bqServices.getDatasetService(options.as(BigQueryOptions.class))
          .getTable(tableReference);
      if (table != null) {
        cachedReadSizeBytes = table.getNumBytes();
      }
      if (cachedReadSizeBytes == null) {
        cachedReadSizeBytes = 0L;
      }
    }

    return cachedReadSizeBytes;
  }

  @Override
  public List<BoundedSource<T>> split(long desiredBundleSizeBytes, PipelineOptions options)
      throws IOException, InterruptedException {
    if (readSession != null) {
      return ImmutableList.of(this);
    }

    CreateSessionRequest.Builder requestBuilder = CreateSessionRequest.newBuilder();
    TableReference tableReference = BigQueryIO.JSON_FACTORY.fromString(
        jsonTableRefProvider.get(), TableReference.class);
    requestBuilder.setTableReference(
        TableReferenceProto.TableReference.newBuilder()
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
      TableReadOptions.Builder readOptionsBuilder = null;
      String sqlFilter = readSessionOptions.getSqlFilter();
      if (!Strings.isNullOrEmpty(sqlFilter)) {
        readOptionsBuilder = TableReadOptions.newBuilder().setSqlFilter(sqlFilter);
      }

      List<String> selectedFields = readSessionOptions.getSelectedFields();
      if (selectedFields != null && !selectedFields.isEmpty()) {
        if (readOptionsBuilder == null) {
          readOptionsBuilder = TableReadOptions.newBuilder();
        }
        for (String selectedField : selectedFields) {
          readOptionsBuilder.addSelectedFields(selectedField);
        }
      }

      if (readOptionsBuilder != null) {
        requestBuilder.setReadOptions(readOptionsBuilder);
      }
    }

    CreateSessionRequest createSessionRequest = requestBuilder.build();
    Session session = bqServices.getTableReadService(options.as(BigQueryOptions.class))
        .createSession(createSessionRequest);

    Long readSizeBytes = tableSizeBytes / session.getInitialReadLocationsCount();
    List<BoundedSource<T>> sources = new ArrayList<>(session.getInitialReadLocationsCount());
    for (ReadLocation readLocation : session.getInitialReadLocationsList()) {
      sources.add(new BigQueryParallelReadTableSource<>(
          jsonTableRefProvider,
          parseFn,
          coder,
          bqServices,
          readSessionOptions,
          session,
          readLocation,
          readSizeBytes));
    }

    return ImmutableList.copyOf(sources);
  }

  /**
   * You can potentially create multiple readers on a source.
   */
  @Override
  public BoundedReader<T> createReader(PipelineOptions options) {
    return new BigQueryParallelReader<>(
        readSession,
        readLocation,
        (readSessionOptions != null && readSessionOptions.getRowBatchSize() != null)
            ? readSessionOptions.getRowBatchSize()
            : 1000,
        parseFn,
        this,
        bqServices,
        options.as(BigQueryOptions.class));
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }
}
