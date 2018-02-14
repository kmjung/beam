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
import com.google.cloud.bigquery.v3.ReadOptions;
import com.google.cloud.bigquery.v3.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.v3.TableReferenceProto;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BoundedSource} for reading BigQuery tables using the BigQuery parallel read API.
 */
class BigQueryParallelReadTableSource<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryParallelReadTableSource.class);

  private final BigQueryServices bqServices;
  private final Coder<T> coder;
  private final SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final Session session;
  private final ValueProvider<String> jsonTable;
  private final ReadLocation initialReadLocation;
  private final ReadSessionOptions readOptions;

  private transient Long tableSizeBytes;
  private transient List<BoundedSource<T>> cachedSplitResult;

  private BigQueryParallelReadTableSource(
      ValueProvider<String> jsonTable,
      Session session,
      ReadLocation readLocation,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryServices bqServices,
      ReadSessionOptions readOptions) {
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.coder = checkNotNull(coder, "coder");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.jsonTable = checkNotNull(jsonTable, "jsonTable");
    this.session = session;
    this.initialReadLocation = readLocation;
    this.readOptions = readOptions;
  }

  /**
   * This method creates a {@link BigQueryParallelReadTableSource} with no initial session or read
   * location.
   */
  public static <T> BigQueryParallelReadTableSource<T> create(
      ValueProvider<TableReference> table,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryServices bqServices,
      ReadSessionOptions readOptions) {
    return new BigQueryParallelReadTableSource<>(
        NestedValueProvider.of(checkNotNull(table, "table"), new TableRefToJson()),
        null,
        null,
        parseFn,
        coder,
        checkNotNull(bqServices),
        readOptions);
  }

  /*
   * Gets the total size of the table.
   */
  @Override
  public synchronized long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (tableSizeBytes == null) {
      TableReference table =
          BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);
      Table tableRef = bqServices.getDatasetService(options.as(BigQueryOptions.class))
          .getTable(table);
      tableSizeBytes = tableRef.getNumBytes();
    }
    return tableSizeBytes;
  }

  private static TableReferenceProto.TableReference
  convertTableReferenceToV3(TableReference tableReference) {
    return TableReferenceProto.TableReference.newBuilder()
        .setProjectId(tableReference.getProjectId())
        .setDatasetId(tableReference.getDatasetId())
        .setTableId(tableReference.getTableId())
        .build();
  }

  /**
   * Creates a read session with the number of readers based on size estimation. This is
   * only for the initial split when job is created. If the caller wants to split more, they will
   * call Split function on the reader.
   */
  @Override
  public List<BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    if (cachedSplitResult == null) {
      ReadOptions.TableReadOptions tableReadOptions = null;
      if (readOptions != null) {
        TableReadOptions.Builder builder = null;
        String sqlFilter = readOptions.getSqlFilter();
        if (!Strings.isNullOrEmpty(sqlFilter)) {
          if (builder == null) {
            builder = TableReadOptions.newBuilder();
          }
          builder.setSqlFilter(sqlFilter);
        }

        List<String> selectedFields = readOptions.getSelectedFields();
        if (selectedFields != null && !selectedFields.isEmpty()) {
          if (builder == null) {
            builder = TableReadOptions.newBuilder();
          }
          for (String selectedField : selectedFields) {
            builder.addSelectedFields(selectedField);
          }
        }

        if (builder != null) {
          tableReadOptions = builder.build();
        }
      }

      // If the reader count is not set, then the server will return the maximum number of initial
      // read locations.
      CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
      builder.setTableReference(convertTableReferenceToV3(
          BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class)));
      if (tableReadOptions != null) {
        builder.setReadOptions(tableReadOptions);
      }

      CreateSessionRequest request = builder.build();
      Session session = bqServices.getTableReadService(bqOptions).createSession(request);
      cachedSplitResult = createSources(jsonTable, session);
    }
    return cachedSplitResult;
  }

  /**
   * Create {@link BigQueryParallelReadTableSource} based on all the initial read locations in
   * {@link Session}.
   */
  private List<BoundedSource<T>> createSources(ValueProvider<String> jsonTable, Session session) {
    List<BoundedSource<T>> sources = Lists.newArrayList();
    for (ReadLocation location : session.getInitialReadLocationsList()) {
      sources.add(new BigQueryParallelReadTableSource<>(
          jsonTable,
          session,
          location,
          parseFn,
          coder,
          bqServices,
          readOptions));
    }
    return ImmutableList.copyOf(sources);
  }

  /**
   * You can potentially create multiple readers on a source.
   */
  @Override
  public BoundedReader<T> createReader(PipelineOptions options) {
    return new BigQueryParallelReader<>(
        session,
        initialReadLocation,
        (readOptions != null) ? readOptions.getRowBatchSize() : null,
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
