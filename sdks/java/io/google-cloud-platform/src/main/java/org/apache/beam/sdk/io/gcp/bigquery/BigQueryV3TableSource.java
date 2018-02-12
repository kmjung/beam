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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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
 * An abstract {@link BoundedSource} to read a table from BigQuery.
 *
 * <p>This source is to read Bigquery data directly using ParallelRead API.
 * <ul>
 * <li>{@link BigQueryV3TableSource} is for reading BigQuery tables</li>
 * </ul>
 * ...
 */
class BigQueryV3TableSource<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryV3TableSource.class);

  private BigQueryServices bqServices;
  private Coder<T> coder;
  private SerializableFunction<SchemaAndRowProto, T> parseFn;
  private final AtomicReference<Long> tableSizeBytes;
  private Session session;
  private ValueProvider<String> jsonTable;
  private ReadLocation initialReadLocation;
  private ReadSessionOptions readOptions;
  private transient List<BoundedSource<T>> cachedSplitResult;

  private BigQueryV3TableSource(ValueProvider<String> jsonTable,
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
    this.tableSizeBytes = new AtomicReference<>();
    this.initialReadLocation = readLocation;
    this.readOptions = readOptions;
  }

  /**
   * Creation the initial {@link BigQueryV3TableSource}, with Session and ReadLocation to be null.
   */
  public static <T> BigQueryV3TableSource<T> create(
      ValueProvider<TableReference> table,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryServices bqServices,
      ReadSessionOptions readOptions) {
    return new BigQueryV3TableSource<>(
        NestedValueProvider.of(checkNotNull(table, "table"), new TableRefToJson()),
        null,
        // First TableSource shouldn't be used to CreateReader.
        ReadLocation.getDefaultInstance(),
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
    if (tableSizeBytes.get() == null) {
      TableReference table =
          BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);

      Table tableRef = bqServices.getDatasetService(options.as(BigQueryOptions.class))
          .getTable(table);
      Long numBytes = tableRef.getNumBytes();
      if (tableRef.getStreamingBuffer() != null) {
        numBytes += tableRef.getStreamingBuffer().getEstimatedBytes().longValue();
      }

      tableSizeBytes.compareAndSet(null, numBytes);
    }
    return tableSizeBytes.get();
  }

  private static com.google.cloud.bigquery.v3.TableReferenceProto.TableReference
  convertTableReferenceToV3(TableReference tableReference) {
    com.google.cloud.bigquery.v3.TableReferenceProto.TableReference.Builder ref =
        com.google.cloud.bigquery.v3.TableReferenceProto.TableReference.newBuilder();
    return ref.setProjectId(tableReference.getProjectId())
        .setDatasetId(tableReference.getDatasetId())
        .setTableId(tableReference.getTableId()).build();
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
      ReadOptions.TableReadOptions.Builder tableReadOptions =
          ReadOptions.TableReadOptions.newBuilder();
      if (readOptions != null) {
        if (!Strings.isNullOrEmpty(readOptions.getSqlFilter())) {
          tableReadOptions.setSqlFilter(readOptions.getSqlFilter());
        }
        if (readOptions.getSelectedFields() != null && !readOptions.getSelectedFields().isEmpty()) {
          for (String field : readOptions.getSelectedFields()) {
            tableReadOptions.addSelectedFields(field);
          }
        }
      }
      // Without setting the reader count, it should always return the max number of readers
      // possible.
      CreateSessionRequest request = CreateSessionRequest.newBuilder()
          .setTableReference(
              convertTableReferenceToV3(
                  BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class)))
          .setReadOptions(tableReadOptions.build())
          .build();
      session = bqServices.getTableReadService(bqOptions).createSession(request);
      LOG.info("Created Session: " + session.getName() + " with "
          + session.getInitialReadLocationsList().size() + " readers");
      cachedSplitResult = createV3Sources(jsonTable, session);
      return cachedSplitResult;
    }
    return cachedSplitResult;
  }

  /**
   * Create {@link BigQueryV3TableSource} based on all the initial read locations in
   * {@link Session}.
   */
  private List<BoundedSource<T>> createV3Sources(ValueProvider<String> jsonTable, Session session) {
    List<BoundedSource<T>> sources = Lists.newArrayList();
    for (ReadLocation location : session.getInitialReadLocationsList()) {
      sources.add(new BigQueryV3TableSource<>(
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
    return BigQueryV3Reader.create(
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

  public Session getSession() {
    return session;
  }
}
