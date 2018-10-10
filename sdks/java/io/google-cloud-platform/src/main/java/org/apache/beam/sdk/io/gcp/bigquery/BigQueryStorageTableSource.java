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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Format;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.TableReadService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link BoundedSource} for reading BigQuery tables using the BigQuery storage read API. */
class BigQueryStorageTableSource<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageTableSource.class);

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

  public static <T> BigQueryStorageTableSource<T> create(
      ValueProvider<TableReference> tableRefProvider,
      @Nullable ReadSessionOptions readSessionOptions,
      TypedRead.Format format,
      @Nullable SerializableFunction<SchemaAndRecord, T> avroParseFn,
      @Nullable SerializableFunction<SchemaAndRowProto, T> rowProtoParseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageTableSource<>(
        NestedValueProvider.of(
            checkNotNull(tableRefProvider, "tableRefProvider"), new TableRefToJson()),
        readSessionOptions,
        format,
        avroParseFn,
        rowProtoParseFn,
        outputCoder,
        bqServices);
  }

  private final ValueProvider<String> jsonTableRefProvider;
  private final ReadSessionOptions readSessionOptions;
  private final TypedRead.Format format;
  private final SerializableFunction<SchemaAndRecord, T> avroParseFn;
  private final SerializableFunction<SchemaAndRowProto, T> rowProtoParseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private transient AtomicReference<Long> cachedReadSizeBytes;

  private BigQueryStorageTableSource(
      ValueProvider<String> jsonTableRefProvider,
      @Nullable ReadSessionOptions readSessionOptions,
      TypedRead.Format format,
      @Nullable SerializableFunction<SchemaAndRecord, T> avroParseFn,
      @Nullable SerializableFunction<SchemaAndRowProto, T> rowProtoParseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.jsonTableRefProvider = checkNotNull(jsonTableRefProvider, "jsonTableRefProvider");
    this.readSessionOptions = readSessionOptions;
    this.format = checkNotNull(format, "format");
    this.avroParseFn = avroParseFn;
    this.rowProtoParseFn = rowProtoParseFn;
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.cachedReadSizeBytes = new AtomicReference<>();
  }

  @Override
  public void validate() {
    if (format == Format.ROW_PROTO) {
      checkState(
          rowProtoParseFn != null, "A row proto parse function is required with format ROW_PROTO");
      checkState(
          avroParseFn == null, "An Avro parse function may not be specified with format ROW_PROTO");
    } else {
      checkState(avroParseFn != null, "An Avro parse function is required with format AVRO");
      checkState(
          rowProtoParseFn == null,
          "A row proto parse function may not be specified with format AVRO");
    }
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public List<? extends BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    TableReference tableReference = getEffectiveTableReference(bqOptions);
    long tableSizeBytes = getEstimatedSizeBytes(bqOptions);
    int streamCount = 0;
    if (desiredBundleSizeBytes > 0) {
      streamCount =
          (tableSizeBytes / desiredBundleSizeBytes) > MAX_SPLIT_COUNT
              ? MAX_SPLIT_COUNT
              : (int) (tableSizeBytes / desiredBundleSizeBytes);
    }

    if (streamCount < MIN_SPLIT_COUNT) {
      streamCount = MIN_SPLIT_COUNT;
    }

    ReadSession readSession;
    try (TableReadService client = bqServices.getTableReadService(bqOptions)) {
      readSession =
          BigQueryHelpers.createReadSession(
              client, tableReference, streamCount, readSessionOptions, format);
    }

    LOG.info("Created read session {} for table {}", readSession.getName(), tableReference);

    if (readSession.getStreamsCount() == 0) {
      return ImmutableList.of();
    }

    List<BoundedSource<T>> sources = new ArrayList<>(readSession.getStreamsCount());
    for (Stream stream : readSession.getStreamsList()) {
      sources.add(
          format == Format.ROW_PROTO
              ? BigQueryRowProtoStreamSource.create(
                  readSession, stream, rowProtoParseFn, outputCoder, bqServices)
              : BigQueryAvroStreamSource.create(
                  readSession, stream, avroParseFn, outputCoder, bqServices));
    }

    return ImmutableList.copyOf(sources);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (cachedReadSizeBytes.get() == null) {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      TableReference tableReference = getEffectiveTableReference(bqOptions);
      Table table = bqServices.getDatasetService(bqOptions).getTable(tableReference);
      long readSizeBytes = 0;
      if (table != null) {
        readSizeBytes = table.getNumBytes();
      }
      cachedReadSizeBytes.compareAndSet(null, readSizeBytes);
    }
    return cachedReadSizeBytes.get();
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) {
    throw new UnsupportedOperationException("BigQuery source must be split before being read");
  }

  /**
   * Returns the effective table reference for the table. If the caller has not specified a project
   * ID directly in the table reference, then the project ID is set from the pipeline options.
   */
  private TableReference getEffectiveTableReference(BigQueryOptions options) throws IOException {
    TableReference tableReference =
        BigQueryIO.JSON_FACTORY.fromString(jsonTableRefProvider.get(), TableReference.class);
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      checkState(
          !Strings.isNullOrEmpty(options.getProject()),
          "No project ID was set in %s or %s; cannot construct a complete %s",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName(),
          TableReference.class.getSimpleName());
      tableReference.setProjectId(options.getProject());
    }
    return tableReference;
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    cachedReadSizeBytes = new AtomicReference<>();
  }
}
