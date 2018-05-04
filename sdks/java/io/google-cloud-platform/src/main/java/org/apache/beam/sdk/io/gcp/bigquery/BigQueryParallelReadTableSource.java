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
import com.google.common.base.Strings;
import java.io.IOException;
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
class BigQueryParallelReadTableSource<T> extends BigQueryParallelReadSourceBase<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryParallelReadTableSource.class);

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
        readSessionOptions);
  }

  private final ValueProvider<String> jsonTableRefProvider;

  private transient Long cachedReadSizeBytes;

  private BigQueryParallelReadTableSource(
      ValueProvider<String> jsonTableRefProvider,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      BigQueryServices bqServices,
      ReadSessionOptions readSessionOptions) {
    super(bqServices, parseFn, coder, readSessionOptions);
    this.jsonTableRefProvider = checkNotNull(jsonTableRefProvider, "jsonTableRefProvider");
  }

  /**
   * Returns the effective table reference for the table. If the caller has not specified a project
   * ID directly in the table reference, then the project ID is set from the pipeline options.
   */
  protected TableReference getEffectiveTableReference(BigQueryOptions options) throws IOException {
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

  /**
   * Gets the estimated number of bytes returned by the current source. For sources created by a
   * split() operation, this is a fraction of the total table size.
   */
  @Override
  public synchronized long getEstimatedSizeBytes(PipelineOptions options)
      throws IOException, InterruptedException {
    if (cachedReadSizeBytes == null) {
      BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
      TableReference tableReference = getEffectiveTableReference(bigQueryOptions);
      Table table = bqServices.getDatasetService(bigQueryOptions).getTable(tableReference);
      if (table != null) {
        cachedReadSizeBytes = table.getNumBytes();
      }
      if (cachedReadSizeBytes == null) {
        cachedReadSizeBytes = 0L;
      }
    }

    return cachedReadSizeBytes;
  }
}
