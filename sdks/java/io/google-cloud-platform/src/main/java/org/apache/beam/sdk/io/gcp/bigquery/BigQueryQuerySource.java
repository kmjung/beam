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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;

import com.google.api.services.bigquery.model.TableReference;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link BigQuerySourceBase} for querying BigQuery tables. */
@VisibleForTesting
class BigQueryQuerySource<T> extends BigQuerySourceBase<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQuerySource.class);

  static <T> BigQueryQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      QueryPriority priority,
      String location) {
    return new BigQueryQuerySource<>(
        stepUuid,
        query,
        flattenResults,
        useLegacySql,
        bqServices,
        coder,
        parseFn,
        priority,
        location);
  }

  private final BigQueryQueryHelper queryHelper;

  private BigQueryQuerySource(
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      QueryPriority priority,
      String location) {
    super(stepUuid, bqServices, coder, parseFn);
    queryHelper =
        new BigQueryQueryHelper(
            stepUuid, query, flattenResults, useLegacySql, bqServices, priority, location);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    return queryHelper.dryRunQueryIfNeeded(bqOptions).getQuery().getTotalBytesProcessed();
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions)
      throws IOException, InterruptedException {
    return queryHelper.createTableAndExecuteQuery(bqOptions);
  }

  @Override
  protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
    TableReference tableToRemove =
        createTempTableReference(
            bqOptions.getProject(), createJobIdToken(bqOptions.getJobName(), stepUuid));

    DatasetService tableService = bqServices.getDatasetService(bqOptions);
    LOG.info("Deleting temporary table with query results {}", tableToRemove);
    tableService.deleteTable(tableToRemove);
    LOG.info("Deleting temporary dataset with query results {}", tableToRemove.getDatasetId());
    tableService.deleteDataset(tableToRemove.getProjectId(), tableToRemove.getDatasetId());
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("query", queryHelper.getQuery()));
  }
}
