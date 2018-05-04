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
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.beam.sdk.io.BoundedSource} to read from BigQuery query results using the
 * BigQuery parallel read API.
 */
public class BigQueryParallelReadQuerySource<T> extends BigQueryParallelReadSourceBase<T> {

  // The maximum number of retries to poll a BigQuery job.
  private static final int JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryParallelReadQuerySource.class);

  public static <T> BigQueryParallelReadQuerySource<T> create(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      ReadSessionOptions readSessionOptions,
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql) {
    return new BigQueryParallelReadQuerySource<>(
        bqServices,
        parseFn,
        coder,
        readSessionOptions,
        stepUuid,
        query,
        flattenResults,
        useLegacySql);
  }

  private final String stepUuid;
  private final ValueProvider<String> query;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;

  private transient AtomicReference<JobStatistics> dryRunJobStats;

  private BigQueryParallelReadQuerySource(
      BigQueryServices bqServices,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      Coder<T> coder,
      ReadSessionOptions readSessionOptions,
      String stepUuid,
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql) {
    super(bqServices, parseFn, coder, readSessionOptions);
    this.stepUuid = checkNotNull(stepUuid, "stepUuid");
    this.query = checkNotNull(query, "query");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.dryRunJobStats = new AtomicReference<>();
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options)
      throws InterruptedException, IOException {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    return dryRunQueryIfNeeded(bqOptions).getQuery().getTotalBytesProcessed();
  }

  @Override
  protected TableReference getEffectiveTableReference(BigQueryOptions bqOptions)
      throws InterruptedException, IOException {
    // 1. Find the location of the query.
    String location = null;
    List<TableReference> referencedTables =
        dryRunQueryIfNeeded(bqOptions).getQuery().getReferencedTables();
    DatasetService tableService = bqServices.getDatasetService(bqOptions);
    if (referencedTables != null && !referencedTables.isEmpty()) {
      TableReference queryTable = referencedTables.get(0);
      location = tableService.getTable(queryTable).getLocation();
    }

    // 2. Create the temporary dataset in the query location.
    TableReference tableToRead = createTempTableReference(
        bqOptions.getProject(), createJobIdToken(bqOptions.getJobName(), stepUuid));

    LOG.info("Creating temporary dataset {} for query results", tableToRead.getDatasetId());

    tableService.createDataset(
        tableToRead.getProjectId(),
        tableToRead.getDatasetId(),
        location,
        "Temporary tables for query results of job " + bqOptions.getJobName(),
        TimeUnit.DAYS.toMillis(1));

    // 3. Execute the query.
    String queryJobId = createJobIdToken(bqOptions.getJobName(), stepUuid) + "-query";

    LOG.info("Writing query results to temporary table {} with job {}", tableToRead, queryJobId);

    executeQuery(
        bqOptions.getProject(),
        queryJobId,
        tableToRead,
        bqServices.getJobService(bqOptions));

    LOG.info("Query job {} completed", queryJobId);

    return tableToRead;
  }

  private synchronized JobStatistics dryRunQueryIfNeeded(BigQueryOptions bqOptions)
      throws InterruptedException, IOException {
    if (dryRunJobStats.get() == null) {
      JobStatistics jobStats = bqServices.getJobService(bqOptions).dryRunQuery(
          bqOptions.getProject(), createBasicQueryConfig());
      dryRunJobStats.compareAndSet(null, jobStats);
    }
    return dryRunJobStats.get();
  }

  private void executeQuery(
      String executingProject,
      String jobId,
      TableReference destinationTable,
      JobService jobService)
      throws InterruptedException, IOException {
    JobReference jobRef = new JobReference()
        .setProjectId(executingProject)
        .setJobId(jobId);

    JobConfigurationQuery queryConfig = createBasicQueryConfig()
        .setAllowLargeResults(true)
        .setCreateDisposition("CREATE_IF_NEEDED")
        .setDestinationTable(destinationTable)
        .setPriority("BATCH")
        .setWriteDisposition("WRITE_EMPTY");

    jobService.startQueryJob(jobRef, queryConfig);
    Job job = jobService.pollJob(jobRef, JOB_POLL_MAX_RETRIES);
    if (BigQueryHelpers.parseStatus(job) != BigQueryHelpers.Status.SUCCEEDED) {
      throw new IOException(String.format(
          "Query job %s failed, status: %s.", jobId,
          BigQueryHelpers.statusToPrettyString(job.getStatus())));
    }
  }

  private JobConfigurationQuery createBasicQueryConfig() {
    return new JobConfigurationQuery()
        .setFlattenResults(flattenResults)
        .setQuery(query.get())
        .setUseLegacySql(useLegacySql);
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    dryRunJobStats = new AtomicReference<>();
  }
}
