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
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryQueryHelper implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQueryHelper.class);

  // The maximum number of polling attempts while waiting for a query job.
  private static final int JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  private final ValueProvider<String> query;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final QueryPriority priority;
  private final String location;

  private transient AtomicReference<JobStatistics> dryRunJobStats;

  BigQueryQueryHelper(
      ValueProvider<String> query,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      String location) {
    this.query = checkNotNull(query, "query");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.priority = priority;
    this.location = location;
    this.dryRunJobStats = new AtomicReference<>();
  }

  String getQuery() {
    return query.get();
  }

  synchronized JobStatistics dryRunQueryIfNeeded(
      BigQueryServices bqServices, BigQueryOptions bqOptions)
      throws InterruptedException, IOException {
    if (dryRunJobStats.get() == null) {
      JobStatistics jobStats =
          bqServices
              .getJobService(bqOptions)
              .dryRunQuery(bqOptions.getProject(), createBaseQueryConfig(), location);
      dryRunJobStats.compareAndSet(null, jobStats);
    }

    return dryRunJobStats.get();
  }

  TableReference executeQuery(
      BigQueryServices bqServices, BigQueryOptions bqOptions, String stepUuid)
      throws IOException, InterruptedException {
    // 1. Find the location in which the query will be executed.
    List<TableReference> referencedTables =
        dryRunQueryIfNeeded(bqServices, bqOptions).getQuery().getReferencedTables();

    String location = null;
    DatasetService datasetService = bqServices.getDatasetService(bqOptions);
    if (referencedTables != null && !referencedTables.isEmpty()) {
      TableReference queryResultTable = referencedTables.get(0);
      location = datasetService.getTable(queryResultTable).getLocation();
    }

    // 2. Create the temporary dataset in the appropriate location.
    TableReference resultTable =
        createTempTableReference(
            bqOptions.getProject(), createJobIdToken(bqOptions.getJobName(), stepUuid));

    LOG.info("Creating temporary dataset {} for query results", resultTable.getDatasetId());

    datasetService.createDataset(
        resultTable.getProjectId(),
        resultTable.getDatasetId(),
        location,
        "Temporary tables for query results of job " + bqOptions.getJobName(),
        TimeUnit.DAYS.toMillis(1));

    // 3. Execute the query, writing the results to the temporary table.
    String queryJobId = createJobIdToken(bqOptions.getJobName(), stepUuid) + "-query";

    LOG.info(
        "Exporting query results into temporary table {} using job {}", resultTable, queryJobId);

    JobReference jobReference =
        new JobReference().setProjectId(bqOptions.getProject()).setJobId(queryJobId);

    JobConfigurationQuery jobConfiguration =
        createBaseQueryConfig()
            .setAllowLargeResults(true)
            .setCreateDisposition("CREATE_IF_NEEDED")
            .setDestinationTable(resultTable)
            .setPriority("BATCH")
            .setWriteDisposition("WRITE_EMPTY");

    JobService jobService = bqServices.getJobService(bqOptions);
    jobService.startQueryJob(jobReference, jobConfiguration);
    Job job = jobService.pollJob(jobReference, JOB_POLL_MAX_RETRIES);
    if (BigQueryHelpers.parseStatus(job) != Status.SUCCEEDED) {
      throw new IOException(
          String.format(
              "Query job %s failed, status: %s.",
              queryJobId, BigQueryHelpers.statusToPrettyString(job.getStatus())));
    }

    return resultTable;
  }

  private JobConfigurationQuery createBaseQueryConfig() {
    return new JobConfigurationQuery()
        .setQuery(query.get())
        .setFlattenResults(flattenResults)
        .setUseLegacySql(useLegacySql);
  }
}
