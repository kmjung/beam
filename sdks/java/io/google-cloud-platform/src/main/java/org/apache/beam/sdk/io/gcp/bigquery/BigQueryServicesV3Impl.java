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

import com.google.cloud.bigquery.v3.ParallelRead.CreateSessionRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsResponse;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import com.google.cloud.bigquery.v3.ParallelReadServiceClient;
import com.google.cloud.bigquery.v3.ParallelReadServiceSettings;
import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

/**
 * An implementation of {@link BigQueryServicesV3} that communicates with the cloud BigQuery
 * service v3 APIs.
 */
class BigQueryServicesV3Impl implements BigQueryServicesV3 {

  @Override
  public TableReadService getTableReadService(GcpOptions options) throws IOException {
    return new TableReadServiceImpl(options);
  }

  static class TableReadServiceImpl implements TableReadService {

    private ParallelReadServiceClient client;

    private TableReadServiceImpl(GcpOptions options) throws IOException {
      ParallelReadServiceSettings settings = ParallelReadServiceSettings.newBuilder()
          .setCredentialsProvider(new GcpCredentialsProvider(options))
          .build();
      this.client = ParallelReadServiceClient.create(settings);
    }

    @Override
    public Session createSession(CreateSessionRequest request) {
      return client.createSession(request);
    }

    @Override
    public Iterator<ReadRowsResponse> readRows(ReadRowsRequest request) {
      return client.readRowsCallable().blockingServerStreamingCall(request);
    }
  }

}
