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
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

/** An interface for real, mock, or fake implementations of Cloud BigQuery services. */
interface BigQueryServicesV3 extends Serializable {

  TableReadService getTableReadService(GcpOptions options) throws IOException;

  interface TableReadService {

    /**
     * Creates a new read session against an existing table.
     */
    Session createSession(CreateSessionRequest request);

    /**
     * Initiates a read stream from an existing session and read location.
     */
    Iterator<ReadRowsResponse> readRows(ReadRowsRequest request);
  }
}
