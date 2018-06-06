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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.bigquery.v3.ParallelRead.CreateSessionRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsResponse;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import com.google.cloud.bigquery.v3.ReadOptions;
import com.google.cloud.bigquery.v3.TableReferenceProto;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.TableReadService;

/**
 * A fake implementation of {@link TableReadService} for testing.
 */
public class FakeTableReadService implements TableReadService, Serializable {

  private CreateSessionRequest createSessionRequest;
  private Session session;
  private ReadRowsRequest readRowsRequest;
  private List<ReadRowsResponse> readRowsResponses;

  void setCreateSessionResult(CreateSessionRequest createSessionRequest, Session session) {
    this.createSessionRequest = createSessionRequest;
    this.session = session;
  }

  void setReadRowsResponses(
      ReadRowsRequest readRowsRequest, List<ReadRowsResponse> readRowsResponses) {
    this.readRowsRequest = readRowsRequest;
    this.readRowsResponses = new ArrayList<>(readRowsResponses);
  }

  @Override
  public Session createSession(CreateSessionRequest request) {

    if (createSessionRequest.hasTableReference()) {
      TableReferenceProto.TableReference tableReference = createSessionRequest.getTableReference();
      if (!Strings.isNullOrEmpty(tableReference.getProjectId())) {
        assertThat(request.getTableReference().getProjectId(), is(tableReference.getProjectId()));
      }
      if (!Strings.isNullOrEmpty(tableReference.getDatasetId())) {
        assertThat(request.getTableReference().getDatasetId(), is(tableReference.getDatasetId()));
      }
      if (!Strings.isNullOrEmpty(tableReference.getTableId())) {
        assertThat(request.getTableReference().getTableId(), is(tableReference.getTableId()));
      }
    }

    if (!Strings.isNullOrEmpty(createSessionRequest.getBillableProjectId())) {
      assertThat(request.getBillableProjectId(), is(createSessionRequest.getBillableProjectId()));
    }

    if (createSessionRequest.hasReadOptions()) {
      ReadOptions.TableReadOptions readOptions = createSessionRequest.getReadOptions();
      if (readOptions.getSelectedFieldsCount() > 0) {
        assertThat(readOptions.getSelectedFieldsList(), is(readOptions.getSelectedFieldsList()));
      }
      if (!Strings.isNullOrEmpty(readOptions.getSqlFilter())) {
        assertThat(readOptions.getSqlFilter(), is(readOptions.getSqlFilter()));
      }
    }

    return session;
  }

  @Override
  public Iterator<ReadRowsResponse> readRows(ReadRowsRequest request) {
    assertThat(request, equalTo(readRowsRequest));
    return readRowsResponses.iterator();
  }
}
