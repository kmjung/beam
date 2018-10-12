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

import com.google.cloud.bigquery.storage.v1alpha1.ReadOptions;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.v3.TableReferenceProto;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.TableReadService;

/** A fake implementation of {@link TableReadService} for testing. */
public class FakeTableReadService implements TableReadService, Serializable {

  private CreateReadSessionRequest createReadSessionRequest;
  private ReadSession readSession;
  private ReadRowsRequest readRowsRequest;
  private List<ReadRowsResponse> readRowsResponses;

  void setCreateSessionResult(
      CreateReadSessionRequest createSessionRequest, ReadSession readSession) {
    this.createReadSessionRequest = createSessionRequest;
    this.readSession = readSession;
  }

  void setReadRowsResponses(
      ReadRowsRequest readRowsRequest, List<ReadRowsResponse> readRowsResponses) {
    this.readRowsRequest = readRowsRequest;
    this.readRowsResponses = new ArrayList<>(readRowsResponses);
  }

  @Override
  public ReadSession createSession(CreateReadSessionRequest request) {

    if (createReadSessionRequest.hasTableReference()) {
      TableReferenceProto.TableReference tableReference =
          createReadSessionRequest.getTableReference();
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

    if (!Strings.isNullOrEmpty(createReadSessionRequest.getBillableProjectId())) {
      assertThat(
          request.getBillableProjectId(), is(createReadSessionRequest.getBillableProjectId()));
    }

    if (createReadSessionRequest.hasReadOptions()) {
      ReadOptions.TableReadOptions readOptions = createReadSessionRequest.getReadOptions();
      if (readOptions.getSelectedFieldsCount() > 0) {
        assertThat(readOptions.getSelectedFieldsList(), is(readOptions.getSelectedFieldsList()));
      }
      if (!Strings.isNullOrEmpty(readOptions.getFilter())) {
        assertThat(readOptions.getFilter(), is(readOptions.getFilter()));
      }
    }

    return readSession;
  }

  @Override
  public Iterator<ReadRowsResponse> readRows(ReadRowsRequest request) {
    assertThat(request, equalTo(readRowsRequest));
    return readRowsResponses.iterator();
  }
}
