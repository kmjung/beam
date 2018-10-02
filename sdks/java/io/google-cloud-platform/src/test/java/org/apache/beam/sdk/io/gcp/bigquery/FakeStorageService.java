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

import static com.google.common.base.Preconditions.checkState;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.cloud.bigquery.storage.v1alpha1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1alpha1.BigQueryStorageGrpc.BigQueryStorageImplBase;
import com.google.cloud.bigquery.storage.v1alpha1.BigQueryStorageSettings;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.SplitReadStreamResponse;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Fake in-memory implementation of the BigQuery storage service, including a gRPC facade. */
public class FakeStorageService {

  private static class FakeBigQueryStorageService implements MockGrpcService {
    private final FakeBigQueryStorageServiceImpl serviceImpl;

    FakeBigQueryStorageService() {
      serviceImpl = new FakeBigQueryStorageServiceImpl();
    }

    @Override
    public List<GeneratedMessageV3> getRequests() {
      return serviceImpl.getRequests();
    }

    @Override
    public void addResponse(GeneratedMessageV3 response) {
      serviceImpl.addResponse(response);
    }

    public void addResponseUnsafe(Object response) {
      serviceImpl.addResponseUnsafe(response);
    }

    @Override
    public void addException(Exception exception) {
      serviceImpl.addException(exception);
    }

    @Override
    public ServerServiceDefinition getServiceDefinition() {
      return serviceImpl.bindService();
    }

    @Override
    public void reset() {
      serviceImpl.reset();
    }
  }

  private static class FakeBigQueryStorageServiceImpl extends BigQueryStorageImplBase {
    private ArrayList<GeneratedMessageV3> requests;
    private ArrayDeque<Object> responses;

    FakeBigQueryStorageServiceImpl() {
      requests = new ArrayList<>();
      responses = new ArrayDeque<>();
    }

    List<GeneratedMessageV3> getRequests() {
      return requests;
    }

    void addResponse(GeneratedMessageV3 response) {
      responses.add(response);
    }

    void addResponseUnsafe(Object response) {
      responses.add(response);
    }

    void addException(Exception exception) {
      responses.add(exception);
    }

    void reset() {
      requests = new ArrayList<>();
      responses = new ArrayDeque<>();
    }

    @Override
    public void createReadSession(
        CreateReadSessionRequest request, StreamObserver<ReadSession> responseObserver) {
      Object response = responses.remove();
      if (response instanceof ReadSession) {
        requests.add(request);
        responseObserver.onNext((ReadSession) response);
        responseObserver.onCompleted();
      } else if (response instanceof Exception) {
        responseObserver.onError((Exception) response);
      } else {
        responseObserver.onError(new IllegalStateException("Unexpected response type"));
      }
    }

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      Object response = responses.remove();
      if (response instanceof List) {
        requests.add(request);
        for (Object streamResponse : (List) response) {
          if (streamResponse instanceof ReadRowsResponse) {
            responseObserver.onNext((ReadRowsResponse) streamResponse);
          } else if (streamResponse instanceof Exception) {
            responseObserver.onError((Exception) streamResponse);
            return;
          } else {
            responseObserver.onError(new IllegalStateException("Unexpected stream response type"));
            return;
          }
        }
        responseObserver.onCompleted();
      } else if (response instanceof ReadRowsResponse) {
        requests.add(request);
        responseObserver.onNext((ReadRowsResponse) response);
        responseObserver.onCompleted();
      } else if (response instanceof Exception) {
        responseObserver.onError((Exception) response);
      } else {
        responseObserver.onError(new IllegalStateException("Unexpected response type"));
      }
    }

    @Override
    public void splitReadStream(
        SplitReadStreamRequest request, StreamObserver<SplitReadStreamResponse> responseObserver) {
      Object response = responses.remove();
      if (response instanceof SplitReadStreamResponse) {
        requests.add(request);
        responseObserver.onNext((SplitReadStreamResponse) response);
        responseObserver.onCompleted();
      } else if (response instanceof Exception) {
        responseObserver.onError((Exception) response);
      } else {
        responseObserver.onError(new IllegalStateException("Unexpected response type"));
      }
    }
  }

  private static AtomicReference<MockServiceHelper> serviceHelper = new AtomicReference<>();
  private static FakeBigQueryStorageService fakeBigQueryStorageService;

  public static void startServer() {
    fakeBigQueryStorageService = new FakeBigQueryStorageService();
    MockServiceHelper helper =
        new MockServiceHelper("in-memory-1", Arrays.asList(fakeBigQueryStorageService));
    MockServiceHelper oldHelper = serviceHelper.getAndSet(helper);
    checkState(oldHelper == null, "Fake storage service must be a singleton");
    helper.start();
  }

  public static void stopServer() {
    MockServiceHelper helper = serviceHelper.getAndSet(null);
    checkState(helper != null, "Fake storage service not started");
    helper.stop();
  }

  public static void reset() {
    MockServiceHelper helper = serviceHelper.get();
    checkState(helper != null, "Fake storage service not started");
    helper.reset();
  }

  public static BigQueryStorageClient getClient() throws IOException {
    MockServiceHelper helper = serviceHelper.get();
    checkState(helper != null, "Fake storage service not started");
    BigQueryStorageSettings settings =
        BigQueryStorageSettings.newBuilder()
            .setTransportChannelProvider(helper.createChannelProvider())
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();
    return BigQueryStorageClient.create(settings);
  }

  public static void addCreateReadSessionResponse(ReadSession readSession) {
    fakeBigQueryStorageService.addResponse(readSession);
  }

  public static void addReadRowsResponses(Collection<ReadRowsResponse> responses) {
    fakeBigQueryStorageService.addResponseUnsafe(responses);
  }

  public static List<GeneratedMessageV3> getRequests() {
    return fakeBigQueryStorageService.getRequests();
  }
}
