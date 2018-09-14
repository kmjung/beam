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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mock implementation of the BigQuery storage service.
 */
public class MockBigQueryStorageService implements Serializable {

  /**
   * A mock GRPC service which wraps a BigQuery storage service implementation.
   */
  private static class MockBigQueryStorage implements MockGrpcService, Serializable {
    private final MockBigQueryStorageImpl serviceImpl;

    public MockBigQueryStorage() {
      serviceImpl = new MockBigQueryStorageImpl();
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

  private static class MockBigQueryStorageImpl
      extends BigQueryStorageImplBase implements Serializable {
    private ArrayList<GeneratedMessageV3> requests;
    private Queue<Object> responses;

    public MockBigQueryStorageImpl() {
      requests = new ArrayList<>();
      responses = new LinkedList<>();
    }

    public List<GeneratedMessageV3> getRequests() {
      return requests;
    }

    public void addResponse(GeneratedMessageV3 response) {
      responses.add(response);
    }

    public void addResponseUnsafe(Object response) {
      responses.add(response);
    }

    public void addException(Exception exception) {
      responses.add(exception);
    }

    public void reset() {
      requests = new ArrayList<>();
      responses = new LinkedList<>();
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
        List responseList = (List) response;
        for (Object streamResponse : responseList) {
          if (streamResponse instanceof ReadRowsResponse) {
            responseObserver.onNext((ReadRowsResponse) streamResponse);
          } else if (streamResponse instanceof Exception) {
            responseObserver.onError((Exception) streamResponse);
            return;
          } else {
            responseObserver.onError(new IllegalStateException("Unexpected response type"));
            return;
          }
        }
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

  private MockBigQueryStorage mockBigQueryStorage;

  public void startServer() {
    mockBigQueryStorage = new MockBigQueryStorage();
    MockServiceHelper helper =
        new MockServiceHelper("in-process-1", Arrays.asList(mockBigQueryStorage));
    MockServiceHelper oldHelper = serviceHelper.getAndSet(helper);
    checkState(oldHelper == null, "Mock storage service must be a singleton");
    helper.start();
  }

  public void stopServer() {
    MockServiceHelper helper = serviceHelper.getAndSet(null);
    checkState(helper != null, "No service helper instance");
    helper.stop();
  }

  public void reset() {
    serviceHelper.get().reset();
  }

  public BigQueryStorageClient newClient() throws IOException {

    checkState(serviceHelper != null, "Call startStaticServer first");

    BigQueryStorageSettings settings =
        BigQueryStorageSettings.newBuilder()
            .setTransportChannelProvider(serviceHelper.get().createChannelProvider())
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    return BigQueryStorageClient.create(settings);
  }

  public void addCreateSessionResponse(ReadSession response) {
    mockBigQueryStorage.addResponse(ReadSession.newBuilder(response).build());
  }

  public void addReadRowsResponses(Collection<ReadRowsResponse> responses) {
    List<ReadRowsResponse> allocatedResponses = new ArrayList<>(responses.size());
    for (ReadRowsResponse response : responses) {
      allocatedResponses.add(ReadRowsResponse.newBuilder(response).build());
    }
    mockBigQueryStorage.addResponseUnsafe(allocatedResponses);
  }

  public List<GeneratedMessageV3> getRequests() {
    return mockBigQueryStorage.getRequests();
  }
}
