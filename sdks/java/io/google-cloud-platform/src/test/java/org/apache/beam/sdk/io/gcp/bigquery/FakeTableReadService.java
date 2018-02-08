package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.bigquery.v3.ParallelRead.CreateSessionRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsResponse;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesV3.TableReadService;

/**
 * A fake implementation of {@link TableReadService} for testing.
 */
public class FakeTableReadService implements TableReadService, Serializable {

  private CreateSessionRequest createSessionRequest;
  private Session session;
  private ReadRowsRequest readRowsRequest;
  private List<ReadRowsResponse> readRowsResponses;

  FakeTableReadService withCreateSessionResult(
      CreateSessionRequest createSessionRequest, Session session) {
    this.createSessionRequest = createSessionRequest;
    this.session = session;
    return this;
  }

  FakeTableReadService withReadRowsResponses(
      ReadRowsRequest readRowsRequest, List<ReadRowsResponse> readRowsResponses) {
    this.readRowsRequest = readRowsRequest;
    this.readRowsResponses = new ArrayList<>(readRowsResponses);
    return this;
  }

  @Override
  public Session createSession(CreateSessionRequest request) {
    assertThat(request, equalTo(createSessionRequest));
    return session;
  }

  @Override
  public Iterator<ReadRowsResponse> readRows(ReadRowsRequest request) {
    assertThat(request, equalTo(readRowsRequest));
    return readRowsResponses.iterator();
  }
}
