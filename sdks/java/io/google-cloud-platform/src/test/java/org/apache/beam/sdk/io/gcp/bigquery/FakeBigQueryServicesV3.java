package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.v3.ParallelReadServiceClient;
import com.google.cloud.bigquery.v3.stub.ParallelReadServiceStub;
import java.io.IOException;
import java.io.Serializable;

/**
 * Fake {@link BigQueryServicesV3} implementation for test.
 */
public class FakeBigQueryServicesV3 implements BigQueryServicesV3, Serializable {
  transient ParallelReadServiceStub stub;

  FakeBigQueryServicesV3(ParallelReadServiceStub mockStub) {
    stub = mockStub;
  }

  /**
   * Get ParallelReadService client.
   * @return
   * @throws IOException
   */
  public ParallelReadServiceClient getParallelReadService() throws IOException {
    return ParallelReadServiceClient.create(stub);
  }
}
