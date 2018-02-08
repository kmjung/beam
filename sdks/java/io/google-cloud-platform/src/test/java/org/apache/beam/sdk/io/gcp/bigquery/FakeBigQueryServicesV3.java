package org.apache.beam.sdk.io.gcp.bigquery;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

/**
 * Fake {@link BigQueryServicesV3} implementation for test.
 */
public class FakeBigQueryServicesV3 implements BigQueryServicesV3 {

  private TableReadService tableReadService;

  FakeBigQueryServicesV3 withTableReadService(TableReadService tableReadService) {
    this.tableReadService = tableReadService;
    return this;
  }

  @Override
  public TableReadService getTableReadService(GcpOptions options) {
    return tableReadService;
  }
}
