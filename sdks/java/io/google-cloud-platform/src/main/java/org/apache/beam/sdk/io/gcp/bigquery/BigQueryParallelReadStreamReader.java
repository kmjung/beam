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

import com.google.cloud.bigquery.v3.ParallelRead.ReadLocation;
import com.google.cloud.bigquery.v3.ParallelRead.ReadOptions;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsResponse;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over all rows assigned to a particular reader in a read session.
 */
class BigQueryParallelReadStreamReader<T> extends BoundedSource.BoundedReader<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryParallelReadStreamReader.class);

  private Session session;
  private final BigQueryServices client;
  private Iterator<ReadRowsResponse> responseIterator;
  private Iterator<Row> rowIterator;
  private final ReadRowsRequest request;
  private SerializableFunction<SchemaAndRowProto, T> parseFn;
  private BoundedSource<T> source;
  private BigQueryOptions options;
  private Row currentRow;

  BigQueryParallelReadStreamReader(
      Session session,
      ReadLocation readLocation,
      Integer rowBatchSize,
      SerializableFunction<SchemaAndRowProto, T> parseFn,
      BoundedSource<T> source,
      BigQueryServices client,
      BigQueryOptions options) {
    this.session = checkNotNull(session, "session");
    this.client = checkNotNull(client, "client");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.source = checkNotNull(source, "source");
    this.options = options;

    this.request = ReadRowsRequest.newBuilder()
        .setReadLocation(readLocation)
        .setOptions(ReadOptions.newBuilder()
            .setMaxRows(checkNotNull(rowBatchSize, "rowBatchSize")))
        .build();
  }

  /**
   * Empty operation, the table is already open for read.
   * @throws IOException on failure
   */
  @Override
  public boolean start() throws IOException {
    responseIterator = client.getTableReadService(options).readRows(request);
    return advance();
  }

  @Override
  public boolean advance() {
    if (rowIterator == null || !rowIterator.hasNext()) {
      if (!responseIterator.hasNext()) {
        currentRow = null;
        return false;
      }
      rowIterator = responseIterator.next().getRowsList().iterator();
    }
    currentRow = rowIterator.next();
    return true;
  }

  @Override
  public T getCurrent() {
    if (currentRow == null) {
      return null;
    }
    return parseFn.apply(new SchemaAndRowProto(session.getProjectedSchema(), currentRow));
  }

  @Override
  public void close() {
    // Do nothing.
  }

  @Override
  public BoundedSource<T> getCurrentSource() {
    return source;
  }
}

