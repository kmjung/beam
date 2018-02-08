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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.v3.ParallelRead;
import com.google.cloud.bigquery.v3.ParallelRead.ReadLocation;
import com.google.cloud.bigquery.v3.ParallelRead.ReadOptions;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsResponse;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.transforms.SerializableFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Iterates over all rows in a table.
 */
class BigQueryV3Reader<T> extends BoundedSource.BoundedReader<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryV3Reader.class);

  private Session session;
  private final BigQueryServicesV3 client;
  private Iterator<TableRow> iteratorOverCurrentBatch;
  private Iterator<ReadRowsResponse> responseIterator;
  private Iterator<Row> rowIterator;
  private final ReadRowsRequest request;
  private SerializableFunction<SchemaAndRowProto, T> parseFn;
  private BoundedSource<T> source;
  private GcpOptions options;
  Row currentRow;

  private BigQueryV3Reader(Session session,
                           ReadLocation location,
                           int readBatch,
                           SerializableFunction<SchemaAndRowProto, T> parseFn,
                           Coder<T> coder,
                           BoundedSource<T> source,
                           BigQueryServicesV3 client,
                           GcpOptions options) {
    this.session = checkNotNull(session, "session");
    this.client = checkNotNull(client, "client");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.source = checkNotNull(source, "source");
    this.request = ReadRowsRequest.newBuilder()
        .setReadLocation(location)
        .setOptions(ReadOptions.newBuilder().setMaxRows(readBatch).build()).build();
    this.options = options;
  }

  public static <T> BigQueryV3Reader create(Session session,
                                            ReadLocation location,
                                            SerializableFunction<SchemaAndRowProto, T> parseFn,
                                            Coder<T> coder,
                                            BoundedSource<T> source,
                                            BigQueryServicesV3 client,
                                            GcpOptions options) {
    return new BigQueryV3Reader(session, location, 1000,
        parseFn, coder, source, client, options);
  }
  /**
   * Empty operation, the table is already open for read.
   * @throws IOException on failure
   */
  @Override
  public boolean start() throws IOException {
    responseIterator = client.getTableReadService(options).readRows(request);
    if (responseIterator != null && responseIterator.hasNext()) {
      rowIterator = responseIterator.next().getRowsList().iterator();
      if (rowIterator.hasNext()) {
        currentRow = rowIterator.next();
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean advance() throws IOException {
    if (responseIterator == null) {
      throw new IOException("start() needs to be called first");
    }
    if (rowIterator != null && rowIterator.hasNext()) {
      currentRow = rowIterator.next();
      return true;
    }
    if (rowIterator == null || !rowIterator.hasNext()) {
      if (responseIterator.hasNext()) {
        rowIterator = responseIterator.next().getRowsList().iterator();
        if (rowIterator.hasNext()) {
          currentRow = rowIterator.next();
          return true;
        }
      }
    }
    currentRow = null;
    return false;
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
    // How to close a stream from client?
  }

  @Override
  public BoundedSource<T> getCurrentSource() {
    return source;
  }
}

