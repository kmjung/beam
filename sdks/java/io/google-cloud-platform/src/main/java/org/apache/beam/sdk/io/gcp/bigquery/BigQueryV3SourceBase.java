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
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link BoundedSource} to read a table from BigQuery using V3 APIs.
 */
abstract class BigQueryV3SourceBase<T> extends BoundedSource<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryV3SourceBase.class);
  protected final BigQueryServices bqServices;
  protected final BigQueryServicesV3 bqServicesV3;

  protected transient List<BoundedSource<T>> cachedSplitResult;
  protected SerializableFunction<SchemaAndRowProto, T> parseFn;
  protected Coder<T> coder;

  BigQueryV3SourceBase(
      BigQueryServices bqServices,
      BigQueryServicesV3 bqServicesV3,
      Coder<T> coder,
      SerializableFunction<SchemaAndRowProto, T> parseFn) {
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.bqServicesV3 = checkNotNull(bqServicesV3, "bqServicesV3");
    this.coder = checkNotNull(coder, "coder");
    this.parseFn = checkNotNull(parseFn, "parseFn");
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    throw new UnsupportedOperationException("BigQuery source must be split before being read");
  }

  @Override
  public void validate() {
    // Do nothing, validation is done in BigQuery.Read.
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }

  public BigQueryServicesV3 getBqServicesV3() {
    return bqServicesV3;
  }
}
