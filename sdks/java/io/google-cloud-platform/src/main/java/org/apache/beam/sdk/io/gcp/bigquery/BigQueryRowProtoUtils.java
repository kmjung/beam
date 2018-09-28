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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructField;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.cloud.bigquery.v3.RowOuterClass.StructValue;
import com.google.cloud.bigquery.v3.RowOuterClass.Type;
import com.google.cloud.bigquery.v3.RowOuterClass.Value;
import com.google.cloud.bigquery.v3.RowOuterClass.Value.ValueCase;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A set of utilities for working with {@link Row} objects. Analogous to {@link BigQueryAvroUtils}.
 */
public class BigQueryRowProtoUtils {

  static TableRow convertRowProtoToTableRow(StructType tableSchema, Row row) {
    return getStructValueAsTableRow(tableSchema, row.getValue());
  }

  private static TableRow getStructValueAsTableRow(StructType structType, StructValue structValue) {
    List<StructField> structFieldTypes = structType.getFieldsList();
    List<Value> structFieldValues = structValue.getFieldsList();

    checkState(
        structFieldTypes.size() == structFieldValues.size(),
        "Struct value %s had unexpected field count %d (schema had %d fields)",
        structValue,
        structFieldValues.size(),
        structFieldTypes.size());

    TableRow tableRow = new TableRow();
    for (int i = 0; i < structFieldTypes.size(); i++) {
      Object fieldValue =
          getFieldValueAsTableRowType(
              structFieldTypes.get(i).getFieldType(), structFieldValues.get(i));
      if (fieldValue != null) {
        tableRow.set(structFieldTypes.get(i).getFieldName(), fieldValue);
      }
    }

    return tableRow;
  }

  @Nullable
  private static Object getFieldValueAsTableRowType(Type fieldType, Value fieldValue) {
    if (fieldValue.getValueCase() == ValueCase.NULL_VALUE) {
      return null;
    }

    switch (fieldType.getTypeKind()) {
      case TYPE_INT64:
        return Long.toString(fieldValue.getInt64Value());
      case TYPE_BOOL:
        return fieldValue.getBoolValue();
      case TYPE_FLOAT64:
        return fieldValue.getFloat64Value();
      case TYPE_STRING:
        return fieldValue.getStringValue();
      case TYPE_BYTES:
        return BaseEncoding.base64().encode(fieldValue.getBytesValue().toByteArray());
      case TYPE_TIMESTAMP:
        return formatTimestamp(fieldValue.getTimestampValue());
      case TYPE_DATE:
        // The date is returned in GoogleSQL date format, which may be incompatible with the Avro-
        // format date that callers expect.
        return fieldValue.getStringValue();
      case TYPE_TIME:
        // The time is returned in GoogleSQL time format, which may be incompatible with the Avro-
        // format time that callers expect.
        return fieldValue.getStringValue();
      case TYPE_DATETIME:
        // The time is returned in GoogleSQL datetime format, which may be incompatible with the
        // Avro-format datetime string that callers expect.
        return fieldValue.getStringValue();
      case TYPE_ARRAY:
        ImmutableList.Builder<Object> listBuilder = ImmutableList.builder();
        for (Value element : fieldValue.getArrayValue().getElementsList()) {
          Object convertedElement = getFieldValueAsTableRowType(fieldType.getArrayType(), element);
          if (convertedElement != null) {
            listBuilder.add(convertedElement);
          }
        }
        return listBuilder.build();
      case TYPE_STRUCT:
        return getStructValueAsTableRow(fieldType.getStructType(), fieldValue.getStructValue());
      default:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType.getTypeKind());
    }
  }

  private static final DateTimeFormatter DATE_AND_SECONDS_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

  private static String formatTimestamp(Timestamp timestamp) {
    long millisecondInstant = TimeUnit.SECONDS.toMillis(timestamp.getSeconds());
    String dayAndTime = DATE_AND_SECONDS_FORMATTER.print(millisecondInstant);
    long nanos = timestamp.getNanos();
    if (nanos == 0) {
      return String.format("%s UTC", dayAndTime);
    }

    int digits = 9;
    long subsecond = nanos;
    while (subsecond % 10 == 0) {
      digits--;
      subsecond /= 10;
    }

    String formatString = String.format("%%0%dd", digits);
    String fractionalSeconds = String.format(formatString, subsecond);
    return String.format("%s.%s UTC", dayAndTime, fractionalSeconds);
  }
}
