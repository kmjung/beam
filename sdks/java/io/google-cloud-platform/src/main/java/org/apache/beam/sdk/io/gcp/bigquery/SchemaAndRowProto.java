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
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructField;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.cloud.bigquery.v3.RowOuterClass.StructValue;
import com.google.cloud.bigquery.v3.RowOuterClass.TypeKind;
import com.google.cloud.bigquery.v3.RowOuterClass.Value;
import com.google.cloud.bigquery.v3.RowOuterClass.Value.ValueCase;
import com.google.common.base.Splitter;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A wrapper for a {@link Row} and the {@link StructType} representing the schema of the table it
 * was generated from. Also provides utility methods to extract data.
 */
public class SchemaAndRowProto {
  private final StructType tableSchema;
  private final Row row;
  TableRow tableRow;

  public SchemaAndRowProto(StructType tableSchema, Row row) {
    this.tableSchema = checkNotNull(tableSchema, "tableSchema");
    this.row = checkNotNull(row, "row");
  }

  public Row getRow() {
    return row;
  }

  public StructType getSchema() {
    return tableSchema;
  }

  /** Gets the value of the specified (possibly nested) field or null if it can't be found. */
  @Nullable
  public Object get(String fieldName) {
    List<String> subFieldNames = Splitter.on('.').splitToList(fieldName);
    StructType structType = tableSchema;
    StructValue structValue = row.getValue();

    for (int i = 0; i < subFieldNames.size() - 1; i++) {
      int fieldIndex = getFieldIndex(structType, subFieldNames.get(i));
      if (fieldIndex == -1) {
        return null;
      }
      StructField structField = structType.getFields(fieldIndex);
      if (structField.getFieldType().getTypeKind() != TypeKind.TYPE_STRUCT) {
        return null;
      }

      Value fieldValue = structValue.getFields(fieldIndex);
      if (fieldValue.getValueCase() == ValueCase.NULL_VALUE) {
        return null;
      }

      structType = structField.getFieldType().getStructType();
      structValue = fieldValue.getStructValue();
    }

    String leafFieldName = subFieldNames.get(subFieldNames.size() - 1);
    int fieldIndex = getFieldIndex(structType, leafFieldName);
    if (fieldIndex == -1) {
      return null;
    }

    StructField leafField = structType.getFields(fieldIndex);
    Value fieldValue = structValue.getFields(fieldIndex);
    if (fieldValue.getValueCase() == ValueCase.NULL_VALUE) {
      return null;
    }

    switch (leafField.getFieldType().getTypeKind()) {
      case TYPE_INT64:
        return fieldValue.getInt64Value();
      case TYPE_BOOL:
        return fieldValue.getBoolValue();
      case TYPE_FLOAT64:
        return fieldValue.getFloat64Value();
      case TYPE_STRING:
        return fieldValue.getStringValue();
      case TYPE_BYTES:
        return fieldValue.getBytesValue();
      case TYPE_TIMESTAMP:
        return fieldValue.getTimestampValue();
      case TYPE_DATE:
        return fieldValue.getStringValue();
      case TYPE_TIME:
        return fieldValue.getStringValue();
      case TYPE_DATETIME:
        return fieldValue.getStringValue();
      case TYPE_ARRAY:
        return fieldValue.getArrayValue();
      case TYPE_STRUCT:
        return fieldValue.getStructValue();
      default:
        return null;
    }
  }

  private int getFieldIndex(StructType structType, String fieldName) {
    for (int i = 0; i < structType.getFieldsCount(); i++) {
      StructField field = structType.getFields(i);
      if (field.getFieldName().equals(fieldName)) {
        return i;
      }
    }
    return -1;
  }
}
