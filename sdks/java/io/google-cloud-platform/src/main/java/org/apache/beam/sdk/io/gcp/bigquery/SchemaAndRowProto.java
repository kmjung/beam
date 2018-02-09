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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.v3.RowOuterClass;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;


/**
 * A wrapper for a {@link Row} and the {@link StructType} representing the schema of the
 * table it was generated from.
 * Also provides function to read the data out.
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

  /**
   * Gets a field value based on field name.
   * Note: The implementation of this function will convert the entire row to {@link TableRow}
   * and use the field name to get the value out of TableRow, based on the assumption that the
   * most efficient way of accessing the subset of fields is to pass down the Field Selections
   * to the read API.
   *
   * @param fieldName
   * @return
   */
  public Object get(String fieldName) {
    TableRow currentRow = getTableRow();
    String[] subFieldNames = fieldName.split(".");
    for (int i = 0; i < subFieldNames.length; i++) {
      if (i == subFieldNames.length - 1) {
        return currentRow.get(subFieldNames[i]);
      } else {
        // If there are further nested levels, the current field should contain a TableRow.
        currentRow = (TableRow) (currentRow.get(subFieldNames[i]));
      }
    }
    return null;
  }

  /**
   * Converts a {@class Row} that returned from ParallelRead API to {@class TableRow}
   * which is a map of {fieldname, value} to be consumed by Data Flow API.
   */
  public TableRow getTableRow() {
    if (tableRow != null) {
      return tableRow;
    }
    tableRow = new TableRow();
    List<RowOuterClass.Value> rowFields = row.getValue().getFieldsList();

    checkState(rowFields.size() == tableSchema.getFieldsCount(),
        "Expected that the row has the same number of cells %s as"
            + " fields in the schema %s",
        rowFields.size(), tableSchema.getFieldsCount());

    for (int i = 0; i < tableSchema.getFieldsCount(); i++) {
      // Convert the object in this cell to the Java type corresponding to its type in the schema.
      Object convertedValue =
          getFieldValue(tableSchema.getFields(i).getFieldType(), rowFields.get(i));

      String fieldName = tableSchema.getFields(i).getFieldName();

      if (convertedValue == null) {
        // BigQuery does not include null values when the export operation (to JSON) is used.
        // To match that behavior, BigQueryTableRowiterator, and the DirectPipelineRunner,
        // intentionally omits columns with null values.
        continue;
      }

      tableRow.set(fieldName, convertedValue);
    }
    return tableRow;
  }

  /**
   * Convert parallel read field value {@link RowOuterClass.Value} into a object.
   */
  @Nullable
  private Object getFieldValue(RowOuterClass.Type fieldType, RowOuterClass.Value fieldValue) {
    // TODO: This check always evaluates to true, and so it breaks parsing.
    // What's the right way to handle null values?
    /*
    if (fieldValue.getNullValue() == RowOuterClass.NullValue.NULL_VALUE) {
      return null;
    }
    */

    switch (fieldType.getTypeKind()) {
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
        return java.util.Date.parse(fieldValue.getStringValue());
      case TYPE_TIME:
        // TODO: How to parse time?
        return org.joda.time.DateTime.parse(fieldValue.getStringValue());
      case TYPE_DATETIME:
        return org.joda.time.DateTime.parse(fieldValue.getStringValue());
      case TYPE_ARRAY:
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        for (RowOuterClass.Value element : fieldValue.getArrayValue().getElementsList()) {
          values.add(getFieldValue(fieldType.getArrayType(), element));
        }
        return values.build();
      case TYPE_STRUCT:
        TableRow fields = new TableRow();
        Iterator<RowOuterClass.Value> structFieldIterator =
            fieldValue.getStructValue().getFieldsList().iterator();
        for (RowOuterClass.StructField field : fieldType.getStructType().getFieldsList()) {
          fields.set(field.getFieldName(),
              getFieldValue(field.getFieldType(), structFieldIterator.next()));
        }
        return fields;
      default:
        throw new UnsupportedOperationException("Not type for " + fieldType.getTypeKind());
    }
  }
}
