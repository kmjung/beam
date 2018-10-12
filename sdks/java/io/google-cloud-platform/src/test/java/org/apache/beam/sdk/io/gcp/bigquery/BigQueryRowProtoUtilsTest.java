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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.v3.RowOuterClass.ArrayValue;
import com.google.cloud.bigquery.v3.RowOuterClass.NullValue;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructField;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.cloud.bigquery.v3.RowOuterClass.StructValue;
import com.google.cloud.bigquery.v3.RowOuterClass.Type;
import com.google.cloud.bigquery.v3.RowOuterClass.TypeKind;
import com.google.cloud.bigquery.v3.RowOuterClass.Value;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.Test;

/** Tests for {@link BigQueryRowProtoUtils}. */
public class BigQueryRowProtoUtilsTest {

  private StructType tableSchema =
      StructType.newBuilder()
          .addFields(
              StructField.newBuilder()
                  .setFieldName("number")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_INT64)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("species")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_STRING)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("quality")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_FLOAT64)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("quantity")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_INT64)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("birthday")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_TIMESTAMP)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("flighted")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_BOOL)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("sound")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_BYTES)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("anniversaryDate")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_DATE)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("anniversaryDateTime")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_DATETIME)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("anniversaryTime")
                  .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_TIME)))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("associates")
                  .setFieldType(
                      Type.newBuilder()
                          .setTypeKind(TypeKind.TYPE_ARRAY)
                          .setArrayType(Type.newBuilder().setTypeKind(TypeKind.TYPE_STRING))))
          .addFields(
              StructField.newBuilder()
                  .setFieldName("scion")
                  .setFieldType(
                      Type.newBuilder()
                          .setTypeKind(TypeKind.TYPE_STRUCT)
                          .setStructType(
                              StructType.newBuilder()
                                  .addFields(
                                      StructField.newBuilder()
                                          .setFieldName("species")
                                          .setFieldType(
                                              Type.newBuilder().setTypeKind(TypeKind.TYPE_STRING)))
                                  .addFields(
                                      StructField.newBuilder()
                                          .setFieldName("genus")
                                          .setFieldType(
                                              Type.newBuilder()
                                                  .setTypeKind(TypeKind.TYPE_STRING))))))
          .build();

  @Test
  public void testConvertRowProtoToTableRow() throws Exception {

    {
      // Test all fields are nullable.
      Row row =
          Row.newBuilder()
              .setValue(
                  StructValue.newBuilder()
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // number
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // species
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // quality
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // quantity
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // birthday
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // flighted
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // sound
                      .addFields(
                          Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // anniversaryDate
                      .addFields(
                          Value.newBuilder()
                              .setNullValue(NullValue.NULL_VALUE)) // anniversaryDateTime
                      .addFields(
                          Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // anniversaryTime
                      .addFields(
                          Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // associates
                      .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))) // scion
              .build();

      TableRow expectedRow = new TableRow();

      assertEquals(expectedRow, BigQueryRowProtoUtils.convertRowProtoToTableRow(tableSchema, row));
    }

    {
      // Test type conversions.
      byte[] soundBytes = "chirp,chirp".getBytes(UTF_8);
      ByteString byteString = ByteString.copyFrom(soundBytes);

      Row row =
          Row.newBuilder()
              .setValue(
                  StructValue.newBuilder()
                      .addFields(Value.newBuilder().setInt64Value(1)) // number
                      .addFields(Value.newBuilder().setStringValue("chickadee")) // species
                      .addFields(Value.newBuilder().setFloat64Value(10.0)) // quality
                      .addFields(Value.newBuilder().setInt64Value(5)) // quantity
                      .addFields(
                          Value.newBuilder()
                              .setTimestampValue(Timestamp.newBuilder().setNanos(500)))
                      .addFields(Value.newBuilder().setBoolValue(true)) // flighted
                      .addFields(Value.newBuilder().setBytesValue(byteString)) // sound
                      .addFields(Value.newBuilder().setStringValue("2000-01-01")) // anniversaryDate
                      .addFields(Value.newBuilder().setStringValue("2000-01-01 00:00:00.000005"))
                      .addFields(Value.newBuilder().setStringValue("00:00:00.000005"))
                      .addFields(
                          Value.newBuilder()
                              .setArrayValue(
                                  ArrayValue.newBuilder()
                                      .addElements(Value.newBuilder().setStringValue("Tweety"))
                                      .addElements(Value.newBuilder().setStringValue("Woody"))))
                      .addFields(
                          Value.newBuilder()
                              .setStructValue(
                                  StructValue.newBuilder()
                                      .addFields(Value.newBuilder().setStringValue("atricapillus"))
                                      .addFields(Value.newBuilder().setStringValue("poecile")))))
              .build();

      TableRow expectedRow =
          new TableRow()
              .set("number", "1")
              .set("species", "chickadee")
              .set("quality", 10.0D)
              .set("quantity", "5")
              .set("birthday", "1970-01-01 00:00:00.0000005 UTC")
              .set("flighted", Boolean.TRUE)
              .set("sound", BaseEncoding.base64().encode(soundBytes))
              .set("anniversaryDate", "2000-01-01")
              .set("anniversaryDateTime", "2000-01-01 00:00:00.000005")
              .set("anniversaryTime", "00:00:00.000005")
              .set("associates", Lists.newArrayList("Tweety", "Woody"))
              .set("scion", new TableRow().set("species", "atricapillus").set("genus", "poecile"));

      assertEquals(expectedRow, BigQueryRowProtoUtils.convertRowProtoToTableRow(tableSchema, row));
    }
  }
}
