package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.bigquery.v3.RowOuterClass.ArrayValue;
import com.google.cloud.bigquery.v3.RowOuterClass.NullValue;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructField;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.cloud.bigquery.v3.RowOuterClass.StructValue;
import com.google.cloud.bigquery.v3.RowOuterClass.Type;
import com.google.cloud.bigquery.v3.RowOuterClass.TypeKind;
import com.google.cloud.bigquery.v3.RowOuterClass.Value;
import org.junit.Test;

/**
 * Tests for {@link SchemaAndRowProto}.
 */
public class SchemaAndRowProtoTest {

  private StructType defaultSchema = StructType.newBuilder()
      .addFields(StructField.newBuilder().setFieldName("simpleField")
          .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_INT64)))
      .addFields(StructField.newBuilder().setFieldName("arrayField")
          .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_ARRAY)
              .setArrayType(Type.newBuilder().setTypeKind(TypeKind.TYPE_INT64))))
      .addFields(StructField.newBuilder().setFieldName("structField")
          .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_STRUCT)
              .setStructType(StructType.newBuilder()
                  .addFields(StructField.newBuilder().setFieldName("nestedField")
                      .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_INT64))))))
      .build();

  private Row defaultRow = Row.newBuilder().setValue(StructValue.newBuilder()
      .addFields(Value.newBuilder().setInt64Value(5L))
      .addFields(Value.newBuilder().setArrayValue(
          ArrayValue.newBuilder()
              .addElements(Value.newBuilder().setInt64Value(6L))
              .addElements(Value.newBuilder().setInt64Value(7L))))
      .addFields(Value.newBuilder().setStructValue(
          StructValue.newBuilder()
              .addFields(Value.newBuilder().setInt64Value(8L)))))
      .build();

  @Test
  public void testGetSimpleField() {
    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertEquals(5L, schemaAndRowProto.get("simpleField"));
  }

  @Test
  public void testGetArrayField() {

    ArrayValue expected = ArrayValue.newBuilder()
        .addElements(Value.newBuilder().setInt64Value(6L))
        .addElements(Value.newBuilder().setInt64Value(7L))
        .build();

    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertEquals(expected, schemaAndRowProto.get("arrayField"));
  }

  @Test
  public void testGetStructField() {

    StructValue expected = StructValue.newBuilder()
        .addFields(Value.newBuilder().setInt64Value(8L))
        .build();

    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertEquals(expected, schemaAndRowProto.get("structField"));
  }

  @Test
  public void testGetNestedField() {
    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertEquals(8L, schemaAndRowProto.get("structField.nestedField"));
  }

  @Test
  public void testGetMissingField() {
    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertNull(schemaAndRowProto.get("missingField"));
  }

  @Test
  public void testGetMissingNestedField() {
    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertNull(schemaAndRowProto.get("structField.missingNestedField"));
  }

  @Test
  public void testGetMissingStructField() {
    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertNull(schemaAndRowProto.get("missingField.nestedField"));
  }

  @Test
  public void testGetMalformedNestedField() {
    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, defaultRow);
    assertNull(schemaAndRowProto.get("simpleField.nestedField"));
  }

  @Test
  public void testNullFields() {

    Row nullRow = Row.newBuilder().setValue(StructValue.newBuilder()
        .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
        .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
        .addFields(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)))
        .build();

    SchemaAndRowProto schemaAndRowProto = new SchemaAndRowProto(defaultSchema, nullRow);
    assertNull(schemaAndRowProto.get("simpleField"));
    assertNull(schemaAndRowProto.get("arrayField"));
    assertNull(schemaAndRowProto.get("structField"));
    assertNull(schemaAndRowProto.get("structField.nestedField"));
  }
}
