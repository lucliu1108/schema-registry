/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.type;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Converts a Jackson {@link JsonNode} to a {@link Variant} (metadata + value binary pair).
 */
public class VariantJsonConverter {

  /**
   * Converts a Jackson JsonNode into a Variant.
   *
   * @param node the JSON node to convert
   * @return a Variant containing the encoded metadata and value
   */
  public static Variant toVariant(JsonNode node) {
    VariantBuilder builder = new VariantBuilder();
    buildValue(builder, node);
    return builder.build();
  }

  private static void buildValue(VariantBuilder builder, JsonNode node) {
    switch (node.getNodeType()) {
      case OBJECT:
        buildObject(builder, node);
        break;
      case ARRAY:
        buildArray(builder, node);
        break;
      case STRING:
        builder.appendString(node.textValue());
        break;
      case NUMBER:
        buildNumber(builder, node);
        break;
      case BOOLEAN:
        builder.appendBoolean(node.booleanValue());
        break;
      case NULL:
        builder.appendNull();
        break;
      default:
        throw new IllegalArgumentException("Unsupported JSON node type: " + node.getNodeType());
    }
  }

  private static void buildObject(VariantBuilder builder, JsonNode node) {
    VariantObjectBuilder obj = builder.startObject();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      obj.appendKey(field.getKey());
      buildValue(obj, field.getValue());
    }
    builder.endObject();
  }

  private static void buildArray(VariantBuilder builder, JsonNode node) {
    VariantArrayBuilder arr = builder.startArray();
    for (JsonNode element : node) {
      buildValue(arr, element);
    }
    builder.endArray();
  }

  private static void buildNumber(VariantBuilder builder, JsonNode node) {
    if (node.isInt()) {
      builder.appendInt(node.intValue());
    } else if (node.isLong()) {
      builder.appendLong(node.longValue());
    } else if (node.isFloat()) {
      builder.appendFloat(node.floatValue());
    } else if (node.isDouble()) {
      builder.appendDouble(node.doubleValue());
    } else if (node.isBigDecimal()) {
      builder.appendDecimal(node.decimalValue());
    } else if (node.isBigInteger()) {
      builder.appendDecimal(new java.math.BigDecimal(node.bigIntegerValue()));
    } else if (node.isShort()) {
      builder.appendShort(node.shortValue());
    } else {
      // Fallback for any other numeric type
      builder.appendDouble(node.doubleValue());
    }
  }
}
