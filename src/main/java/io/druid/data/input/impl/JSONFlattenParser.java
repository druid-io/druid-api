/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;

import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JSONFlattenParser implements Parser<String, Object>
{
  private static final Pattern arrayIndexRegex = Pattern.compile("(.*)(\\[\\d+\\])");
  private final ObjectMapper objectMapper;
  private ArrayList<String> fieldNames;
  private final Set<String> exclude;
  private final ParseFlattenSpec flattenSpec;
  private Function<JsonNode, Object> valFn;

  public static final Function<JsonNode, Object> flattenValueFunction = new Function<JsonNode, Object>()
  {
    @Override
    public Object apply(JsonNode node)
    {
      if (node == null || node.isMissingNode() || node.isNull()) {
        return null;
      }
      if (node.isIntegralNumber()) {
        if (node.canConvertToLong()) {
          return node.asLong();
        } else {
          return node.asDouble();
        }
      }
      if (node.isFloatingPointNumber()) {
        return node.asDouble();
      }

      final String s = node.asText();
      final CharsetEncoder enc = Charsets.UTF_8.newEncoder();
      if (s != null && !enc.canEncode(s)) {
        // Some whacky characters are in this string (e.g. \uD900). These are problematic because they are decodeable
        // by new String(...) but will not encode into the same character. This dance here will replace these
        // characters with something more sane.
        return new String(s.getBytes(Charsets.UTF_8), Charsets.UTF_8);
      } else {
        return s;
      }
    }
  };

  public JSONFlattenParser(
      ObjectMapper objectMapper,
      Iterable<String> fieldNames,
      String timeColumn,
      ParseFlattenSpec flattenSpec,
      Function<JsonNode, Object> valFn
  )
  {
    this(objectMapper, fieldNames, null, timeColumn, flattenSpec, valFn);
  }

  public JSONFlattenParser(
      ObjectMapper objectMapper,
      Iterable<String> fieldNames,
      Iterable<String> exclude,
      String timeColumn,
      ParseFlattenSpec flattenSpec,
      Function<JsonNode, Object> valFn
  )
  {
    this.objectMapper = objectMapper;
    if (fieldNames != null) {
      setFieldNames(fieldNames);
    }
    this.fieldNames.add(timeColumn);
    this.exclude = exclude != null ? Sets.newHashSet(exclude) : Sets.<String>newHashSet();
    this.valFn = valFn == null ? flattenValueFunction : valFn;
    this.flattenSpec = flattenSpec;
    for (String metric : flattenSpec.getMetrics()) {
      this.fieldNames.add(metric);
    }
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
    ParserUtils.validateFields(fieldNames);
    this.fieldNames = Lists.newArrayList(fieldNames);
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Function<JsonNode, Object> getValFn()
  {
    return valFn;
  }

  public void setValFn(Function<JsonNode, Object> valFn)
  {
    this.valFn = valFn;
  }

  @Override
  public Map<String, Object> parse(String input)
  {
    try {
      Map<String, Object> map = new LinkedHashMap<>();
      JsonNode root = objectMapper.readTree(input);

      Iterator<String> keysIter = (fieldNames == null ? root.fieldNames() : fieldNames.iterator());

      while (keysIter.hasNext()) {
        String key = keysIter.next();

        if (exclude.contains(key)) {
          continue;
        }

        JsonNode node;
        String nestPrefix = flattenSpec.getNestPrefix();
        if (nestPrefix != null && key.startsWith(nestPrefix)) {
          String strippedKey = key.substring(nestPrefix.length());
          String[] splits = strippedKey.split("\\.");
          node = getNestedNode(splits, root);
        } else {
          node = root.path(key);
        }

        if (node.isArray()) {
          final List<Object> nodeValue = Lists.newArrayListWithExpectedSize(node.size());
          for (final JsonNode subnode : node) {
            final Object subnodeValue = valFn.apply(subnode);
            if (subnodeValue != null) {
              nodeValue.add(subnodeValue);
            }
          }
          map.put(key, nodeValue);
        } else {
          final Object nodeValue = valFn.apply(node);
          if (nodeValue != null) {
            map.put(key, nodeValue);
          }
        }
      }
      return map;
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  public JsonNode getNestedNode(String[] keys, JsonNode root)
  {
    JsonNode node = root;
    for (String key : keys) {
      if (key.endsWith("]")) {
        Matcher matcher = arrayIndexRegex.matcher(key);
        if (!matcher.matches()) {
          return null;
        }

        String newKey = matcher.group(1);
        node = node.path(newKey);

        if (node.isArray()) {
          String match = matcher.group(2);
          int idx = Integer.parseInt(match.substring(1, match.length() - 1));
          node = ((ArrayNode) node).path(idx);
        } else {
          //TODO: error
          node = null;
        }
      } else {
        node = node.path(key);
      }
    }

    return node;
  }

}