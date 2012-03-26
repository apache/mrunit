/**
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
package org.apache.hadoop.mrunit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

public class Serialization {

  /**
   * Creates a new copy of the orig object. Depending on the serialization used,
   * the serialization class may or may not copy the orig object into the copy
   * object based on the contract on
   * org.apache.hadoop.io.serializer.Deserializer.deserialize
   * 
   * @param orig
   * @param copy
   *          if null always returns a new object, if not null may or may not
   *          copy orig into copy depending on what serialization class is used
   * @param conf
   *          specifies the serialization classes to use in io.serializations
   * @return a copy of the orig object
   */
  @SuppressWarnings("unchecked")
  public static Object copy(final Object orig, final Object copy,
      final Configuration conf) {
    if (copy != null && orig.getClass() != copy.getClass()) {
      throw new IllegalArgumentException(orig.getClass() + "!="
          + copy.getClass());
    }
    final Class<?> clazz = orig.getClass();
    final SerializationFactory serializationFactory = new SerializationFactory(
        conf);
    final Serializer<Object> serializer = (Serializer<Object>) serializationFactory
        .getSerializer(clazz);
    final Deserializer<Object> deserializer = (Deserializer<Object>) serializationFactory
        .getDeserializer(clazz);
    try {
      final DataOutputBuffer outputBuffer = new DataOutputBuffer();
      serializer.open(outputBuffer);
      serializer.serialize(orig);
      final DataInputBuffer inputBuffer = new DataInputBuffer();
      inputBuffer.reset(outputBuffer.getData(), outputBuffer.getLength());
      deserializer.open(inputBuffer);
      return deserializer.deserialize(copy);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new copy of the orig object
   * 
   * @param orig
   * @param conf
   *          specifies the serialization classes to use in io.serializations
   * @return a new copy of the orig object
   */
  public static Object copy(final Object orig, final Configuration conf) {
    return copy(orig, null, conf);
  }

}
