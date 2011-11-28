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
import org.apache.hadoop.util.ReflectionUtils;

public class Serialization {

  @SuppressWarnings("unchecked")
  public static void copy(Object orig, Object copy, Configuration conf) {
    if(orig == null || copy == null) {
      return;
    }
    if(orig.getClass() != copy.getClass()) {
      throw new IllegalArgumentException(orig.getClass() + "!=" +  copy.getClass());
    }
    Class<?> clazz = orig.getClass();
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Serializer<Object> serializer = (Serializer<Object>) serializationFactory.getSerializer(orig.getClass());
    Deserializer<Object> deserializer = (Deserializer<Object>) serializationFactory.getDeserializer(clazz);
    try {
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      serializer.open(outputBuffer);
      serializer.serialize(orig);
      DataInputBuffer inputBuffer = new DataInputBuffer();
      inputBuffer.reset(outputBuffer.getData(), outputBuffer.getLength());
      deserializer.open(inputBuffer);
      deserializer.deserialize(copy);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } 
  }
  
  public static Object copy(Object orig, Configuration conf) {
    if(orig == null) {
      return null;
    }
    Class<?> clazz = orig.getClass();
    Object copy = ReflectionUtils.newInstance(clazz, conf);
    copy(orig, copy, conf);
    return copy;
  }

}
