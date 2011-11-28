package org.apache.hadoop.mrunit;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestConfigureClose {

  @Test
  public void testMapperConfigureClose() {
    final AtomicBoolean configureWasCalled = new AtomicBoolean(false);
    final AtomicBoolean closeWasCalled = new AtomicBoolean(false);
    Mapper<Text, Text, Text, Text> mapper = new Mapper<Text, Text, Text, Text>() {
      @Override
      public void configure(JobConf arg0) {
        configureWasCalled.set(true);
      }

      @Override
      public void close() throws IOException {
        closeWasCalled.set(true);
      }
      @Override
      public void map(Text key, Text value, OutputCollector<Text, Text> output,
          Reporter reporter) throws IOException { 
      }
    };    
    MapDriver<Text, Text, Text, Text> driver = new MapDriver<Text, Text, Text, Text>(mapper);
    driver.runTest();
    assertTrue(configureWasCalled.get());
    assertTrue(closeWasCalled.get());
  }
  @Test
  public void testReducerConfigureClose() {
    final AtomicBoolean configureWasCalled = new AtomicBoolean(false);
    final AtomicBoolean closeWasCalled = new AtomicBoolean(false);
    Reducer<Text, Text, Text, Text> reducer = new Reducer<Text, Text, Text, Text>() {
      @Override
      public void configure(JobConf arg0) {
        configureWasCalled.set(true);
      }

      @Override
      public void close() throws IOException {
        closeWasCalled.set(true);
      }

      @Override
      public void reduce(Text arg0, Iterator<Text> arg1,
          OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
      }
    };    
    ReduceDriver<Text, Text, Text, Text> driver = new ReduceDriver<Text, Text, Text, Text>(reducer);
    driver.runTest();
    assertTrue(configureWasCalled.get());
    assertTrue(closeWasCalled.get());
  }
}
