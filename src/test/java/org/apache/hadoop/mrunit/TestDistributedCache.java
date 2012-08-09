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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.junit.Before;
import org.junit.Test;

public class TestDistributedCache {

  private static final Text DUMMY = new Text("DUMMY");

  private Mapper<Text,Text,Text,Text> mapper = new TestDistributedCacheMapperAndReducer();
  private Reducer<Text,Text,Text,Text> reducer = new TestDistributedCacheMapperAndReducer();

  private MapDriver<Text,Text,Text,Text> mapDriver = 
      MapDriver.newMapDriver(mapper);
  private ReduceDriver<Text,Text,Text,Text> reduceDriver = 
      ReduceDriver.newReduceDriver(reducer);
  private MapReduceDriver<Text,Text,Text,Text,Text,Text> mapReduceDriver = 
      MapReduceDriver.newMapReduceDriver();

  /**
   * A dual Mapper and Reducer class which loads files / archives from distributed
   * cache and outputs the filenames as keys, and whether the cache item is a file
   * or directory ("file" or "dir") as value
   */
  private static class TestDistributedCacheMapperAndReducer extends MapReduceBase 
    implements Mapper<Text,Text,Text,Text>, Reducer<Text,Text,Text,Text> {

    private static final Text DIR = new Text("dir");
    private static final Text FILE = new Text("file");

    private List<Path> cachePaths = new ArrayList<Path>();
    private FileSystem fs;
    private Text outputKey = new Text();

    @Override
    public void configure(JobConf job) {
      try {
        fs = FileSystem.get(job);
        cachePaths.clear();
        Path[] localCacheArchives = DistributedCache.getLocalCacheArchives(job);
        if (localCacheArchives != null) {
          cachePaths.addAll(Arrays.asList(localCacheArchives));
        }
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(job);
        if (localCacheFiles != null) {
          cachePaths.addAll(Arrays.asList(localCacheFiles));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void map(Text key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      outputCachePaths(output, reporter);
    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      outputCachePaths(output, reporter);
    }

    private void outputCachePaths(OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      for (Path path: cachePaths) {
        outputPath("", path, output, reporter);
      }
    }

    private void outputPath(String parentPath, Path path, 
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      FileStatus fstat = fs.getFileStatus(path);
      boolean isDir = fstat.isDir();
      if (parentPath.length() > 0) {
        //append parent name if the file was extracted from an archive
        outputKey.set(parentPath + "/" + path.getName());
      } else {
        outputKey.set(path.getName());
      }
      // output the file info
      output.collect(outputKey, isDir ? DIR : FILE);
      // recurse into extracted directories
      if (isDir) {
        for (FileStatus subStat: fs.listStatus(path)) {
          outputPath(path.getName(), subStat.getPath(), output, reporter);
        }
      }
    }
  }

  @Before
  public void setup() {
    // all drivers need dummy input to drive the tests
    mapDriver.withInput(DUMMY, DUMMY);
    reduceDriver.withInput(DUMMY, Arrays.asList(DUMMY,DUMMY,DUMMY));
    mapReduceDriver.withInput(DUMMY, DUMMY);
  }

  @Test
  public void testAddCacheFileToMapperUsingDriverMethod() throws IOException
  {
    mapDriver.withCacheFile("testfile")
      .withOutput(new Text("testfile"), new Text("file")).runTest();
  }

  @Test
  public void testAddCacheFileToMapperUsingStaticMethod() throws Exception
  {
    Configuration conf = new Configuration();
    DistributedCache.addCacheFile(new File("README.txt").toURI(), conf);
    mapDriver.withConfiguration(conf)
      .withOutput(new Text("README.txt"), new Text("file")).runTest();
  }

  @Test
  public void testAddCacheArchiveToMapperUsingDriverMethod() throws IOException
  {
    // Cache archives should be extracted into a directory named the same
    // as the original file
    mapDriver.withCacheArchive("testarchive.tar")
      .withOutput(new Text("testarchive.tar"), new Text("dir"))
      .withOutput(new Text("testarchive.tar/a"), new Text("file"))
      .withOutput(new Text("testarchive.tar/b"), new Text("file"))
      .withOutput(new Text("testarchive.tar/c"), new Text("file"))
      .withOutput(new Text("testarchive.tar/d"), new Text("file"))
      .runTest();
  }

  @Test
  public void testAddCacheArchiveWithInvalidExtension() throws IOException
  {
    // Cache archives with a non-archive extension should be passed
    // through without being expanded
    mapDriver.withCacheArchive("testarchive.tar.abc")
      .withOutput(new Text("testarchive.tar.abc"), new Text("file"))
      .runTest();
  }

  @Test
  public void testAddCacheFileToReducerUsingDriverMethod() throws IOException
  {
    reduceDriver.withCacheFile("testfile")
      .withOutput(new Text("testfile"), new Text("file")).runTest();
  }
  
  @Test
  public void testAddCacheFileToReducerUsingStaticMethod() throws Exception
  {
    Configuration conf = new Configuration();
    DistributedCache.addCacheFile(new File("README.txt").toURI(), conf);
    reduceDriver.withConfiguration(conf)
      .withOutput(new Text("README.txt"), new Text("file")).runTest();
  }

  @Test
  public void testAddCacheArchiveToReducerUsingDriverMethod() throws IOException
  {
    reduceDriver.withCacheArchive("testarchive.tar")
      .withOutput(new Text("testarchive.tar"), new Text("dir"))
      .withOutput(new Text("testarchive.tar/a"), new Text("file"))
      .withOutput(new Text("testarchive.tar/b"), new Text("file"))
      .withOutput(new Text("testarchive.tar/c"), new Text("file"))
      .withOutput(new Text("testarchive.tar/d"), new Text("file"))
      .runTest();
  }

  @Test
  public void testAddCacheArchiveToMapReduceUsingDriverMethod() throws IOException
  {
    // Tests that dist cache files are correctly processed by mapper
    // as part of the mapreduce driver pipeline
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(new IdentityReducer<Text,Text>());
    mapReduceDriver.withCacheArchive("testarchive.tar")
      .withOutput(new Text("testarchive.tar"), new Text("dir"))
      .withOutput(new Text("testarchive.tar/a"), new Text("file"))
      .withOutput(new Text("testarchive.tar/b"), new Text("file"))
      .withOutput(new Text("testarchive.tar/c"), new Text("file"))
      .withOutput(new Text("testarchive.tar/d"), new Text("file"))
      .runTest();
  }
  
  @Test
  public void testAddCacheArchiveToMapReduceUsingDriverMethod2() throws IOException
  {
    // Tests that dist cache files are correctly processed by reducer
    // as part of the mapreduce driver pipeline
    mapReduceDriver.setMapper(new IdentityMapper<Text,Text>());
    mapReduceDriver.setReducer(reducer);
    mapReduceDriver.withCacheArchive("testarchive.tar")
      .withOutput(new Text("testarchive.tar"), new Text("dir"))
      .withOutput(new Text("testarchive.tar/a"), new Text("file"))
      .withOutput(new Text("testarchive.tar/b"), new Text("file"))
      .withOutput(new Text("testarchive.tar/c"), new Text("file"))
      .withOutput(new Text("testarchive.tar/d"), new Text("file"))
      .runTest();
  }

}
