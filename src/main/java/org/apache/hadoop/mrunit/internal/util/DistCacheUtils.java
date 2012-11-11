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
package org.apache.hadoop.mrunit.internal.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.RunJar;

public class DistCacheUtils {

  private static final String CACHE_LOCALARCHIVES = "mapred.cache.localArchives";
  private static final String CACHE_LOCALFILES = "mapred.cache.localFiles";
  private static final Log LOG = LogFactory.getLog(DistCacheUtils.class);

  private DistCacheUtils() {
    //
  }

  /**
   * Attempt to create a URI from a string path. First tries to load as a 
   * class resource, and failing that, loads as a File.
   *  
   * @param path path to resource
   * @return the uri of the resource
   */
  public static URI findResource(String path) {
    URI uri = null;
    try {
      URL resourceUrl = DistCacheUtils.class.getClassLoader().getResource(path);
      if (resourceUrl != null) {
        uri = resourceUrl.toURI();
      } else {
        uri = new File(path).toURI();
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(path + " could not be found", e);
    }
    if (uri == null) {
      throw new IllegalArgumentException(path + " could not be found");
    }
    return uri;
  }

  /**
   * Creates a comma separated list from a list of Path objects. 
   * Method borrowed from Hadoop's TaskDistributedCacheManager
   */
  public static String stringifyPathList(List<Path> p){
    if (p == null || p.isEmpty()) {
      return null;
    }
    StringBuilder str = new StringBuilder(p.get(0).toString());
    for (int i = 1; i < p.size(); i++){
      str.append(",");
      str.append(p.get(i).toString());
    }
    return str.toString();
  }

  /**
   * Create a randomly named temporary directory
   * 
   * @return the file handle of the directory
   * @throws IOException
   */
  public static File createTempDirectory() throws IOException {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"), 
        "mrunit-" + UUID.randomUUID().toString());
    LOG.debug("Creating temp directory " + tmpDir);
    tmpDir.mkdirs();
    return tmpDir;
  }

  /**
   * Extract an archive to the temp directory.
   * Code borrowed from Hadoop's TrackerDistributedCacheManager
   * 
   * @param cacheArchive the cache archive to extract
   * @param tmpDir root location of temp directory
   * @return the path to the extracted archive
   * @throws IOException
   */
  public static Path extractArchiveToTemp(Path cacheArchive, File tmpDir) throws IOException {
    String tmpArchive = cacheArchive.getName().toLowerCase();
    File srcFile = new File(cacheArchive.toString());
    File destDir = new File(tmpDir, srcFile.getName());
    LOG.debug(String.format("Extracting %s to %s",
             srcFile.toString(), destDir.toString()));
    if (tmpArchive.endsWith(".jar")) {
      RunJar.unJar(srcFile, destDir);
    } else if (tmpArchive.endsWith(".zip")) {
      FileUtil.unZip(srcFile, destDir);
    } else if (isTarFile(tmpArchive)) {
      FileUtil.unTar(srcFile, destDir);
    } else {
      LOG.warn(String.format(
          "Cache file %s specified as archive, but not valid extension.",
          srcFile.toString()));
      return cacheArchive;
    }
    return new Path(destDir.toString());
  }

  private static boolean isTarFile(String filename) {
    return (filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
           filename.endsWith(".tar"));
  }

  public static void addLocalFiles(Configuration conf, String str) {
    String files = conf.get(CACHE_LOCALFILES);
    conf.set(CACHE_LOCALFILES, files == null ? str
        : files + "," + str);
  }

  public static void addLocalArchives(Configuration conf, String str) {
    String files = conf.get(CACHE_LOCALARCHIVES);
    conf.set(CACHE_LOCALARCHIVES, files == null ? str
        : files + "," + str);
  }

}
