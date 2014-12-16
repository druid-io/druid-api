/*
 * Copyright 2014 Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.StreamUtils;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 */
public class CompressionUtils
{
  private static final Logger log = new Logger(CompressionUtils.class);

  public static long zip(File directory, File outputZipFile) throws IOException
  {
    if (!outputZipFile.getName().endsWith(".zip")) {
      log.warn("No .zip suffix[%s], putting files from [%s] into it anyway.", outputZipFile, directory);
    }

    try (final FileOutputStream out = new FileOutputStream(outputZipFile)) {
      return zip(directory, out);
    }
  }

  public static long zip(File directory, OutputStream out) throws IOException
  {
    if (!directory.isDirectory()) {
      throw new IOException(String.format("directory[%s] is not a directory", directory));
    }

    long totalSize = 0;
    ZipOutputStream zipOut = null;
    try {
      zipOut = new ZipOutputStream(out);
      File[] files = directory.listFiles();
      for (File file : files) {
        log.info("Adding file[%s] with size[%,d].  Total size so far[%,d]", file, file.length(), totalSize);
        if (file.length() >= Integer.MAX_VALUE) {
          zipOut.finish();
          throw new IOException(String.format("file[%s] too large [%,d]", file, file.length()));
        }
        zipOut.putNextEntry(new ZipEntry(file.getName()));
        totalSize += ByteStreams.copy(Files.newInputStreamSupplier(file), zipOut);
      }
      zipOut.closeEntry();
    }
    finally {
      if (zipOut != null) {
        zipOut.finish();
      }
    }

    return totalSize;
  }

  public static void unzip(File pulledFile, File outDir) throws IOException
  {
    if (!(outDir.exists() && outDir.isDirectory())) {
      throw new ISE("outDir[%s] must exist and be a directory", outDir);
    }

    log.info("Unzipping file[%s] to [%s]", pulledFile, outDir);
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(pulledFile));
      unzip(in, outDir);
    }
    finally {
      CloseQuietly.close(in);
    }
  }

  public static void unzip(InputStream in, File outDir) throws IOException
  {
    ZipInputStream zipIn = new ZipInputStream(in);

    ZipEntry entry;
    while ((entry = zipIn.getNextEntry()) != null) {
      try (FileOutputStream out = new FileOutputStream(new File(outDir, entry.getName()))){
        ByteStreams.copy(zipIn, out);
        zipIn.closeEntry();
      }
    }
  }

  public static void gunzip(File pulledFile, File outDir) throws IOException
  {
    log.info("Gunzipping file[%s] to [%s]", pulledFile, outDir);
    StreamUtils.copyToFileAndClose(new GZIPInputStream(new FileInputStream(pulledFile)), outDir);
    if (!pulledFile.delete()) {
      log.error("Could not delete tmpFile[%s].", pulledFile);
    }
  }

}
