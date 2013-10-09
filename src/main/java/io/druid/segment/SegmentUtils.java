package io.druid.segment;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 */
public class SegmentUtils
{
  public static int getVersionFromDir(File inDir) throws IOException
  {
    File versionFile = new File(inDir, "version.bin");
    if (versionFile.exists()) {
      return Ints.fromByteArray(Files.toByteArray(versionFile));
    }

    final File indexFile = new File(inDir, "index.drd");
    int version;
    try (InputStream in = new FileInputStream(indexFile)) {
      version = in.read();
    }
    return version;
  }
}
