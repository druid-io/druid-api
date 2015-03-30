package io.druid.utils;

import com.metamx.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 */
public class CompressionUtils
{
  private static final Logger log = new Logger(CompressionUtils.class);


  @Deprecated // Use com.metamx.common.CompressionUtils.zip
  public static long zip(File directory, File outputZipFile) throws IOException
  {
    return com.metamx.common.CompressionUtils.zip(directory, outputZipFile);
  }


  @Deprecated // Use com.metamx.common.CompressionUtils.zip
  public static long zip(File directory, OutputStream out) throws IOException
  {
    return com.metamx.common.CompressionUtils.zip(directory, out);
  }

  @Deprecated // Use com.metamx.common.CompressionUtils.unzip
  public static void unzip(File pulledFile, File outDir) throws IOException
  {
    com.metamx.common.CompressionUtils.unzip(pulledFile, outDir);
  }

  @Deprecated // Use com.metamx.common.CompressionUtils.unzip
  public static void unzip(InputStream in, File outDir) throws IOException
  {
    com.metamx.common.CompressionUtils.unzip(in, outDir);
  }

  /**
   * Uncompress using a gzip uncompress algorithm from the `pulledFile` to the `outDir`.
   * Unlike `com.metamx.common.CompressionUtils.gunzip`, this function takes an output *DIRECTORY* and tries to guess the file name.
   * It is recommended that the caller use `com.metamx.common.CompressionUtils.gunzip` and specify the output file themselves to ensure names are as expected
   *
   * @param pulledFile The source file
   * @param outDir     The destination directory to put the resulting file
   *
   * @throws IOException on propogated IO exception, IAE if it cannot determine the proper new name for `pulledFile`
   */
  @Deprecated // See description for alternative
  public static void gunzip(File pulledFile, File outDir) throws IOException
  {
    final File outFile = new File(outDir, com.metamx.common.CompressionUtils.getGzBaseName(pulledFile.getName()));
    com.metamx.common.CompressionUtils.gunzip(pulledFile, outFile);
    if (!pulledFile.delete()) {
      log.error("Could not delete tmpFile[%s].", pulledFile);
    }
  }

}
