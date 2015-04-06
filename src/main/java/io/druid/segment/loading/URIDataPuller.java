/*
 * Copyright 2015 Metamarkets Group, Inc.
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

package io.druid.segment.loading;

import com.google.common.base.Predicate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.regex.Pattern;

/**
 * A URIDataPuller has handlings for URI based data
 */
public interface URIDataPuller
{
  /**
   * Create a new InputStream based on the URI
   *
   * @param uri The URI to open an Input Stream to
   *
   * @return A new InputStream which streams the URI in question
   *
   * @throws IOException
   */
  public InputStream getInputStream(URI uri) throws IOException;

  /**
   * Returns an abstract "version" for the URI. The exact meaning of the version is left up to the implementation.
   *
   * @param uri The URI to check
   *
   * @return A "version" as interpreted by the URIDataPuller implementation
   *
   * @throws IOException on error
   */
  public String getVersion(URI uri) throws IOException;

  /**
   * Evaluates a Throwable to see if it is recoverable. This is expected to be used in conjunction with the other methods
   * to determine if anything thrown from the method should be retried.
   *
   * @return Predicate function indicating if the Throwable is recoverable
   */
  public Predicate<Throwable> shouldRetryPredicate();

  /**
   * Use `matchPattern` to filter and find the most up to date data for `uri`.
   * It is expected that the URI has some form of hierarchy and the `matchPattern` is on the file name portion,
   * but implementations may interpret the pattern in whatever way is appropriate. Calls to `getVersion(...)` should
   * yield the correct version of the URI returned from this method.
   *
   * @param matchPattern A Pattern, whose exact functionality depends on the implementation, which is intended
   *                     to assist the URIDataPuller in determining what other URIs might be alternate versions.
   *
   * @return A URI which the URIDataPuller believes is the most up to date version of the URI passed in. Returns `null` if no version is found
   */
  public URI getLatestVersion(URI uri, Pattern matchPattern);
}
