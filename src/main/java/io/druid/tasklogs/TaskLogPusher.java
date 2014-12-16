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

package io.druid.tasklogs;

import java.io.File;
import java.io.IOException;

/**
 * Something that knows how to persist local task logs to some form of long-term storage.
 */
public interface TaskLogPusher
{
  public void pushTaskLog(String taskid, File logFile) throws IOException;
}
