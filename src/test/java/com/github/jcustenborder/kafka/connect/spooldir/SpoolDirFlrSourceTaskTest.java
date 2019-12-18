/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class SpoolDirFlrSourceTaskTest extends SpoolDirSourceTaskTest<SpoolDirFlrSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirFlrSourceTaskTest.class);

  @Override
  protected SpoolDirFlrSourceTask createTask() {
    return new SpoolDirFlrSourceTask();
  }

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();
    return settings;
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "flr";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }
}
