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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.TaskConfigs;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

@Title("FLR Source Connector")
@Description("The SpoolDirFlrSourceConnector will monitor the directory specified in `input.path` for files and read them as a CSV " +
    "converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.")
@DocumentationTip("To get a starting point for a schema you can use the following command to generate an all String schema. This " +
    "will give you the basic structure of a schema. From there you can changes the types to match what you expect.\n\n" +
    ".. code-block:: bash\n\n" +
    "   mvn clean package\n" +
    "   export CLASSPATH=\"$(find target/kafka-connect-target/usr/share/kafka-connect/kafka-connect-spooldir -type f -name '*.jar' | tr '\\n' ':')\"\n" +
    "   kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.SchemaGenerator -t csv -f src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/csv/FieldsMatch.data -c config/CSVExample.properties -i id\n" +
    "")
public class SpoolDirFlrSourceConnector extends SourceConnector {

  Map<String, String> settings;

  @Override public void start(Map<String, String> map) {
    SpoolDirFlrSourceConnectorConfig config =
        new SpoolDirFlrSourceConnectorConfig(false, map);
    this.settings = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirFlrSourceTask.class;
  }

  @Override public List<Map<String, String>> taskConfigs(int i) {
    return TaskConfigs.single(this.settings);
  }

  @Override public void stop() {

  }

  @Override
  public ConfigDef config() {
    return SpoolDirFlrSourceConnectorConfig.conf();
  }

  @Override public String version() {
    return "1.0.0";
  }
}
