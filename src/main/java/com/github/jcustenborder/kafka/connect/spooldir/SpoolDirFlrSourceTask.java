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


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpoolDirFlrSourceTask extends SpoolDirSourceTask<SpoolDirFlrSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirFlrSourceTask.class);
  LineNumberReader reader;



  @Override
  protected SpoolDirFlrSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirFlrSourceConnectorConfig(true, settings);
  }

  @Override
  protected void configure(InputStream inputStream, Map<String, String> metadata, final Long lastOffset) throws IOException {
    if (null != this.reader) {
      this.reader.close();
    }
    Reader streamReader = new InputStreamReader(inputStream);
    this.reader = new LineNumberReader(streamReader);
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
  }

  @Override
  public long recordOffset() {
    return this.reader.getLineNumber();
  }

  @Override
  public List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);



    int recordCount = 0;
    String line = null;
    while (recordCount < this.config.batchSize && null != (line = this.reader.readLine())) {
      Struct keyStruct = new Struct(this.config.keySchema);
      Struct valueStruct = new Struct(this.config.valueSchema);

      int readIndex = 0;

      for (Field field : this.config.valueSchema.fields()) {
        String fieldName = field.name();
        if (field.schema().parameters() == null ||
            !field.schema().parameters().containsKey("flr.length")) {
          throw new RuntimeException("flr.length must be specified on each node of the value schema");
        }
        int fieldLength = Integer.parseInt(field.schema().parameters().get("flr.length"));
        if (readIndex + fieldLength > line.length()) {
          log.warn(String
              .format("Reader exceeds length of row when reading %s, readIndex %d", fieldName,
                  readIndex));
          break;
        }

        Object fieldValue = this.parser.parseString(field.schema(),
            line.substring(readIndex, readIndex + fieldLength).trim());
        readIndex += fieldLength;
        log.trace("process() - output = '{}'", fieldValue);
        valueStruct.put(field, fieldValue);

        Field keyField = this.config.keySchema.field(fieldName);
        if (null != keyField) {
          log.trace("process() - Setting key field '{}' to '{}'", keyField.name(), fieldValue);
          keyStruct.put(keyField, fieldValue);
        }
      }
      if (log.isInfoEnabled() && this.reader.getLineNumber() % ((long) this.config.batchSize * 20) == 0) {
        log.info("Processed {} lines of {}", this.reader.getLineNumber(), this.inputFile);
      }

      addRecord(
          records,
          new SchemaAndValue(keyStruct.schema(), keyStruct),
          new SchemaAndValue(valueStruct.schema(), valueStruct)
      );
    }



    return records;
  }

}
