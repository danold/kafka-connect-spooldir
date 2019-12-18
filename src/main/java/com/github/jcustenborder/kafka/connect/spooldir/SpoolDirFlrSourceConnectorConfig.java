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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;

import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Map;

class SpoolDirFlrSourceConnectorConfig extends SpoolDirSourceConnectorConfig {

  public static final String FLR_CHARSET_CONF = "flr.file.charset";
  static final String FLR_CHARSET_DOC = "Character set to read wth file with.";
  static final String FLR_CHARSET_DISPLAY = "File character set.";
  static final String FLR_CHARSET_DEFAULT = Charset.defaultCharset().name();
  static final String FLR_GROUP = "FLR Parsing";
  private static final Character NULL_CHAR = (char) 0;

  public final Charset charset;

  public SpoolDirFlrSourceConnectorConfig(final boolean isTask, Map<String, ?> settings) {
    super(isTask, conf(), settings);

    String charsetName = this.getString(SpoolDirFlrSourceConnectorConfig.FLR_CHARSET_CONF);
    this.charset = Charset.forName(charsetName);

  }

  static final ConfigDef conf() {

    return SpoolDirSourceConnectorConfig.config()

        .define(
            ConfigKeyBuilder.of(FLR_CHARSET_CONF, ConfigDef.Type.STRING)
                .defaultValue(FLR_CHARSET_DEFAULT)
                .validator(CharsetValidator.of())
                .importance(ConfigDef.Importance.LOW)
                .documentation(FLR_CHARSET_DOC)
                .displayName(FLR_CHARSET_DISPLAY)
                .group(FLR_GROUP)
                .width(ConfigDef.Width.LONG)
                .build()
        );
  }

  final char getChar(String key) {
    int intValue = this.getInt(key);
    return (char) intValue;
  }



  @Override
  public boolean schemasRequired() {
    return true;
  }

  static class CharsetValidator implements ConfigDef.Validator {
    static CharsetValidator of() {
      return new CharsetValidator();
    }

    @Override
    public void ensureValid(String s, Object o) {
      try {
        Preconditions.checkState(o instanceof String);
        String input = (String) o;
        Charset.forName(input);
      } catch (IllegalArgumentException e) {
        throw new DataException(
            String.format("Charset '%s' is invalid for %s", o, s),
            e
        );
      }
    }

    @Override
    public String toString() {
      return Joiner.on(",").join(Charset.availableCharsets().keySet());
    }
  }
}
