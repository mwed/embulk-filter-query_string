/*
 * Copyright 2016 Minnano Wedding Co., Ltd.
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

package org.embulk.filter.query_string;

import com.indeed.util.urlparsing.ParseUtils;
import com.indeed.util.urlparsing.QueryStringParser;
import com.indeed.util.urlparsing.QueryStringParserCallback;
import com.indeed.util.urlparsing.QueryStringParserCallbackBuilder;
import org.embulk.config.Config;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStringFilterPlugin
        implements FilterPlugin
{
    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        String columnName = task.getQueryStringColumnName();

        Schema.Builder builder = Schema.builder();
        for (Column inputColumn : inputSchema.getColumns()) {
            if (columnName.equals(inputColumn.getName())) {
                insertColumns(builder, task.getExpandedColumns());
            }
            else {
                builder.add(inputColumn.getName(), inputColumn.getType());
            }
        }
        control.run(task.dump(), builder.build());
    }

    private void insertColumns(Schema.Builder builder, List<ColumnConfig> expandedColumns)
    {
        for (ColumnConfig columnConfig : expandedColumns) {
            builder.add(columnConfig.getName(), columnConfig.getType());
        }
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput()
        {
            final Logger logger = Exec.getLogger(this.getClass());

            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            private final QueryStringParserCallback stringParser = new QueryStringParserCallback<Map<String, Object>>()
            {
                @Override
                public void parseKeyValuePair(String queryString, int keyStart, int keyEnd, int valueStart, int valueEnd, Map<String, Object> storage)
                {
                    String key = queryString.substring(keyStart, keyEnd);

                    StringBuilder decoded = new StringBuilder();
                    ParseUtils.urlDecodeInto(queryString, valueStart, valueEnd, decoded);

                    storage.put(key, decoded.toString());
                }
            };

            private final QueryStringParserCallback booleanParser = new QueryStringParserCallback<Map<String, Object>>()
            {
                @Override
                public void parseKeyValuePair(String queryString, int keyStart, int keyEnd, int valueStart, int valueEnd, Map<String, Object> storage)
                {
                    String key = queryString.substring(keyStart, keyEnd);
                    String value = queryString.substring(valueStart, valueEnd);

                    storage.put(key, Boolean.parseBoolean(value));
                }
            };

            private final QueryStringParserCallback doubleParser = new QueryStringParserCallback<Map<String, Object>>()
            {
                @Override
                public void parseKeyValuePair(String queryString, int keyStart, int keyEnd, int valueStart, int valueEnd, Map<String, Object> storage)
                {
                    String key = queryString.substring(keyStart, keyEnd);
                    String value = queryString.substring(valueStart, valueEnd);

                    storage.put(key, Double.parseDouble(value));
                }
            };

            private final QueryStringParserCallback longParser = new QueryStringParserCallback<Map<String, Object>>()
            {
                @Override
                public void parseKeyValuePair(String queryString, int keyStart, int keyEnd, int valueStart, int valueEnd, Map<String, Object> storage)
                {
                    String key = queryString.substring(keyStart, keyEnd);
                    long value = ParseUtils.parseUnsignedLong(queryString, valueStart, valueEnd);

                    storage.put(key, value);
                }
            };

            private final QueryStringParserCallback unixTimestampParser = new QueryStringParserCallback<Map<String, Object>>()
            {
                @Override
                public void parseKeyValuePair(String queryString, int keyStart, int keyEnd, int valueStart, int valueEnd, Map<String, Object> storage)
                {
                    String key = queryString.substring(keyStart, keyEnd);
                    long value = ParseUtils.parseUnsignedLong(queryString, valueStart, valueEnd);

                    storage.put(key, Timestamp.ofEpochSecond(value));
                }
            };

            @Override
            public void add(Page page)
            {
                String columnName = task.getQueryStringColumnName();
                QueryStringParserCallback<Map<String, Object>> callback = buildQueryStringParserCallback();

                reader.setPage(page);
                while (reader.nextRecord()) {
                    int curr = 0;

                    for (Column inputColumn : inputSchema.getColumns()) {
                        logger.info("inputColumn -> " + inputColumn.getName() + ", " + inputColumn.getType());

                        if (columnName.equals(inputColumn.getName())) {
                            String path = reader.getString(inputColumn);

                            int pos = path.indexOf('?');
                            if (pos > 0) {
                                String queryString = path.substring(pos + 1);

                                Map<String, Object> map = new HashMap<>();
                                QueryStringParser.parseQueryString(queryString, callback, map);
                                logger.info("map -> " + map);

                                for (ColumnConfig config : task.getExpandedColumns()) {
                                    Object object = map.get(config.getName());
                                    logger.info("builder.set " + config.getType().toString() + "(" + curr + ", " + object + ")");

                                    if (object == null) {
                                        builder.setNull(curr);
                                    }
                                    else {
                                        if (Types.STRING.equals(config.getType())) {
                                            builder.setString(curr, (String) object);
                                        }
                                        else if (Types.BOOLEAN.equals(config.getType())) {
                                            builder.setBoolean(curr, (Boolean) object);
                                        }
                                        else if (Types.DOUBLE.equals(config.getType())) {
                                            builder.setDouble(curr, (Double) object);
                                        }
                                        else if (Types.LONG.equals(config.getType())) {
                                            builder.setLong(curr, (Long) object);
                                        }
                                        else if (Types.TIMESTAMP.equals(config.getType())) {
                                            builder.setTimestamp(curr, (Timestamp) object);
                                        }
                                    }

                                    curr++;
                                }
                            }
                            else {
                                logger.warn("The column was ignored because it does not include query string: " + path);

                                for (ColumnConfig config : task.getExpandedColumns()) {
                                    logger.info("builder.set " + config.getType().toString() + "(" + curr + ", null)");

                                    builder.setNull(curr);
                                    curr++;
                                }
                            }
                        }
                        else {
                            if (reader.isNull(inputColumn)) {
                                builder.setNull(curr);
                            }
                            else {
                                if (Types.STRING.equals(inputColumn.getType())) {
                                    logger.info("builder.set " + inputColumn.getType().toString() + "(" + curr + ", " + reader.getString(inputColumn) + ")");

                                    builder.setString(curr, reader.getString(inputColumn));
                                }
                                else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                                    logger.info("builder.set " + inputColumn.getType().toString() + "(" + curr + ", " + reader.getBoolean(inputColumn) + ")");

                                    builder.setBoolean(curr, reader.getBoolean(inputColumn));
                                }
                                else if (Types.DOUBLE.equals(inputColumn.getType())) {
                                    logger.info("builder.set " + inputColumn.getType().toString() + "(" + curr + ", " + reader.getDouble(inputColumn) + ")");

                                    builder.setDouble(curr, reader.getDouble(inputColumn));
                                }
                                else if (Types.LONG.equals(inputColumn.getType())) {
                                    logger.info("builder.set " + inputColumn.getType().toString() + "(" + curr + ", " + reader.getLong(inputColumn) + ")");

                                    builder.setLong(curr, reader.getLong(inputColumn));
                                }
                                else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                                    logger.info("builder.set " + inputColumn.getType().toString() + "(" + curr + ", " + reader.getTimestamp(inputColumn) + ")");

                                    builder.setTimestamp(curr, reader.getTimestamp(inputColumn));
                                }
                            }

                            curr++;
                        }
                    }

                    builder.addRecord();
                }
            }

            private QueryStringParserCallback<Map<String, Object>> buildQueryStringParserCallback()
            {
                QueryStringParserCallbackBuilder<Map<String, Object>> callbackBuilder = new QueryStringParserCallbackBuilder<>();

                for (ColumnConfig config : task.getExpandedColumns()) {
                    if (Types.STRING.equals(config.getType())) {
                        callbackBuilder.addCallback(config.getName(), stringParser);
                    }
                    else if (Types.BOOLEAN.equals(config.getType())) {
                        callbackBuilder.addCallback(config.getName(), booleanParser);
                    }
                    else if (Types.DOUBLE.equals(config.getType())) {
                        callbackBuilder.addCallback(config.getName(), doubleParser);
                    }
                    else if (Types.LONG.equals(config.getType())) {
                        callbackBuilder.addCallback(config.getName(), longParser);
                    }
                    else if (Types.TIMESTAMP.equals(config.getType())) {
                        callbackBuilder.addCallback(config.getName(), unixTimestampParser);
                    }
                }
                return callbackBuilder.buildCallback();
            }

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }
        };
    }

    public interface PluginTask
            extends Task
    {
        @Config("query_string_column_name")
        public String getQueryStringColumnName();

        @Config("expanded_columns")
        public List<ColumnConfig> getExpandedColumns();
    }
}
