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

import com.google.common.base.Throwables;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.embulk.spi.type.Types.STRING;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestQueryStringFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testTransaction()
    {
        ConfigSource configSource = loadConfigSource("testTransaction.yml");
        final Schema inputSchema = Schema.builder()
                .add("qb", STRING)
                .add("qs", STRING)
                .add("qa", STRING)
                .build();

        final QueryStringFilterPlugin plugin = new QueryStringFilterPlugin();

        plugin.transaction(configSource, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                assertThat(outputSchema.getColumnCount(), is(5));

                assertThat(outputSchema.getColumn(0).getName(), is("qb"));
                assertThat(outputSchema.getColumn(1).getName(), is("q1"));
                assertThat(outputSchema.getColumn(2).getName(), is("q2"));
                assertThat(outputSchema.getColumn(3).getName(), is("q3"));
                assertThat(outputSchema.getColumn(4).getName(), is("qa"));
            }
        });
    }

    @Test
    public void testOpenSuccessfully()
    {
        ConfigSource configSource = loadConfigSource("testOpen.yml");
        final Schema inputSchema = Schema.builder()
                .add("qb", STRING)
                .add("qs", STRING)
                .add("qa", STRING)
                .build();

        final QueryStringFilterPlugin plugin = new QueryStringFilterPlugin();
        plugin.transaction(configSource, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                TestPageBuilderReader.MockPageOutput mockPageOutput = new TestPageBuilderReader.MockPageOutput();
                PageOutput pageOutput = plugin.open(taskSource, inputSchema, outputSchema, mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, "before", "/path?q1=one&q2=2#fragment", "after");
                for (Page page : pages) {
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);
                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);

                    assertThat(pageReader.getString(0), is("before"));
                    assertThat(pageReader.getString(1), is("one"));
                    assertEquals(2L, pageReader.getLong(2));
                    assertThat(pageReader.getString(3), is("after"));
                }
            }
        });
    }

    @Ignore
    @Test
    public void testOpenWithInvalidValue()
    {
        ConfigSource configSource = loadConfigSource("testOpenWithInvalidValue.yml");
        final Schema inputSchema = Schema.builder()
                .add("qs", STRING)
                .build();

        final QueryStringFilterPlugin plugin = new QueryStringFilterPlugin();
        plugin.transaction(configSource, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                TestPageBuilderReader.MockPageOutput mockPageOutput = new TestPageBuilderReader.MockPageOutput();
                PageOutput pageOutput = plugin.open(taskSource, inputSchema, outputSchema, mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, "/path?q1=alpha");
                for (Page page : pages) {
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);
                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);

                    assertTrue(pageReader.isNull(0));
                }
            }
        });
    }

    private ConfigSource loadConfigSource(String yamlPath)
    {
        try {
            ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
            InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(yamlPath);

            return loader.fromYaml(stream);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
