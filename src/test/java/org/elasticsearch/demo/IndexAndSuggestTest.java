/*
 * Copyright 2013 Lukas Vlcek and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.demo;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexAndSuggestTest extends BaseTestSupport {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    private Settings settings;

    @Before
    public void prepare() throws IOException {
        String tempFolderName = testFolder.newFolder(IndexAndSearchTest.class.getName()).getCanonicalPath();
        settings = settingsBuilder()
                .put("index.store.type", "memory")
                .put("gateway.type", "none")
                .put("path.data", tempFolderName)
                .build();
    }

    /**
     * Index and search data. To make sure we can search for data that has been indexed just now call the refresh.
     * By default the refresh is executed every second.
     *
     * @throws IOException
     */
    @Test
    public void shouldIndexAndSuggestRightAfterRefresh() throws IOException {

        String INDEX = "brands";
        String TYPE = "brand";

        // For unit tests it is recommended to use local node.
        // This is to ensure that your node will never join existing cluster on the network.
        Node node = NodeBuilder.nodeBuilder()
                .settings(settings)
                .local(true)
                .node();

        // Get client
        Client client = node.client();
        client.admin().indices().prepareCreate(INDEX).execute().actionGet();

        // Mapping
        XContentBuilder builder = jsonBuilder().
                startObject().
                    startObject(TYPE).
                        startObject("properties").
                            startObject("name").
                                field("type", "string").
                            endObject().
                            startObject("name_suggest").
                                field("type", "completion").field("payloads", "false").
                            endObject().
                        endObject().
                endObject().
                endObject();
        client.admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(builder).execute().actionGet();

        // Index some data
        XContentBuilder brand1 = jsonBuilder().startObject().field("name", "advil").field("name_suggest", "advil").endObject();
        XContentBuilder brand2 = jsonBuilder().startObject().field("name", "tylenol").field("name_suggest", "tylenol").endObject();
        XContentBuilder brand3 = jsonBuilder().startObject().field("name", "motrin").field("name_suggest", "motrin").endObject();

        // Index some data
        client.prepareIndex(INDEX, TYPE, "1").setSource(brand1).execute().actionGet();
        client.prepareIndex(INDEX, TYPE, "2").setSource(brand2).execute().actionGet();
        client.prepareIndex(INDEX, TYPE, "3").setSource(brand3).execute().actionGet();

        // Refresh index reader
        client.admin().indices().refresh(Requests.refreshRequest("_all")).actionGet();

        // Prepare and execute query
        QueryBuilder queryBuilder = QueryBuilders.termQuery("name", "advil");

        SearchResponse searchResponse = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(queryBuilder)
                .execute()
                .actionGet();

        // Make sure we got back expected data
        List<String> expected = new ArrayList<String>();
        expected.add("1");

        assertEquals(expected.size(), searchResponse.getHits().getTotalHits());

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            assertTrue(expected.contains(hit.id()));
        }

        // Suggestion test
        SuggestResponse suggestResponse = client.prepareSuggest(INDEX)
                .addSuggestion(new CompletionSuggestionBuilder(INDEX).field("name_suggest").text("ad"))
                .execute().actionGet();

        Iterator<? extends Suggest.Suggestion.Entry.Option> iterator =
                suggestResponse.getSuggest().getSuggestion(INDEX).iterator().next().getOptions().iterator();

        List<String> suggestion = new ArrayList<String>();
        while (iterator.hasNext()) {
            Suggest.Suggestion.Entry.Option next = iterator.next();
            suggestion.add(next.getText().string());
        }
        List<String> expectedSuggestion = new ArrayList<String>();
        expectedSuggestion.add("advil");

        assertEquals(expectedSuggestion.size(), suggestion.size());

        for (String s : suggestion) {
            assertTrue(expectedSuggestion.contains(s));
        }

        // cleanup
        client.close();
        node.close();

        assertTrue(node.isClosed());
    }
}
