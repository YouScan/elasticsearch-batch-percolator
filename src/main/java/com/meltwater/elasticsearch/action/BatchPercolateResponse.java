/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.meltwater.elasticsearch.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.action.support.RestActions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Response from a {@link BatchPercolateRequest}
 */
public class BatchPercolateResponse extends BroadcastResponse implements Streamable, Iterable<BatchPercolateResponseItem>, ToXContent {

    private List<BatchPercolateResponseItem> results;
    private long tookMs;

    public BatchPercolateResponse(){}

    BatchPercolateResponse(List<BatchPercolateResponseItem> results, long tookInMillis, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.results = results;
        this.tookMs = tookInMillis;
    }

    public List<BatchPercolateResponseItem> getResults() {
        return this.results;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookMs = in.readVLong();
        int size = in.readVInt();
        this.results= new ArrayList<>(size);
        for(int i = 0; i < size; i++){
            BatchPercolateResponseItem item = new BatchPercolateResponseItem();
            item.readFrom(in);
            results.add(item);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookMs);
        out.writeVInt(results.size());
        for (BatchPercolateResponseItem item : results) {
            item.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOOK, tookMs);
        RestActions.buildBroadcastShardsHeader(builder, params, this);

        builder.startArray("results");
        for (BatchPercolateResponseItem result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public Iterator<BatchPercolateResponseItem> iterator() {
        return results.iterator();
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
    }

}
