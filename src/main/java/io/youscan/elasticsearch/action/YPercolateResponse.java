package io.youscan.elasticsearch.action;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.highlight.HighlightField;

import java.io.IOException;
import java.util.*;

public class YPercolateResponse  extends BroadcastResponse implements Iterable<YPercolateResponse.Match>, ToXContent {

    public static final Match[] EMPTY = new Match[0];

    private long tookInMillis;
    private Match[] matches;
    private long count;
    private InternalAggregations aggregations;


    YPercolateResponse(
            int totalShards,
            int successfulShards,
            int failedShards,
            List<ShardOperationFailedException> shardFailures,
            Match[] matches,
            long count,
            long tookInMillis,
            InternalAggregations aggregations
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);

        if (tookInMillis < 0) {
            throw new IllegalArgumentException("tookInMillis must be positive but was: " + tookInMillis);
        }

        this.tookInMillis = tookInMillis;
        this.matches = matches;
        this.count = count;
        this.aggregations = aggregations;
    }

    YPercolateResponse(int totalShards,
                       int successfulShards,
                       int failedShards,
                       List<ShardOperationFailedException> shardFailures,
                       long tookInMillis,
                       Match[] matches
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);

        if (tookInMillis < 0) {
            throw new IllegalArgumentException("tookInMillis must be positive but was: " + tookInMillis);
        }

        this.tookInMillis = tookInMillis;
        this.matches = matches;
    }

    YPercolateResponse() {
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public Match[] getMatches() {
        return this.matches;
    }

    public long getCount() {
        return count;
    }

    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public Iterator<Match> iterator() {
        return Arrays.asList(matches).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOOK, tookInMillis);
        RestActions.buildBroadcastShardsHeader(builder, params, this);

        builder.field(Fields.TOTAL, count);
        if (matches != null) {
            builder.startArray(Fields.MATCHES);
            boolean justIds = "ids".equals(params.param("percolate_format"));
            if (justIds) {
                for (Match match : matches) {
                    builder.value(match.getId());
                }
            } else {
                for (Match match : matches) {
                    builder.startObject();
                    builder.field(Fields._INDEX, match.getIndex());
                    builder.field(Fields._ID, match.getId());
                    float score = match.getScore();

                    // TODO Get rid of this
                    if (score != PercolatorService.NO_SCORE) {
                        builder.field(Fields._SCORE, match.getScore());
                    }

                    if (match.getHighlightFields() != null) {
                        builder.startObject(Fields.HIGHLIGHT);
                        for (HighlightField field : match.getHighlightFields().values()) {
                            builder.field(field.name());
                            if (field.fragments() == null) {
                                builder.nullValue();
                            } else {
                                builder.startArray();
                                for (Text fragment : field.fragments()) {
                                    builder.value(fragment);
                                }
                                builder.endArray();
                            }
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
        }

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        count = in.readVLong();
        int size = in.readVInt();
        if (size != -1) {
            matches = new Match[size];
            for (int i = 0; i < size; i++) {
                matches[i] = new Match();
                matches[i].readFrom(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeVLong(count);
        if (matches == null) {
            out.writeVInt(-1);
        } else {
            out.writeVInt(matches.length);
            for (Match match : matches) {
                match.writeTo(out);
            }
        }
    }
    public static class Match implements Streamable {

        private Text index;
        private Text id;
        private float score;
        private Map<String, HighlightField> hl;

        /**
         * Constructor only for internal usage.
         */
        public Match(Text index, Text id, float score, Map<String, HighlightField> hl) {
            this.id = id;
            this.score = score;
            this.index = index;
            this.hl = hl;
        }

        /**
         * Constructor only for internal usage.
         */
        public Match(Text index, Text id, float score) {
            this.id = id;
            this.score = score;
            this.index = index;
        }

        Match() {
        }

        /**
         * @return The index that the matched percolator query resides in.
         */
        public Text getIndex() {
            return index;
        }

        /**
         * @return The id of the matched percolator query.
         */
        public Text getId() {
            return id;
        }

        /**
         * @return If in the percolate request a query was specified this returns the score representing how well that
         * query matched on the metadata associated with the matching query otherwise {@link Float#NaN} is returned.
         */
        public float getScore() {
            return score;
        }

        /**
         * @return If highlighting was specified in the percolate request the this returns highlight snippets for each
         * matching field in the document being percolated based on this query otherwise <code>null</code> is returned.
         */
        @Nullable
        public Map<String, HighlightField> getHighlightFields() {
            return hl;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readText();
            index = in.readText();
            score = in.readFloat();
            int size = in.readVInt();
            if (size > 0) {
                hl = new HashMap<>(size);
                for (int j = 0; j < size; j++) {
                    hl.put(in.readString(), HighlightField.readHighlightField(in));
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeText(id);
            out.writeText(index);
            out.writeFloat(score);
            if (hl != null) {
                out.writeVInt(hl.size());
                for (Map.Entry<String, HighlightField> entry : hl.entrySet()) {
                    out.writeString(entry.getKey());
                    entry.getValue().writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _SCORE = new XContentBuilderString("_score");
        static final XContentBuilderString HIGHLIGHT = new XContentBuilderString("highlight");
    }
}
