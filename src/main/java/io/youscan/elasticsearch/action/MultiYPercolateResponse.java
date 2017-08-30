package io.youscan.elasticsearch.action;

import com.google.common.collect.Iterators;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;

public class MultiYPercolateResponse extends ActionResponse implements Iterable<MultiYPercolateResponse.Item>, ToXContent {

    private Item[] items;

    MultiYPercolateResponse(Item[] items) {
        this.items = items;
    }

    MultiYPercolateResponse() {
        this.items = new Item[0];

    }

    @Override
    public Iterator<Item> iterator() {
        return Iterators.forArray(items);
    }

    public Item[] items() {
        return items;
    }

    public Item[] getItems() {
        return items;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.RESPONSES);
        for (Item item : items) {
            builder.startObject();
            if (item.isFailure()) {
                ElasticsearchException.renderThrowable(builder, params, item.getFailure());
            } else {
                item.getResponse().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(items.length);
        for (Item item : items) {
            item.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        items = new Item[size];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item();
            items[i].readFrom(in);
        }
    }

    public static class Item implements Streamable {

        private YPercolateResponse response;
        private Throwable throwable;

        Item(YPercolateResponse response) {
            this.response = response;
        }

        Item(Throwable Throwable) {
            this.throwable = Throwable;
        }

        Item() {
        }

        @Nullable
        public YPercolateResponse getResponse() {
            return response;
        }

        @Nullable
        public String getErrorMessage() {
            return throwable == null ? null : throwable.getMessage();
        }

        public boolean isFailure() {
            return throwable != null;
        }

        public Throwable getFailure() {
            return throwable;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                response = new YPercolateResponse();
                response.readFrom(in);
            } else {
                throwable = in.readThrowable();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (response != null) {
                out.writeBoolean(true);
                response.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeThrowable(throwable);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString RESPONSES = new XContentBuilderString("responses");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
    }
}
