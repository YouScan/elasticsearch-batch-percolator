package io.youscan.elasticsearch.action;


import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class YPercolateRequest extends BroadcastRequest<YPercolateRequest> implements CompositeIndicesRequest {

    private String documentType;
    private String routing;
    private String preference;
    private GetRequest getRequest;
    private boolean onlyCount;

    private BytesReference source;

    private BytesReference docSource;

    // Used internally in order to compute tookInMillis, TransportBroadcastAction itself doesn't allow
    // to hold it temporarily in an easy way
    long startTime;

    public YPercolateRequest() {
    }

    YPercolateRequest(YPercolateRequest request, BytesReference docSource) {
        super(request);
        this.indices = request.indices();
        this.documentType = request.documentType();
        this.routing = request.routing();
        this.preference = request.preference();
        this.source = request.source;
        this.docSource = docSource;
        this.onlyCount = request.onlyCount;
        this.startTime = request.startTime;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        List<IndicesRequest> requests = new ArrayList<>();
        requests.add(this);
        if (getRequest != null) {
            requests.add(getRequest);
        }
        return requests;
    }

    public String documentType() {
        return documentType;
    }

    public YPercolateRequest documentType(String type) {
        this.documentType = type;
        return this;
    }

    public String routing() {
        return routing;
    }

    public YPercolateRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    public String preference() {
        return preference;
    }

    public YPercolateRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public GetRequest getRequest() {
        return getRequest;
    }

    public YPercolateRequest getRequest(GetRequest getRequest) {
        this.getRequest = getRequest;
        return this;
    }

    public BytesReference source() {
        return source;
    }

    public YPercolateRequest source(Map document) throws ElasticsearchGenerationException {
        return source(document, Requests.CONTENT_TYPE);
    }

    @SuppressWarnings("unchecked")
    public YPercolateRequest source(Map document, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(document);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + document + "]", e);
        }
    }

    public YPercolateRequest source(String document) {
        this.source = new BytesArray(document);
        return this;
    }

    public YPercolateRequest source(XContentBuilder documentBuilder) {
        source = documentBuilder.bytes();
        return this;
    }

    public YPercolateRequest source(byte[] document) {
        return source(document, 0, document.length);
    }

    public YPercolateRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    public YPercolateRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    public YPercolateRequest source(PercolateSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        return this;
    }

    public boolean onlyCount() {
        return onlyCount;
    }

    public YPercolateRequest onlyCount(boolean onlyCount) {
        this.onlyCount = onlyCount;
        return this;
    }

    BytesReference docSource() {
        return docSource;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (documentType == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null && getRequest == null) {
            validationException = addValidationError("source or get is missing", validationException);
        }
        if (getRequest != null && getRequest.fields() != null) {
            validationException = addValidationError("get fields option isn't supported via percolate request", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startTime = in.readVLong();
        documentType = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        source = in.readBytesReference();
        docSource = in.readBytesReference();
        if (in.readBoolean()) {
            getRequest = new GetRequest(null);
            getRequest.readFrom(in);
        }
        onlyCount = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(startTime);
        out.writeString(documentType);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBytesReference(source);
        out.writeBytesReference(docSource);
        if (getRequest != null) {
            out.writeBoolean(true);
            getRequest.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(onlyCount);
    }
}
