package io.youscan.elasticsearch.shard;

import com.google.common.collect.Maps;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;

import java.io.IOException;
import java.util.Map;

public class YQueriesLoaderCollector  implements Collector, LeafCollector {

    public static final String ID_FIELD = "ysid"; //We're using custom id field. Elastic ignores "id"
    private final Map<String, QueryAndSource> queries = Maps.newHashMap();
    private final FieldsVisitor fieldsVisitor;
    private final YPercolatorQueriesRegistry percolator;
    private final IndexFieldData idFieldData;
    private final ESLogger logger;

    private SortedBinaryDocValues idValues;
    private LeafReader reader;

    YQueriesLoaderCollector(YPercolatorQueriesRegistry percolator, ESLogger logger, MapperService mapperService, IndexFieldDataService indexFieldDataService) {
        this.percolator = percolator;
        this.logger = logger;

        final MappedFieldType idMapper = mapperService.smartNameFieldType(ID_FIELD); //we use our own ID field.
        this.idFieldData = indexFieldDataService.getForField(idMapper);

        this.fieldsVisitor = new FieldsVisitor(true){
            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                    return Status.YES;
                }
                return source != null ? Status.STOP : Status.NO;
            }
        };
    }

    public Map<String, QueryAndSource> queries() {
        return this.queries;
    }

    @Override
    public void collect(int doc) throws IOException {
        idValues.setDocument(doc);
        if (idValues.count() > 0) {
            assert idValues.count() == 1;
            BytesRef id = idValues.valueAt(0);
            fieldsVisitor.reset();
            reader.document(doc, fieldsVisitor);
            try {
                // id is only used for logging, if we fail we log the id in the catch statement
                final QueryAndSource queryAndSource = percolator.parsePercolatorDocument(null, fieldsVisitor.source());
                queries.put(id.utf8ToString(), queryAndSource);
            } catch (Exception e) {
                logger.warn("failed to add query [{}]", e, id.utf8ToString());
            }

        } else {
            logger.error("failed to load query since field [{}] not present", ID_FIELD);
        }
    }

    @Override
    public final LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        doSetNextReader(context);
        return this;
    }

    private void doSetNextReader(LeafReaderContext context) throws IOException {
        reader = context.reader();
        idValues = idFieldData.load(context).getBytesValues();
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        // no-op by default
    }
}
