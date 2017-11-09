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
package com.meltwater.elasticsearch.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Indexes {@link ParsedDocument}s into
 * a {@link RAMDirectory}
 */
public class RamDirectoryPercolatorIndex {

    private final String documentType;
    private final MapperService mapperService;

    public RamDirectoryPercolatorIndex(String documentType, MapperService mapperService) {
        this.documentType = documentType;
        this.mapperService = mapperService;
    }

    public Directory indexDocuments(List<ParsedDocument> parsedDocuments) {
        try{
            Directory directory = new RAMDirectory();
            PerFieldAnalyzerWrapper analyzer = getPerFieldAnalyzerWrapper();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            IndexWriter iwriter = new IndexWriter(directory, conf);
            for(ParsedDocument document : parsedDocuments){
                for(ParseContext.Document doc : document.docs()){
                    iwriter.addDocument(doc);
                }
            }
            iwriter.close();
            return directory;
        } catch(IOException e) {
            throw new ElasticsearchException("Failed to write documents to RAMDirectory", e);
        }
    }

    private PerFieldAnalyzerWrapper getPerFieldAnalyzerWrapper() {

        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        DocumentMapper mapping =  mapperService.documentMapper(documentType);
        for (FieldMapper field : mapping.mappers()){
            if (field.fieldType().tokenized() &&
                field.fieldType().indexAnalyzer() != null &&
                field.fieldType().indexAnalyzer().name() != "default")
                fieldAnalyzers.put(field.name(), field.fieldType().indexAnalyzer().analyzer());
        }

        NamedAnalyzer defaultAnalyzer = mapperService.analysisService().defaultIndexAnalyzer();
        return new PerFieldAnalyzerWrapper(defaultAnalyzer, fieldAnalyzers);
    }
}
