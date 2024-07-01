package com.wuqing.business.bigstore.lucene;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.wuqing.client.bigstore.bean.LineRange;
import com.wuqing.client.bigstore.util.CommonUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wuqing on 16/5/20.
 */
public class LuceneUtil {

    private final static Logger logger = LoggerFactory.getLogger(LuceneUtil.class);

    private static final String INDEX_DIR_LOCAL = "/Users/wuqing/index";

    private static final String NUMBER = "number";

    private static final String CONTEXT = "context";

    private static Map<String, DirectoryReader> readMap = new ConcurrentLinkedHashMap.Builder<String, DirectoryReader>()
    .maximumWeightedCapacity(10).listener(new EvictionListener<String, DirectoryReader>() {
        @Override
        public void onEviction(String key, DirectoryReader value) {  //添加监听事件，当对象被释放后，主动释放内存，而不是等待full gc后再释放
            try {
                if (value != null) {
                    value.close();
                }
            } catch (Exception e) {
                logger.error("close lucene directoryReader fail.", e);
            }
        }
    }).build();    //缓存100个reader

    private static Map<String, IndexWriter> writeMap = new ConcurrentLinkedHashMap.Builder<String, IndexWriter>()
            .maximumWeightedCapacity(2).listener(new EvictionListener<String, IndexWriter>() {
                @Override
                public void onEviction(String key, IndexWriter value) {  //添加监听事件，当对象被释放后，主动释放内存，而不是等待full gc后再释放
                    try {
                        if (value != null) {
                            value.close();
                        }
                    } catch (Exception e) {
                        logger.error("close lucene directoryReader fail.", e);
                    }
                }
            }).build();    //缓存100个reader

    private static synchronized IndexReader getReader(String path) throws IOException {
        DirectoryReader reader = readMap.get(path);
        if (reader == null) {
            Directory dir = FSDirectory.open(Paths.get(path));
            reader = DirectoryReader.open(dir);
            readMap.put(path, reader);
            return reader;
        }
        DirectoryReader readerNew = DirectoryReader.openIfChanged(reader);
        if (readerNew == null) {
            return reader;
        }
        reader = readerNew; //更新
        readMap.put(path, reader);
        return reader;
    }

    private static synchronized IndexWriter getWriter(String path) throws IOException {
        IndexWriter writer = writeMap.get(path);
        if (writer == null) {
            Directory directory = FSDirectory.open(Paths.get(getIndexDir(path)));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setRAMBufferSizeMB(512);
            writer = new IndexWriter(directory, iwc);
            writeMap.put(path, writer);
        }
        return writer;
    }

    /**
     * 创建文件夹等操作
     * @param dirPath
     * @return
     */
    private static String getIndexDir(String dirPath) {
        File f = new File(dirPath);
        if (!f.exists()) {
            f.mkdir();
        }
        return dirPath;
    }

    /**
     * 批量创建索引
     * @param indexDir
     * @param datas
     * @return
     */
    public static boolean batchCreateIndex(String indexDir, List<LuceneBean> datas) {
        try {
            IndexWriter writer = getWriter(indexDir);
            List<Document> docs = new ArrayList<Document>();
            for (LuceneBean b : datas) {
                Document document = new Document();
                document.add(new LongField(NUMBER, b.getNumber(), Field.Store.YES));
                document.add(new TextField(CONTEXT, b.getContext(), Field.Store.NO));
                docs.add(document);
            }
            writer.addDocuments(docs);
            writer.commit();
            return true;
        } catch (Exception e) {
            logger.error("create index fail", e);
        } finally {
        }
        return false;
    }

    /**
     * 创建索引
     * @param indexDir
     * @param number
     * @param context
     * @return
     */
    public static boolean createIndex(String indexDir, int number, String context) {
        IndexWriter writer = null;
        Directory directory = null;
        try {
            directory = FSDirectory.open(Paths.get(getIndexDir(indexDir)));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            writer = new IndexWriter(directory, iwc);
            Document document = new Document();
            document.add(new IntField("number", number, Field.Store.YES));
            document.add(new TextField("context", context, Field.Store.NO));
            writer.addDocument(document);
            return true;
        } catch (Exception e) {
            logger.error("create index fail", e);
        } finally {
            CommonUtil.close(writer);
            CommonUtil.close(directory);
        }
        return false;
    }

    public static LuceneResult search(String indexDir, int start, int limit, List<LineRange> numberRanges, String... searchs) {
        return query(indexDir, start, limit, numberRanges, searchs, null);
    }

    public static LuceneResult like(String indexDir, int start, int limit, List<LineRange> numberRanges, String... likes) {
        return query(indexDir, start, limit, numberRanges, null, likes);
    }

    /**
     * 全文检索，模糊匹配
     * @param indexDir
     * @param searchs 搜索，基于分词
     * @return
     */
    public static LuceneResult query(String indexDir, int start, int limit, List<LineRange> numberRanges, String[] searchs, String[] likes) {
        List<Long> result = new ArrayList<Long>();
        long total = 0;
        try {
            IndexSearcher is = new IndexSearcher(getReader(indexDir));
            BooleanQuery booleanQuery = new BooleanQuery();
            if (searchs != null) {
                for (String s : searchs) {
                    booleanQuery.add(new TermQuery(new Term(CONTEXT, s)), BooleanClause.Occur.MUST);
                }
            }
            if (likes != null) {
                for (String l : likes) {
                    booleanQuery.add(new WildcardQuery(new Term(CONTEXT, l)), BooleanClause.Occur.MUST);
                }
            }
            if (!CommonUtil.isEmpty(numberRanges)) {
                BooleanQuery numberQuery = new BooleanQuery();
                for (LineRange nm : numberRanges) {
                    numberQuery.add(NumericRangeQuery.newLongRange(NUMBER, nm.getStart(), nm.getEnd(), true, true), BooleanClause.Occur.SHOULD);
                }
                booleanQuery.add(numberQuery, BooleanClause.Occur.MUST);
            }
            //Query query1 = ;
            if (limit == 0) {
                total = is.count(booleanQuery);
            } else {
                TopDocs topDocs = is.search(booleanQuery, start + limit);  //为了避免内存暴掉，设置limit=10000
                //System.out.println("总共匹配多少个：" + topDocs.totalHits);
                total = topDocs.totalHits;
                ScoreDoc[] hits = topDocs.scoreDocs;
                int size = 0;
                for (int i = start, k = hits.length; i < k; i++) {
                    Document document = is.doc(hits[i].doc);
                    result.add(Long.parseLong(document.get("number")));
                    if (++size >= limit) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("search fail", e);
        }
        return new LuceneResult(total, result);
    }

    public static void main(String[] args) throws Exception {
        long s = System.currentTimeMillis();
        /*String dataPath = "/Users/wuqing/data/apollo_request";
        List<String> dataList = FileUtil.readAll(dataPath, false);
        int iii = 0;
        for (int ii = 0; ii < 10; ii++) {
            List<LuceneBean> datas = new ArrayList<LuceneBean>();
            for (int i = 0, k = dataList.size(); i < k; i++) {
                datas.add(new LuceneBean(iii++, dataList.get(i)));
            }
            batchCreateIndex(INDEX_DIR_LOCAL, datas);
        }*/

        for (int i = 0; i < 10; i++) {
            s = System.currentTimeMillis();
            //List<String> list = search(INDEX_DIR_LOCAL, "221.229.231.96");
            LuceneResult result = search(INDEX_DIR_LOCAL, 0, 10, CommonUtil.asList(new LineRange(0, 100000)), "121.239.179.125");
            //System.out.println("size:" + list.size() + ", time:" + (System.currentTimeMillis() - s));
        }
    }
}
