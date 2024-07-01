package com.wuqing.ui.bigstore.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.wuqing.ui.bigstore.bean.UiResult;
import com.wuqing.ui.bigstore.config.Config;
import com.wuqing.ui.bigstore.query.QueryUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author wuqing
 */
public class HttpServerHandleAdapter extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpServerHandleAdapter.class);
    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 60;


    /**
     * 资源所在路径
     */
    //private static final String LOCATION = "/tmp/ui-2.0.0";
    private static String LOCATION;

    static {
        //线上运行版本
        LOCATION = System.getProperty("user.dir");
        if (LOCATION.indexOf("wangxu") > -1) {
            LOCATION = "/Users/wangxu/IdeaProjects/bigstore/ui/src/main/resources";
        }
        LOGGER.info("LOCATION:" + LOCATION);
    }

    /**
     * 404文件页面地址
     */
    private static final String NOT_FOUND = LOCATION + "/404.html";


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        // 获取URI
        String uri = request.uri();
        if (uri.endsWith(".do")) {
            String json = "{}";
            if (Config.GET_DATA_BASE_URL.equals(uri)) {
                UiResult<List<String>> result = new UiResult();
                List<String> database = QueryUtil.showDatabase();
                result.setSuccessData(database);
                json = JSON.toJSONString(result);
            } else if (Config.USE_DATA_BASE_URL.equals(uri)) {
                UiResult<String> result = new UiResult();
                Map<String, String> map = getParams(request);
                String database = map.get("database");
                QueryUtil.useDatabase(database);
                result.setSuccessData(database);
                json = JSON.toJSONString(result);
            } else if (Config.QUERY_DATA_BASE_URL.equals(uri)) {
                Map<String, String> map = getParams(request);
                String sql = map.get("sql");
                UiResult<List<List<String>>> result = QueryUtil.query(sql);
                json = JSON.toJSONString(result);
            } else if (Config.ASYNC_STORE_DATA_URL.equals(uri)) {
                Map<String, String> map = getParams(request);
                String table = map.get("table");
                String data = map.get("data");
                List<String> line = JSON.parseObject(data, new TypeReference<List<String>>(){});
                String[] ln = new String[line.size()];
                line.toArray(ln);
                UiResult uiResult = QueryUtil.asyncStoreData(table, ln);
                json = JSON.toJSONString(uiResult);
            } else {
                UiResult uiResult = new UiResult();
                uiResult.setError("uri is invalid");
                json = JSON.toJSONString(uiResult);
            }
            byte[] bytes = json.getBytes();
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(bytes));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
            boolean keepAlive = HttpUtil.isKeepAlive(request);

            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            ctx.write(response);

            ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            if (!keepAlive) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        } else {
            doWithStatic(uri, ctx, request);
        }

    }

    private void doWithStatic(String uri, ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        // 设置不支持favicon.ico文件
        if ("favicon.ico".equals(uri)) {
            return;
        }
        if ("/".equals(uri)) {
            uri = "/index.html";
        }

        // 根据路径地址构建文件
        String path = LOCATION + uri;
        File html = new File(path);

        if (path == null) {
            path = NOT_FOUND;
        }

        File file = new File(path);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            file = new File(NOT_FOUND);
        }

        // Cache Validation
        String ifModifiedSince = request.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
            Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            long fileLastModifiedSeconds = file.lastModified() / 1000;
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                sendNotModified(ctx);
                return;
            }
        }

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException ignore) {
            return;
        }
        long fileLength = raf.length();

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpUtil.setContentLength(response, fileLength);
        setContentTypeHeader(response, file);
        setDateAndCacheHeaders(response, file);
        if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        // Write the initial line and the header.
        ctx.write(response);

        // Write the content.
        ChannelFuture sendFileFuture;
        ChannelFuture lastContentFuture;
        if (ctx.pipeline().get(SslHandler.class) == null) {
            sendFileFuture = ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength),
                    ctx.newProgressivePromise());
            // Write the end marker.
            lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        } else {
            sendFileFuture = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength,
                    8192)), ctx.newProgressivePromise());
            // HttpChunkedInput will write the end marker (LastHttpContent) for us.
            lastContentFuture = sendFileFuture;
        }

        sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                if (total < 0) { // total unknown
                    System.err.println(future.channel() + " Transfer progress: " + progress);
                } else {
                    System.err.println(future.channel() + " Transfer progress: " + progress + " / " + total);
                }
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
                System.err.println(future.channel() + " Transfer complete.");
            }
        });

        // Decide whether to close the connection or not.
        if (!HttpUtil.isKeepAlive(request)) {
            // Close the connection when the whole content is written out.
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }

    }

    private static void send100Continue(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE);
        ctx.writeAndFlush(response);
    }

    private static Map<String, String> getParams(FullHttpRequest fullReq) throws IOException {
        HttpMethod method = fullReq.method();

        Map<String, String> parmMap = new HashMap<>();

        if (HttpMethod.GET == method) {
            // 是GET请求
            QueryStringDecoder decoder = new QueryStringDecoder(fullReq.uri());
            decoder.parameters().entrySet().forEach( entry -> {
                // entry.getValue()是一个List, 只取第一个元素
                parmMap.put(entry.getKey(), entry.getValue().get(0));
            });
        } else if (HttpMethod.POST == method) {
            // 是POST请求
            HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(fullReq);
            decoder.offer(fullReq);

            List<InterfaceHttpData> parmList = decoder.getBodyHttpDatas();

            for (InterfaceHttpData parm : parmList) {
                Attribute data = (Attribute) parm;
                parmMap.put(data.getName(), data.getValue());
            }
            //fullReq.content().toString(CharsetUtil.UTF_8);
        } else {
            // 不支持其它方法
            throw new RuntimeException("type of httpMethod is invalid"); // 这是个自定义的异常, 可删掉这一行
        }

        return parmMap;
    }

    private static void setContentTypeHeader(HttpResponse response, File file) {
        /*MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));*/
        String filePath = file.getPath();
        if (filePath.endsWith(".html")){
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
        } else if (filePath.endsWith(".js")){
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/x-javascript");
        } else if (filePath.endsWith(".css")){
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/css; charset=UTF-8");
        } else {
            MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
        }
    }

    private static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        // Date header
        Calendar time = new GregorianCalendar();
        response.headers().set(HttpHeaderNames.DATE, dateFormatter.format(time.getTime()));

        // Add cache headers
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(HttpHeaderNames.EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers().set(
                HttpHeaderNames.LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static void sendNotModified(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_MODIFIED);
        setDateHeader(response);

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static void setDateHeader(FullHttpResponse response) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        Calendar time = new GregorianCalendar();
        response.headers().set(HttpHeaderNames.DATE, dateFormatter.format(time.getTime()));
    }


}
