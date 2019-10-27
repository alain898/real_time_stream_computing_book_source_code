package com.alain898.book.realtimestreaming.chapter2.datacollector.netty;

import com.alain898.book.realtimestreaming.common.ExecutorHelper;
import com.alain898.book.realtimestreaming.common.RestHelper;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriter;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriterMock;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by alain on 18/5/8.
 */
public class AsyncServerHandler extends
        SimpleChannelInboundHandler<HttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(NettyDataCollector.class);

    private final String kafkaBroker = "127.0.0.1:9092";
    private final String topic = "collector_event";
    private final KafkaWriter kafkaWriter = new KafkaWriterMock(kafkaBroker);


    // step1: 对消息进行解码
    private JSONObject decode(ChannelHandlerContext ctx, HttpRequest req) {
        logger.info(String.format("decode thread[%s]", Thread.currentThread().toString()));

        if (!req.getDecoderResult().isSuccess()) {
            throw new RequestException(BAD_REQUEST.code(),
                    RestHelper.genResponseString(BAD_REQUEST.code(), "非法请求"));
        }

        if (OPTIONS.equals(req.getMethod())) {
            throw new RequestException(OK.code(),
                    RestHelper.genResponseString(OK.code(), "OPTIONS"));
        }

        if (req.getMethod() != POST) {
            throw new RequestException(METHOD_NOT_ALLOWED.code(),
                    RestHelper.genResponseString(METHOD_NOT_ALLOWED.code(), "方法不允许"));
        }

        String uri = req.getUri();
        if (!"/event".equals(uri)) {
            throw new RequestException(BAD_REQUEST.code(),
                    RestHelper.genResponseString(BAD_REQUEST.code(), "非法请求路径"));
        }

        byte[] body = readRequestBodyAsString((HttpContent) req);
        String jsonString = new String(body, Charsets.UTF_8);
        return JSON.parseObject(jsonString);
    }

    // step2: 对消息进行抽取、清洗、转化
    private JSONObject doExtractCleanTransform(ChannelHandlerContext ctx, HttpRequest req,
                                               JSONObject event) {
        logger.info(String.format("doExtractCleanTransform thread[%s]", Thread.currentThread().toString()));
        Preconditions.checkNotNull(event, "event is null");

        // TODO: 实现抽取、清洗、转化具体逻辑
        return event;
    }

    // step3: 将格式规整化的消息发到消息中间件kafka
    private Void send(ChannelHandlerContext ctx, HttpRequest req,
                      JSONObject event) {
        logger.info(String.format("send thread[%s]", Thread.currentThread().toString()));
        Preconditions.checkNotNull(event, "event is null");

        try {
            kafkaWriter.send(topic, event.toJSONString().getBytes(Charsets.UTF_8));
            sendResponse(ctx, OK,
                    RestHelper.genResponseString(200, "ok"));
        } catch (Exception e) {
            logger.error(String.format("exception caught, normEvent[%s]", event), e);
            sendResponse(ctx, INTERNAL_SERVER_ERROR,
                    RestHelper.genResponseString(500, "发送失败"));
        }
        return null;
    }

    final private Executor decoderExecutor = ExecutorHelper.createExecutor(2, "decoder");
    final private Executor ectExecutor = ExecutorHelper.createExecutor(8, "ect");
    final private Executor senderExecutor = ExecutorHelper.createExecutor(2, "sender");

    private static class RefController {
        private final ChannelHandlerContext ctx;
        private final HttpRequest req;

        public RefController(ChannelHandlerContext ctx, HttpRequest req) {
            this.ctx = ctx;
            this.req = req;
        }

        public void retain() {
            ReferenceCountUtil.retain(ctx);
            ReferenceCountUtil.retain(req);
        }

        public void release() {
            ReferenceCountUtil.release(req);
            ReferenceCountUtil.release(ctx);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req)
            throws Exception {
        logger.info(String.format("current thread[%s]", Thread.currentThread().toString()));
        final RefController refController = new RefController(ctx, req);
        refController.retain();
        CompletableFuture
                .supplyAsync(() -> this.decode(ctx, req), this.decoderExecutor)
                .thenApplyAsync(e -> this.doExtractCleanTransform(ctx, req, e), this.ectExecutor)
                .thenApplyAsync(e -> this.send(ctx, req, e), this.senderExecutor)
                .thenAccept(v -> refController.release())
                .exceptionally(e -> {
                    try {
                        logger.error("exception caught", e);
                        if (RequestException.class.isInstance(e.getCause())) {
                            RequestException re = (RequestException) e.getCause();
                            sendResponse(ctx, HttpResponseStatus.valueOf(re.getCode()), re.getResponse());
                        } else {
                            sendResponse(ctx, INTERNAL_SERVER_ERROR,
                                    RestHelper.genResponseString(500, "服务器内部错误"));
                        }
                        return null;
                    } finally {
                        refController.release();
                    }
                });

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        if (ctx.channel().isActive()) {
            sendResponse(ctx, INTERNAL_SERVER_ERROR,
                    RestHelper.genResponseString(500, "服务器内部错误"));
        }
    }


    private static void setAllowDomain(FullHttpResponse response) {
        response.headers().set("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        response.headers().set("Access-Control-Max-Age", "3600");
        response.headers().set("Access-Control-Allow-Credentials", "true");
    }

    private static void sendResponse(ChannelHandlerContext ctx,
                                     HttpResponseStatus status,
                                     String msg) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
        setAllowDomain(response);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }


    private byte[] readRequestBodyAsString(HttpContent httpContent) {
        ByteBuf byteBuf = httpContent.content();
        byte[] data = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(data);
        return data;
    }
}
