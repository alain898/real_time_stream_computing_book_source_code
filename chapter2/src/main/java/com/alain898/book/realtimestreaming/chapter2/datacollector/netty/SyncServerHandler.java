package com.alain898.book.realtimestreaming.chapter2.datacollector.netty;

import com.alain898.book.realtimestreaming.common.RestHelper;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriter;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriterMock;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by alain on 18/5/7.
 */
public class SyncServerHandler extends
        SimpleChannelInboundHandler<HttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(NettyDataCollector.class);

    private final String kafkaBroker = "127.0.0.1:9092";
    private final String topic = "collector_event";
    private final KafkaWriter kafkaWriter = new KafkaWriterMock(kafkaBroker);

    private JSONObject doExtractCleanTransform(JSONObject event) {
        // TODO: 实现抽取、清洗、转化具体逻辑
        return event;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req)
            throws Exception {
        if (!req.getDecoderResult().isSuccess()) {
            sendResponse(ctx, BAD_REQUEST,
                    RestHelper.genResponseString(400, "非法请求"));
            return;
        }

        if (OPTIONS.equals(req.getMethod())) {
            sendResponse(ctx, OK,
                    RestHelper.genResponseString(200, "OPTIONS"));
            return;
        }

        if (req.getMethod() != POST) {
            sendResponse(ctx, METHOD_NOT_ALLOWED,
                    RestHelper.genResponseString(405, "方法不允许"));
            return;
        }

        String uri = req.getUri();
        if (!"/event".equals(uri)) {
            sendResponse(ctx, BAD_REQUEST,
                    RestHelper.genResponseString(400, "非法请求路径"));
            return;
        }

        byte[] body = readRequestBodyAsString((HttpContent) req);
        // step1: 对消息进行解码
        JSONObject bodyJson;
        try {
            bodyJson = JSONObject.parseObject(new String(body, Charsets.UTF_8));
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("request bodyJson[%s]", bodyJson));
            }
        } catch (Exception e) {
            logger.error(String.format("exception caught, body[%s]", JSONObject.toJSONString(body)), e);
            sendResponse(ctx, BAD_REQUEST,
                    RestHelper.genResponseString(400, "非法请求"));
            return;
        }

        // step2: 对消息进行抽取、清洗、转化
        JSONObject normEvent;
        try {
            normEvent = doExtractCleanTransform(bodyJson);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("normEvent[%s]", normEvent));
            }
        } catch (Exception e) {
            logger.error(String.format("exception caught, bodyJson[%s]", bodyJson), e);
            sendResponse(ctx, INTERNAL_SERVER_ERROR,
                    RestHelper.genResponseString(500, "转化失败"));
            return;
        }

        // step3: 将格式规整化的消息发到消息中间件kafka
        try {
            if (normEvent != null) {
                kafkaWriter.send(topic, normEvent.toJSONString().getBytes(Charsets.UTF_8));
            }
            sendResponse(ctx, OK,
                    RestHelper.genResponseString(200, "ok"));
        } catch (Exception e) {
            logger.error(String.format("exception caught, normEvent[%s]", normEvent), e);
            sendResponse(ctx, INTERNAL_SERVER_ERROR,
                    RestHelper.genResponseString(500, "发送失败"));
        }

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
