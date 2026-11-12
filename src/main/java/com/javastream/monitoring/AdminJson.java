package com.javastream.monitoring;

import com.javastream.admin.BrokerAdminService;
import com.javastream.topic.TopicMetadata;

import java.util.List;
import java.util.Map;

/** Minimal JSON serializer for admin HTTP responses (no third-party JSON library). */
final class AdminJson {

    private AdminJson() {}

    static String encode(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String s) {
            return "\"" + escape(s) + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof List<?> list) {
            StringBuilder builder = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append(encode(list.get(i)));
            }
            return builder.append(']').toString();
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder builder = new StringBuilder("{");
            int i = 0;
            for (var entry : map.entrySet()) {
                if (i++ > 0) {
                    builder.append(',');
                }
                builder.append('"').append(escape(String.valueOf(entry.getKey()))).append("\":");
                builder.append(encode(entry.getValue()));
            }
            return builder.append('}').toString();
        }
        if (value instanceof TopicMetadata metadata) {
            return encodeTopicMetadata(metadata);
        }
        if (value instanceof BrokerAdminService.BrokerInfo info) {
            return encodeBrokerInfo(info);
        }
        if (value instanceof BrokerAdminService.GroupDescription group) {
            return encodeGroup(group);
        }
        if (value instanceof BrokerAdminService.ConsumerLagEntry lag) {
            return encodeLag(lag);
        }
        return "\"" + escape(value.toString()) + "\"";
    }

    private static String encodeTopicMetadata(TopicMetadata metadata) {
        return "{"
                + "\"topicName\":\"" + escape(metadata.topicName()) + "\","
                + "\"partitionCount\":" + metadata.partitionCount() + ","
                + "\"partitions\":" + encode(metadata.partitions())
                + "}";
    }

    private static String encodeBrokerInfo(BrokerAdminService.BrokerInfo info) {
        return "{"
                + "\"brokerId\":" + info.brokerId() + ","
                + "\"host\":\"" + escape(info.host()) + "\","
                + "\"clientPort\":" + info.clientPort() + ","
                + "\"replicationPort\":" + info.replicationPort() + ","
                + "\"state\":\"" + escape(info.state()) + "\","
                + "\"uptimeSeconds\":" + info.uptimeSeconds()
                + "}";
    }

    private static String encodeGroup(BrokerAdminService.GroupDescription group) {
        return "{"
                + "\"groupId\":\"" + escape(group.groupId()) + "\","
                + "\"topic\":\"" + escape(group.topic()) + "\","
                + "\"generation\":" + group.generation() + ","
                + "\"members\":" + encode(group.members())
                + "}";
    }

    private static String encodeLag(BrokerAdminService.ConsumerLagEntry lag) {
        return "{"
                + "\"partition\":" + lag.partition() + ","
                + "\"committedOffset\":" + lag.committedOffset() + ","
                + "\"logEndOffset\":" + lag.logEndOffset() + ","
                + "\"lag\":" + lag.lag()
                + "}";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
