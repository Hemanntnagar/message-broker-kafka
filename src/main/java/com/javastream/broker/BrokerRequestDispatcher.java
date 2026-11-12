package com.javastream.broker;

import com.javastream.admin.BrokerAdminService;
import com.javastream.delivery.IdempotentSequenceException;
import com.javastream.delivery.ProducerIdempotencyCache;
import com.javastream.exception.ProtocolDecodeException;
import com.javastream.logging.BrokerRequestLogging;
import com.javastream.metrics.BrokerMetrics;
import com.javastream.monitoring.MonitoringBundle;
import com.javastream.partition.PartitionId;
import com.javastream.protocol.OperationCode;
import com.javastream.protocol.ProtocolConstants;
import com.javastream.protocol.RecordPayloadCodec;
import com.javastream.protocol.Request;
import com.javastream.protocol.Response;
import com.javastream.protocol.StatusCode;
import com.javastream.storage.BrokerMessageStore;
import com.javastream.storage.BrokerMessageStore.StoredRecord;
import com.javastream.security.ConnectionSecurityContext;
import com.javastream.security.MessageSizeValidator;
import com.javastream.security.SecurityPolicy;
import com.javastream.security.SecuritySettings;
import com.javastream.topic.TopicMetadata;
import com.javastream.topic.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Handles produce/fetch/topic operations for connected TCP clients. */
public final class BrokerRequestDispatcher {

    private static final Logger log = LoggerFactory.getLogger(BrokerRequestDispatcher.class);

    private final BrokerServices services;
    private final BrokerMetrics metrics;
    private final BrokerAdminService adminService;
    private final com.javastream.admin.AdminAuthority adminAuthority;
    private final SecurityPolicy securityPolicy;
    private final MessageSizeValidator messageSizeValidator;

    public BrokerRequestDispatcher(BrokerServices services) {
        this(services, null);
    }

    public BrokerRequestDispatcher(BrokerServices services, MonitoringBundle monitoring) {
        this.services = services;
        if (monitoring == null) {
            this.metrics = null;
            this.adminService = null;
            this.adminAuthority = com.javastream.admin.AdminAuthority.disabled();
            this.securityPolicy = new SecurityPolicy(SecuritySettings.disabled());
            this.messageSizeValidator = new MessageSizeValidator(SecuritySettings.DEFAULT_MAX_MESSAGE_BYTES);
        } else {
            this.metrics = monitoring.metrics();
            this.adminService = monitoring.adminService();
            this.adminAuthority = monitoring.adminAuthority();
            this.securityPolicy = new SecurityPolicy(SecuritySettings.disabled());
            this.messageSizeValidator = new MessageSizeValidator(SecuritySettings.DEFAULT_MAX_MESSAGE_BYTES);
        }
    }

    public BrokerRequestDispatcher(
            BrokerServices services,
            BrokerMetrics metrics,
            com.javastream.admin.AdminAuthority adminAuthority,
            BrokerAdminService adminService) {
        this(services, metrics, adminAuthority, adminService, SecuritySettings.disabled());
    }

    public BrokerRequestDispatcher(
            BrokerServices services,
            BrokerMetrics metrics,
            com.javastream.admin.AdminAuthority adminAuthority,
            BrokerAdminService adminService,
            SecuritySettings securitySettings) {
        this.services = services;
        this.metrics = metrics;
        this.adminAuthority = adminAuthority;
        this.adminService = adminService;
        SecuritySettings settings = securitySettings == null ? SecuritySettings.disabled() : securitySettings;
        this.securityPolicy = new SecurityPolicy(settings);
        this.messageSizeValidator = new MessageSizeValidator(settings.maxMessageBytes());
    }

    public Response dispatch(Request request) {
        return dispatch(request, "unknown", new ConnectionSecurityContext());
    }

    public Response dispatch(Request request, String clientId) {
        return dispatch(request, clientId, new ConnectionSecurityContext());
    }

    public Response dispatch(Request request, String clientId, ConnectionSecurityContext securityContext) {
        Response response;
        String error = null;
        try {
            response = dispatchInternal(request, securityContext);
        } catch (RuntimeException e) {
            error = e.getMessage();
            response =
                    new Response(
                            ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.INTERNAL_ERROR, new byte[0]);
        }
        if (metrics != null) {
            if (response.status() != StatusCode.OK) {
                metrics.recordFailedRequest();
            }
        }
        BrokerRequestLogging.logRequest(
                log,
                services.brokerId(),
                clientId == null ? "unknown" : clientId,
                request.correlationId(),
                request.operation(),
                response,
                error,
                securityContext == null ? null : securityContext.principalOrAnonymous());
        return response;
    }

    private Response dispatchInternal(Request request, ConnectionSecurityContext securityContext) {
        ConnectionSecurityContext ctx =
                securityContext == null ? new ConnectionSecurityContext() : securityContext;
        if (securityPolicy.isAuthenticationRequired(request.operation()) && !securityPolicy.isAuthenticated(ctx)) {
            return forbidden(request.correlationId());
        }
        try {
            return switch (request.operation()) {
                case PING -> Response.ok(request.correlationId(), new byte[0]);
                case CLIENT_AUTH -> handleClientAuth(request, ctx);
                case CREATE_TOPIC -> handleCreateTopic(request, ctx);
                case PRODUCE -> handleProduce(request, ctx);
                case FETCH -> handleFetch(request, ctx);
                case CONSUMER_ACK -> handleAck(request, ctx);
                case JOIN_GROUP -> handleJoinGroup(request, ctx);
                case HEARTBEAT -> handleHeartbeat(request);
                case LEAVE_GROUP -> handleLeaveGroup(request);
                case COMMIT_OFFSETS -> handleCommitOffsets(request, ctx);
                case FETCH_COMMITTED_OFFSETS -> handleFetchCommittedOffsets(request, ctx);
                case LIST_TOPICS -> handleListTopics(request);
                case GET_METADATA -> handleGetMetadata(request);
                case DELETE_TOPIC -> handleDeleteTopic(request, ctx);
                case DESCRIBE_GROUP -> handleDescribeGroup(request);
                case GET_CONSUMER_LAG -> handleGetConsumerLag(request);
                case LIST_BROKERS -> handleListBrokers(request);
                case GET_BROKER_INFO -> handleGetBrokerInfo(request);
                default ->
                        new Response(
                                ProtocolConstants.VERSION_1,
                                request.correlationId(),
                                StatusCode.UNKNOWN_OPERATION,
                                new byte[0]);
            };
        } catch (ProtocolDecodeException e) {
            log.debug("Malformed request payload: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.MALFORMED, new byte[0]);
        } catch (IdempotentSequenceException e) {
            log.debug("Idempotent produce rejected: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        } catch (com.javastream.exception.JavaStreamException e) {
            log.debug("Bad request: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        } catch (IllegalArgumentException | IllegalStateException e) {
            log.debug("Bad request: {}", e.getMessage());
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
    }

    private Response handleClientAuth(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.ClientAuthRequest auth = RecordPayloadCodec.decodeClientAuthRequest(request.payload());
        var principal =
                securityPolicy
                        .settings()
                        .authenticator()
                        .authenticate(auth.username(), auth.password().toCharArray());
        if (principal.isEmpty()) {
            log.debug("Client authentication failed for user {}", com.javastream.security.PasswordHasher.redact(auth.username()));
            RecordPayloadCodec.ClientAuthResponse body =
                    new RecordPayloadCodec.ClientAuthResponse(false, "invalid credentials");
            return new Response(
                    ProtocolConstants.VERSION_1,
                    request.correlationId(),
                    StatusCode.FORBIDDEN,
                    RecordPayloadCodec.encodeClientAuthResponse(body));
        }
        ctx.setPrincipal(principal.get());
        RecordPayloadCodec.ClientAuthResponse body =
                new RecordPayloadCodec.ClientAuthResponse(true, "authenticated");
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeClientAuthResponse(body));
    }

    private Response handleCreateTopic(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.CreateTopicRequest create = RecordPayloadCodec.decodeCreateTopicRequest(request.payload());
        if (!adminMutationAllowed(ctx, create.adminToken(), create.topic())) {
            return forbidden(request.correlationId());
        }
        String validated = com.javastream.topic.TopicNameValidator.validate(create.topic());
        TopicName topic = new TopicName(validated);
        if (services.partitionCount(topic).isPresent()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        if (create.partitionCount() <= 0) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        services.registerTopic(topic, create.partitionCount());
        services.replicationEngine().ifPresent(engine -> engine.onTopicCreatedLocally(topic, create.partitionCount()));
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleListTopics(Request request) {
        List<RecordPayloadCodec.TopicEntry> entries = new ArrayList<>();
        for (String name : services.listTopics()) {
            TopicName topic = new TopicName(name);
            int count = services.partitionCount(topic).orElse(0);
            entries.add(new RecordPayloadCodec.TopicEntry(name, count));
        }
        RecordPayloadCodec.ListTopicsResponse list = new RecordPayloadCodec.ListTopicsResponse(entries);
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeListTopicsResponse(list));
    }

    private Response handleGetMetadata(Request request) {
        RecordPayloadCodec.GetMetadataRequest meta = RecordPayloadCodec.decodeGetMetadataRequest(request.payload());
        TopicName topic = new TopicName(meta.topic());
        Optional<TopicMetadata> metadata = services.topicMetadata(topic);
        if (metadata.isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        return Response.ok(
                request.correlationId(), RecordPayloadCodec.encodeGetMetadataResponse(metadata.get()));
    }

    private Response handleDeleteTopic(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.DeleteTopicRequest delete = RecordPayloadCodec.decodeDeleteTopicRequest(request.payload());
        if (!adminMutationAllowed(ctx, delete.adminToken(), delete.topic())) {
            return forbidden(request.correlationId());
        }
        TopicName topic = new TopicName(delete.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        services.deleteTopic(topic);
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleDescribeGroup(Request request) {
        if (adminService == null) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.INTERNAL_ERROR, new byte[0]);
        }
        RecordPayloadCodec.DescribeGroupRequest payload =
                RecordPayloadCodec.decodeDescribeGroupRequest(request.payload());
        BrokerAdminService.GroupDescription description = adminService.describeGroup(payload.groupId());
        List<RecordPayloadCodec.GroupMemberAssignment> members = new ArrayList<>();
        for (var entry : description.members().entrySet()) {
            members.add(new RecordPayloadCodec.GroupMemberAssignment(entry.getKey(), entry.getValue()));
        }
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeDescribeGroupResponse(
                        new RecordPayloadCodec.DescribeGroupResponse(
                                description.groupId(),
                                description.topic(),
                                description.generation(),
                                members)));
    }

    private Response handleGetConsumerLag(Request request) {
        if (adminService == null) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.INTERNAL_ERROR, new byte[0]);
        }
        RecordPayloadCodec.GetConsumerLagRequest payload =
                RecordPayloadCodec.decodeGetConsumerLagRequest(request.payload());
        List<BrokerAdminService.ConsumerLagEntry> lagEntries =
                adminService.getConsumerLag(payload.groupId(), payload.topic());
        List<RecordPayloadCodec.ConsumerLagEntry> encoded = new ArrayList<>(lagEntries.size());
        for (BrokerAdminService.ConsumerLagEntry entry : lagEntries) {
            encoded.add(
                    new RecordPayloadCodec.ConsumerLagEntry(
                            entry.partition(), entry.committedOffset(), entry.logEndOffset(), entry.lag()));
        }
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeGetConsumerLagResponse(new RecordPayloadCodec.GetConsumerLagResponse(encoded)));
    }

    private Response handleListBrokers(Request request) {
        if (adminService == null) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.INTERNAL_ERROR, new byte[0]);
        }
        List<RecordPayloadCodec.BrokerDirectoryEntry> brokers = new ArrayList<>();
        for (BrokerAdminService.BrokerInfo broker : adminService.listBrokers()) {
            brokers.add(toDirectoryEntry(broker));
        }
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeListBrokersResponse(new RecordPayloadCodec.ListBrokersResponse(brokers)));
    }

    private Response handleGetBrokerInfo(Request request) {
        if (adminService == null) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.INTERNAL_ERROR, new byte[0]);
        }
        RecordPayloadCodec.GetBrokerInfoRequest payload =
                RecordPayloadCodec.decodeGetBrokerInfoRequest(request.payload());
        BrokerAdminService.BrokerInfo info = adminService.getBrokerInfo(payload.brokerId());
        RecordPayloadCodec.BrokerDirectoryEntry broker = toDirectoryEntry(info);
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeGetBrokerInfoResponse(new RecordPayloadCodec.GetBrokerInfoResponse(broker)));
    }

    private static RecordPayloadCodec.BrokerDirectoryEntry toDirectoryEntry(BrokerAdminService.BrokerInfo broker) {
        return new RecordPayloadCodec.BrokerDirectoryEntry(
                broker.brokerId(),
                broker.host(),
                broker.clientPort(),
                broker.replicationPort(),
                broker.state(),
                broker.uptimeSeconds());
    }

    private Response handleProduce(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.ProduceRequest produce = RecordPayloadCodec.decodeProduceRequest(request.payload());
        if (!messageSizeValidator.isWithinLimit(produce)) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.PAYLOAD_TOO_LARGE, new byte[0]);
        }
        TopicName topic = new TopicName(produce.topic());
        if (!securityPolicy.authorizeProduce(ctx, produce.topic())) {
            return forbidden(request.correlationId());
        }
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        List<RecordPayloadCodec.ProduceRecord> records = produce.allRecords();
        List<RecordPayloadCodec.ProduceResponse> responses = new ArrayList<>(records.size());
        long sequence = produce.sequenceNumber();
        if (metrics != null) {
            metrics.markProducerActive();
        }
        for (RecordPayloadCodec.ProduceRecord record : records) {
            int partition = services.resolvePartition(topic, produce.partitionHint(), record.key());
            PartitionId partitionId = new PartitionId(topic, partition);
            if (services.leadership().isPresent()
                    && !services.leadership().get().isLocalLeader(partitionId, services.brokerId())) {
                return new Response(
                        ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
            }
            ProducerIdempotencyCache idempotency = services.producerIdempotencyCache();
            var duplicate = idempotency.beginIdempotentAppend(produce.producerId(), partitionId, sequence);
            if (duplicate.isPresent()) {
                ProducerIdempotencyCache.DedupResult cached = duplicate.get();
                responses.add(
                        new RecordPayloadCodec.ProduceResponse(cached.messageId(), cached.partition(), cached.offset()));
                sequence++;
                continue;
            }
            BrokerMessageStore.AppendResult append =
                    services.appendRecord(
                            partitionId,
                            record.key(),
                            record.value(),
                            record.headers(),
                            System.currentTimeMillis());
            idempotency.recordSuccessfulAppend(
                    produce.producerId(), partitionId, sequence, append.messageId(), append.offset());
            services
                    .replicationEngine()
                    .ifPresent(
                            engine ->
                                    engine.replicateAfterLeaderAppend(
                                            partitionId,
                                            append.messageId(),
                                            append.offset(),
                                            record.key(),
                                            record.value(),
                                            record.headers(),
                                            System.currentTimeMillis()));
            services
                    .produceAcknowledgementService()
                    .awaitAcknowledgement(
                            produce.ackLevel(), services.messageStore(), partitionId, append.offset());
            responses.add(new RecordPayloadCodec.ProduceResponse(append.messageId(), partition, append.offset()));
            sequence++;
        }
        if (metrics != null) {
            metrics.recordPublished(responses.size());
        }
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeProduceBatchResponse(new RecordPayloadCodec.ProduceBatchResponse(responses)));
    }

    private Response handleFetch(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.FetchRequest fetch = RecordPayloadCodec.decodeFetchRequest(request.payload());
        if (!securityPolicy.authorizeConsume(ctx, fetch.topic())) {
            return forbidden(request.correlationId());
        }
        TopicName topic = new TopicName(fetch.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        int partition = fetch.partition();
        Integer partitions = services.partitionCount(topic).orElse(0);
        if (partition < 0 || partition >= partitions) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.BAD_REQUEST, new byte[0]);
        }
        if (metrics != null) {
            metrics.markConsumerActive();
        }
        PartitionId partitionId = new PartitionId(topic, partition);
        List<StoredRecord> stored =
                services.messageStore().fetch(partitionId, fetch.offset(), Math.max(1, fetch.maxRecords()));
        List<RecordPayloadCodec.FetchedRecord> records = new ArrayList<>(stored.size());
        for (StoredRecord record : stored) {
            records.add(
                    new RecordPayloadCodec.FetchedRecord(
                            record.messageId(),
                            partition,
                            record.offset(),
                            record.timestampEpochMillis(),
                            record.key(),
                            record.value(),
                            record.headers()));
        }
        if (metrics != null) {
            metrics.recordConsumed(records.size());
        }
        RecordPayloadCodec.FetchResponse response = new RecordPayloadCodec.FetchResponse(records);
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeFetchResponse(response));
    }

    private Response handleAck(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.AckRequest ack = RecordPayloadCodec.decodeAckRequest(request.payload());
        if (!securityPolicy.authorizeConsume(ctx, ack.topic())) {
            return forbidden(request.correlationId());
        }
        TopicName topic = new TopicName(ack.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        services.acknowledge(topic, ack.partition(), ack.offset());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleJoinGroup(Request request, ConnectionSecurityContext ctx) {
        if (metrics != null) {
            metrics.markConsumerActive();
        }
        RecordPayloadCodec.JoinGroupRequest join = RecordPayloadCodec.decodeJoinGroupRequest(request.payload());
        if (!securityPolicy.authorizeConsume(ctx, join.topic())) {
            return forbidden(request.correlationId());
        }
        var result =
                services.consumerGroupManager()
                        .joinGroup(join.groupId(), join.memberId(), join.topic());
        List<RecordPayloadCodec.CommittedOffsetEntry> committed = new ArrayList<>();
        for (var entry : result.committedOffsets().entrySet()) {
            committed.add(new RecordPayloadCodec.CommittedOffsetEntry(entry.getKey(), entry.getValue()));
        }
        RecordPayloadCodec.JoinGroupResponse response =
                new RecordPayloadCodec.JoinGroupResponse(
                        result.generation(),
                        result.assignedPartitions(),
                        result.revokedPartitions(),
                        committed);
        return Response.ok(request.correlationId(), RecordPayloadCodec.encodeJoinGroupResponse(response));
    }

    private Response handleHeartbeat(Request request) {
        RecordPayloadCodec.HeartbeatRequest heartbeat = RecordPayloadCodec.decodeHeartbeatRequest(request.payload());
        services.consumerGroupManager().heartbeat(heartbeat.groupId(), heartbeat.memberId());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleLeaveGroup(Request request) {
        RecordPayloadCodec.LeaveGroupRequest leave = RecordPayloadCodec.decodeLeaveGroupRequest(request.payload());
        services.consumerGroupManager().leaveGroup(leave.groupId(), leave.memberId());
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleCommitOffsets(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.CommitOffsetsRequest commit =
                RecordPayloadCodec.decodeCommitOffsetsRequest(request.payload());
        if (!securityPolicy.authorizeConsume(ctx, commit.topic())) {
            return forbidden(request.correlationId());
        }
        TopicName topic = new TopicName(commit.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        Map<Integer, Long> offsets = new java.util.LinkedHashMap<>();
        for (RecordPayloadCodec.CommittedOffsetEntry entry : commit.offsets()) {
            offsets.put(entry.partition(), entry.offset());
        }
        services.consumerGroupManager().commitOffsets(commit.groupId(), commit.topic(), offsets);
        return Response.ok(request.correlationId(), new byte[0]);
    }

    private Response handleFetchCommittedOffsets(Request request, ConnectionSecurityContext ctx) {
        RecordPayloadCodec.FetchCommittedOffsetsRequest fetch =
                RecordPayloadCodec.decodeFetchCommittedOffsetsRequest(request.payload());
        if (!securityPolicy.authorizeConsume(ctx, fetch.topic())) {
            return forbidden(request.correlationId());
        }
        TopicName topic = new TopicName(fetch.topic());
        if (services.partitionCount(topic).isEmpty()) {
            return new Response(
                    ProtocolConstants.VERSION_1, request.correlationId(), StatusCode.NOT_FOUND, new byte[0]);
        }
        var offsets =
                services.consumerGroupManager()
                        .fetchCommittedOffsets(fetch.groupId(), fetch.topic(), fetch.partitions());
        List<RecordPayloadCodec.CommittedOffsetEntry> entries = new ArrayList<>();
        for (var entry : offsets.entrySet()) {
            entries.add(new RecordPayloadCodec.CommittedOffsetEntry(entry.getKey(), entry.getValue()));
        }
        return Response.ok(
                request.correlationId(),
                RecordPayloadCodec.encodeFetchCommittedOffsetsResponse(
                        new RecordPayloadCodec.FetchCommittedOffsetsResponse(entries)));
    }

  private boolean adminMutationAllowed(
            ConnectionSecurityContext ctx, Optional<String> presentedAdminToken, String topic) {
        boolean hasValidAdminToken =
                adminAuthority.isRequired() && adminAuthority.authorize(presentedAdminToken);
        boolean aclEnforced = securityPolicy.settings().accessControl().enabled();
        boolean hasAclAdmin = aclEnforced && securityPolicy.authorizeAdmin(ctx, topic);
        if (adminAuthority.isRequired() || aclEnforced) {
            return hasValidAdminToken || hasAclAdmin;
        }
        return true;
    }

    private static Response forbidden(int correlationId) {
        return new Response(ProtocolConstants.VERSION_1, correlationId, StatusCode.FORBIDDEN, new byte[0]);
    }
}
