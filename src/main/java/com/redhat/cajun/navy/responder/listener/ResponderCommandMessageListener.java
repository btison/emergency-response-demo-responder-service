package com.redhat.cajun.navy.responder.listener;

import java.util.Arrays;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.redhat.cajun.navy.responder.message.Message;
import com.redhat.cajun.navy.responder.message.ResponderUpdatedEvent;
import com.redhat.cajun.navy.responder.message.UpdateResponderCommand;
import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.service.ResponderService;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class ResponderCommandMessageListener {

    private final static Logger log = LoggerFactory.getLogger(ResponderCommandMessageListener.class);

    private static final String UPDATE_RESPONDER_COMMAND = "UpdateResponderCommand";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {UPDATE_RESPONDER_COMMAND};

    @Autowired
    private ResponderService responderService;

    @Autowired
    private KafkaTemplate<String, Message<?>> kafkaTemplate;

    @Value("${sender.destination.responder-updated-event}")
    private String destination;

    @KafkaListener(topics = "${listener.destination.update-responder-command}")
    public void processMessage(@Payload String messageAsJson,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        acceptMessageType(messageAsJson, ack).ifPresent(m -> processUpdateResponderCommand(messageAsJson, topic, partition, ack));
    }

    private void processUpdateResponderCommand(String messageAsJson, String topic, int partition, Acknowledgment ack) {

        Message<UpdateResponderCommand> message;
        try {
            message = new ObjectMapper().readValue(messageAsJson, new TypeReference<Message<UpdateResponderCommand>>() {});
            Responder responder = message.getBody().getResponder();

            log.debug("Processing '" + UPDATE_RESPONDER_COMMAND + "' message for responder '" + responder.getId()
                    + "' from topic:partition " + topic + ":" + partition);

            Triple<Boolean, String, Responder> result = responderService.updateResponder(responder);

            // Only send a responder updated event message if there is a 'incidentId' header in the incoming message
            if (message.getHeaderValue("incidentId") != null) {
                String status = (result.getLeft() ? "success" : "error");
                ResponderUpdatedEvent event = new ResponderUpdatedEvent.Builder(status, result.getRight())
                        .statusMessage(result.getMiddle()).build();
                Message eventMessage = new Message.Builder<>("ResponderUpdatedEvent",
                        "ResponderService", event)
                        .header("incidentId", message.getHeaderValue("incidentId"))
                        .build();
                ListenableFuture<SendResult<String, Message<?>>> future = kafkaTemplate.send(destination, responder.getId(), eventMessage);
                future.addCallback(
                        res -> log.debug("Sent 'ResponderUpdatedEvent' message for responder " + responder.getId()),
                        ex -> log.error("Error sending 'IncidentReportedEvent' message for incident " + responder.getId(), ex));
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing msg " + messageAsJson, e);
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    private Optional<String> acceptMessageType(String messageAsJson, Acknowledgment ack) {
        try {
            String messageType = JsonPath.read(messageAsJson, "$.messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType)) {
                return Optional.of(messageType);
            }
            log.debug("Message with type '" + messageType + "' is ignored");
        } catch (Exception e) {
            log.warn("Unexpected message without 'messageType' field.");
        }
        ack.acknowledge();
        return Optional.empty();
    }

}
