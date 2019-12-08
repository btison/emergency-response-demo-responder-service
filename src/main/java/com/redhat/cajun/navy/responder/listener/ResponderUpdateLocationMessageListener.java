package com.redhat.cajun.navy.responder.listener;

import java.math.BigDecimal;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.ReadContext;
import com.redhat.cajun.navy.responder.message.Message;
import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.service.ResponderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ResponderUpdateLocationMessageListener {

    private final static Logger log = LoggerFactory.getLogger(ResponderUpdateLocationMessageListener.class);

    @Autowired
    private ResponderService responderService;

    @Autowired
    private KafkaTemplate<String, Message<?>> kafkaTemplate;

    @KafkaListener(topics = "${listener.destination.responder-location-update-event}")
    public void processMessage(@Payload String messageAsJson,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, Acknowledgment ack) {

        try {
            ReadContext ctx = JsonPath.parse(messageAsJson);
            String responderId = ctx.read("$.responderId");
            BigDecimal lat = ctx.read("$.lat", BigDecimal.class);
            BigDecimal lon = ctx.read("$.lon", BigDecimal.class);
            String status = ctx.read("$.status");
            if ("MOVING".equalsIgnoreCase(status)) {
                Responder responder = new Responder.Builder(responderId).latitude(lat).longitude(lon).build();
                log.debug("Processing 'ResponderUpdateLocationEvent' message for responder '" + responder.getId()
                        + "' from topic:partition " + topic + ":" + partition);
                responderService.updateResponderLocation(responder);
            }
        } catch (JsonPathException e) {
            log.warn("Unexpected message structure: " + messageAsJson);
        }
        ack.acknowledge();
    }

}
