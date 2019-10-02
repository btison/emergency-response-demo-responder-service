package com.redhat.cajun.navy.responder.listener;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.math.BigDecimal;
import java.util.HashMap;

import com.redhat.cajun.navy.responder.message.Message;
import com.redhat.cajun.navy.responder.message.ResponderUpdatedEvent;
import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.service.ResponderService;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.concurrent.ListenableFuture;

public class ResponderCommandMessageListenerTest {

    @Mock
    private ResponderService responderService;

    @Mock
    private KafkaTemplate kafkaTemplate;

    @Mock
    private Acknowledgment ack;

    @Captor
    private ArgumentCaptor<Responder> responderCaptor;

    @Captor
    private ArgumentCaptor<Message<ResponderUpdatedEvent>> messageCaptor;

    private ResponderCommandMessageListener messageListener;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        initMocks(this);
        messageListener = new ResponderCommandMessageListener();
        setField(messageListener, null, responderService, ResponderService.class);
        setField(messageListener, null, kafkaTemplate, KafkaTemplate.class);
        setField(messageListener, "destination", "test-topic", String.class);
        setField(messageListener, null, GlobalTracer.get(), Tracer.class);
        ListenableFuture future = mock(ListenableFuture.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(Message.class))).thenReturn(future);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMessageUpdateResponder() {

        String json = "{\"messageType\" : \"UpdateResponderCommand\"," +
                "\"id\" : \"messageId\"," +
                "\"invokingService\" : \"messageSender\"," +
                "\"timestamp\" : 1521148332397," +
                "\"header\" : {" +
                "\"incidentId\" : \"incident\"" +
                "}," +
                "\"body\" : {" +
                "\"responder\" : {" +
                "\"id\" : \"1\"," +
                "\"available\" : false" +
                "} " +
                "} " +
                "}";

        Responder updated = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .longitude(new BigDecimal("30.12345"))
                .latitude(new BigDecimal("-77.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .build();
        when(responderService.updateResponder(any(Responder.class))).thenReturn(new ImmutableTriple<>(true, "ok", updated));

        messageListener.processMessage(json,"topic", 1, new HashMap<String, Object>(), ack);

        verify(responderService).updateResponder(responderCaptor.capture());
        Responder captured = responderCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getId(), equalTo("1"));
        assertThat(captured.isAvailable(), equalTo(false));
        assertThat(captured.getName(), nullValue());
        assertThat(captured.getPhoneNumber(), nullValue());
        assertThat(captured.getLatitude(), nullValue());
        assertThat(captured.getLongitude(), nullValue());
        assertThat(captured.getBoatCapacity(), nullValue());
        assertThat(captured.isMedicalKit(), nullValue());

        verify(kafkaTemplate).send(eq("test-topic"), eq("1"), messageCaptor.capture());
        Message<ResponderUpdatedEvent> eventMessage = messageCaptor.getValue();
        assertThat(eventMessage, notNullValue());
        assertThat(eventMessage.getInvokingService(), equalTo("ResponderService"));
        assertThat(eventMessage.getMessageType(), equalTo("ResponderUpdatedEvent"));
        assertThat(eventMessage.getHeaderValue("incidentId"), equalTo("incident"));
        ResponderUpdatedEvent event = eventMessage.getBody();
        assertThat(event, notNullValue());
        assertThat(event.getResponder(), equalTo(updated));
        assertThat(event.getStatus(), equalTo("success"));
        assertThat(event.getStatusMessage(), equalTo("ok"));
        verify(ack).acknowledge();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessMessageUpdateResponderNoIncidentIdHeader() {

        String json = "{\"messageType\" : \"UpdateResponderCommand\"," +
                "\"id\" : \"messageId\"," +
                "\"invokingService\" : \"messageSender\"," +
                "\"timestamp\" : 1521148332397," +
                "\"body\" : {" +
                "\"responder\" : {" +
                "\"id\" : \"1\"," +
                "\"available\" : false" +
                "} " +
                "} " +
                "}";

        Responder updated = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .longitude(new BigDecimal("30.12345"))
                .latitude(new BigDecimal("-77.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .build();
        when(responderService.updateResponder(any(Responder.class))).thenReturn(new ImmutableTriple<>(true, "ok", updated));

        messageListener.processMessage(json,"topic", 1, new HashMap<String, Object>(), ack);

        verify(responderService).updateResponder(responderCaptor.capture());
        Responder captured = responderCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getId(), equalTo("1"));
        assertThat(captured.isAvailable(), equalTo(false));
        assertThat(captured.getName(), nullValue());
        assertThat(captured.getPhoneNumber(), nullValue());
        assertThat(captured.getLatitude(), nullValue());
        assertThat(captured.getLongitude(), nullValue());
        assertThat(captured.getBoatCapacity(), nullValue());
        assertThat(captured.isMedicalKit(), nullValue());

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any(Message.class));
        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWrongMessageType() {

        String json = "{\"messageType\":\"WrongType\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\":{} " +
                "}";

        messageListener.processMessage(json,"topic", 1, new HashMap<String, Object>(), ack);

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(ack).acknowledge();
    }

    @Test
    public void testProcessMessageWrongMessage() {
        String json = "{\"field1\":\"value1\"," +
                "\"field2\":\"value2\"}";

        messageListener.processMessage(json,"topic", 1, new HashMap<String, Object>(), ack);

        verify(responderService, never()).updateResponder(any(Responder.class));
        verify(ack).acknowledge();
    }

}
