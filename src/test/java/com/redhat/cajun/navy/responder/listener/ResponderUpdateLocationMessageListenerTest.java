package com.redhat.cajun.navy.responder.listener;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.service.ResponderService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.kafka.support.Acknowledgment;


public class ResponderUpdateLocationMessageListenerTest {

    @Mock
    private Acknowledgment ack;

    @Mock
    private ResponderService responderService;

    @Captor
    private ArgumentCaptor<Responder> responderCaptor;

    private ResponderUpdateLocationMessageListener messageListener;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        initMocks(this);
        messageListener = new ResponderUpdateLocationMessageListener();
        setField(messageListener, null, responderService, ResponderService.class);
    }

    @Test
    public void testResponderLocationUpdateEvent() {
        String json = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        when(responderService.updateResponderLocation(any(Responder.class))).thenReturn(true);

        messageListener.processMessage(json, "topic", 1, ack);

        verify(responderService).updateResponderLocation(responderCaptor.capture());
        Responder captured = responderCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.getId(), equalTo("64"));
        assertThat(captured.getLatitude().toString(), equalTo("34.1701"));
        assertThat(captured.getLongitude().toString(), equalTo("-77.9482"));
        verify(ack).acknowledge();
    }

    @Test
    public void testResponderLocationUpdateEventStatusNotMoving() {
        String json = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"DROPPED\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        when(responderService.updateResponderLocation(any(Responder.class))).thenReturn(true);

        messageListener.processMessage(json, "topic", 1, ack);

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        verify(ack).acknowledge();
    }

    @Test
    public void testResponderLocationUpdateEventWrongMessage() {
        String json = "{\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        when(responderService.updateResponderLocation(any(Responder.class))).thenReturn(true);

        messageListener.processMessage(json, "topic", 1, ack);

        verify(responderService, never()).updateResponderLocation(any(Responder.class));
        verify(ack).acknowledge();
    }

}
