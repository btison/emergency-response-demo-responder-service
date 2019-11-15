package com.redhat.cajun.navy.responder;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.redhat.cajun.navy.responder.listener.ResponderCommandMessageListener;
import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.service.ResponderService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.util.UriComponentsBuilder;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {
       RespondersController.class, ResponderService.class
})
public class ResponderControllerMvcTest {

    @MockBean
    private ResponderService responderService;

    @Autowired
    private RespondersController controller;

    private MockMvc mockMvc;

    @MockBean
    private ResponderCommandMessageListener responderCommandMessageListener;

    @Captor
    private ArgumentCaptor<Responder> responderCaptor;

    @Captor
    private ArgumentCaptor<List<Responder>> responderListCaptor;

    @Before
    public void initTest() {
        mockMvc = MockMvcBuilders
                .standaloneSetup(controller)
                .build();
    }

    @Test
    public void testAvailableResponders() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders/available").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).availableResponders();
    }

    @Test
    public void testAvailableRespondersOrderedByPersonNoOffset() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders/available?limit=10").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).availableResponders(10, 0);
    }

    @Test
    public void testAvailableRespondersOrderedByPersonWithOffset() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders/available?limit=10&offset=5").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).availableResponders(10, 5);
    }

    @Test
    public void testAllResponders() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).allResponders();
    }

    @Test
    public void testAllRespondersWithLimitNoOffset() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders?limit=10").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).allResponders(10, 0);
    }

    @Test
    public void testAllRespondersWithLimitAndOffset() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders?limit=10&offset=5").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).allResponders(10, 5);
    }

    @Test
    public void testPersonResponders() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders/person").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).personResponders();
    }

    @Test
    public void testPersonRespondersWithLimitNoOffset() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders/person?limit=10").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).personResponders(10, 0);
    }

    @Test
    public void testPersonRespondersWithLimitAndOffset() throws Exception {

        initService();

        final ResultActions result = mockMvc.perform(
                get("/responders/person?limit=10&offset=5").accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk());
        result.andExpect(jsonPath("$.length()").value(2));

        verify(responderService).personResponders(10, 5);
    }

    @Test
    public void testCreateResponder() throws Exception {

        String json = "{" +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.98765," +
                "\"boatCapacity\" : 3," +
                "\"medicalKit\" : true," +
                "\"available\": true" +
                "}";

        final ResultActions result = mockMvc.perform(post("/responder")
                .contentType(MimeTypeUtils.APPLICATION_JSON_VALUE).content(json));

        result.andExpect(status().isCreated());
        verify(responderService).createResponder(responderCaptor.capture());
        Responder responder = responderCaptor.getValue();
        assertThat(responder, notNullValue());
        assertThat(responder.getName(), equalTo("John Doe"));
        assertThat(responder.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(responder.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(responder.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(responder.getBoatCapacity(), equalTo(3));
        assertThat(responder.isMedicalKit(), equalTo(true));
        assertThat(responder.isAvailable(), equalTo(true));
    }

    @Test
    public void testCreateResponders() throws Exception {

        String json = "[" + "{" +
                "\"name\" : \"John Doe\"," +
                "\"phoneNumber\" : \"111-222-333\"," +
                "\"latitude\" : 30.12345," +
                "\"longitude\" : -70.98765," +
                "\"boatCapacity\" : 3," +
                "\"medicalKit\" : true," +
                "\"available\": true," +
                "\"person\": true," +
                "\"enrolled\": true" +
                "}" +"," +
                "{" +
                "\"name\" : \"John Foo\"," +
                "\"phoneNumber\" : \"222-333-444\"," +
                "\"latitude\" : 31.12345," +
                "\"longitude\" : -71.98765," +
                "\"boatCapacity\" : 4," +
                "\"medicalKit\" : true," +
                "\"available\": true," +
                "\"person\": false," +
                "\"enrolled\": true" +
                "}" +
                "]";

        final ResultActions result = mockMvc.perform(post("/responders")
                .contentType(MimeTypeUtils.APPLICATION_JSON_VALUE).content(json));

        result.andExpect(status().isCreated());
        verify(responderService).createResponders(responderListCaptor.capture());
        List<Responder> responders = responderListCaptor.getValue();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        assertThat(responders.get(0).getName(), anyOf(equalTo("John Doe"), equalTo("John Foo")));
    }

    @Test
    public void testFindByName() throws Exception {

        Responder responder = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderService.getResponderByName(any(String.class))).thenReturn(responder);

        URI url = UriComponentsBuilder.fromUriString("/responder/byname").pathSegment("John Doe").build().encode().toUri();
        final ResultActions result = mockMvc.perform(get(url).accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("John Doe"))
                .andExpect(jsonPath("$.available").value(true))
                .andExpect(jsonPath("$.person").value(true))
                .andExpect(jsonPath("$.enrolled").value(true));

        verify(responderService).getResponderByName(eq("John Doe"));
    }

    @Test
    public void testFindByNameNotFound() throws Exception {

        when(responderService.getResponderByName(any(String.class))).thenReturn(null);

        URI url = UriComponentsBuilder.fromUriString("/responder/byname").pathSegment("John Doe").build().encode().toUri();
        final ResultActions result = mockMvc.perform(get(url).accept(MimeTypeUtils.APPLICATION_JSON_VALUE));

        result.andExpect(status().isNotFound());

        verify(responderService).getResponderByName(eq("John Doe"));
    }

    private void initService() {
        Responder responder1 = new Responder.Builder("1")
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .build();

        Responder responder2 = new Responder.Builder("2")
                .name("John Foo")
                .phoneNumber("999-888-777")
                .latitude(new BigDecimal("35.12345"))
                .longitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .build();

        List<Responder> responders = new ArrayList<>();
        responders.add(responder1);
        responders.add(responder2);

        when(responderService.availableResponders()).thenReturn(responders);
        when(responderService.availableResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);
        when(responderService.allResponders()).thenReturn(responders);
        when(responderService.allResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);
        when(responderService.personResponders()).thenReturn(responders);
        when(responderService.personResponders(any(Integer.class), any(Integer.class))).thenReturn(responders);
    }

}
