package com.redhat.cajun.navy.responder.service;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.springframework.test.util.ReflectionTestUtils.setField;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redhat.cajun.navy.responder.dao.ResponderDao;
import com.redhat.cajun.navy.responder.entity.ResponderEntity;
import com.redhat.cajun.navy.responder.message.Message;
import com.redhat.cajun.navy.responder.message.RespondersCreatedEvent;
import com.redhat.cajun.navy.responder.message.RespondersDeletedEvent;
import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.model.ResponderStats;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.ListenableFuture;

public class ResponderServiceTest {

    @Mock
    private ResponderDao responderDao;

    @Mock
    private KafkaTemplate<String, Message<?>> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ResponderEntity> entityCaptor;

    @Captor
    private ArgumentCaptor<Message<RespondersCreatedEvent>> respondersCreatedEventMessageCaptor;

    @Captor
    private ArgumentCaptor<Message<RespondersDeletedEvent>> respondersDeletedEventMessageCaptor;

    private ResponderService service;

    @Before
    public void init() {
        initMocks(this);
        service = new ResponderService();
        setField(service, null, responderDao, ResponderDao.class);
        setField(service, null, kafkaTemplate, KafkaTemplate.class);
        setField(service, "respondersCreatedDestination", "test-topic", String.class);
        setField(service, "respondersDeletedDestination", "test-topic", String.class);
        ListenableFuture future = mock(ListenableFuture.class);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(future);
    }

    @Test
    public void testFindResponderById() {
        ResponderEntity found = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderDao.findById(any(Long.class))).thenReturn(found);

        Responder responder = service.getResponder(1);

        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("1"));
        assertThat(responder.getName(), equalTo("John Doe"));

        verify(responderDao).findById(eq(1L));
    }

    @Test
    public void testFindResponderByIdWhenNotFound() {

        when(responderDao.findById(any(Long.class))).thenReturn(null);

        Responder responder = service.getResponder(1);

        assertThat(responder, nullValue());

        verify(responderDao).findById(eq(1L));
    }

    @Test
    public void testAvailableResponders() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = new ArrayList<>();
        responderEntities.add(responder1);
        responderEntities.add(responder2);

        when(responderDao.availableResponders()).thenReturn(responderEntities);

        List<Responder> responders = service.availableResponders();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
    }

    @Test
    public void testAllResponders() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = new ArrayList<>();
        responderEntities.add(responder1);
        responderEntities.add(responder2);

        when(responderDao.allResponders()).thenReturn(responderEntities);

        List<Responder> responders = service.allResponders();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
    }

    @Test
    public void testPersonResponders() {

        ResponderEntity responder1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        List<ResponderEntity> responderEntities = new ArrayList<>();
        responderEntities.add(responder1);
        responderEntities.add(responder2);

        when(responderDao.personResponders()).thenReturn(responderEntities);

        List<Responder> responders = service.personResponders();
        assertThat(responders, notNullValue());
        assertThat(responders.size(), equalTo(2));
        Responder responder = responders.get(0);
        assertThat(responder.getId(), anyOf(equalTo("1"), equalTo("2")));
        ResponderEntity matched;
        if (responder.getId().equals("1")) {
            matched = responder1;
        } else {
            matched = responder2;
        }
        assertThat(responder.getName(), equalTo(matched.getName()));
        assertThat(responder.getPhoneNumber(), equalTo(matched.getPhoneNumber()));
        assertThat(responder.getLatitude(), equalTo(matched.getCurrentPositionLatitude()));
        assertThat(responder.getLongitude(), equalTo(matched.getCurrentPositionLongitude()));
        assertThat(responder.getBoatCapacity(), equalTo(matched.getBoatCapacity()));
        assertThat(responder.isMedicalKit(),equalTo(matched.getMedicalKit()));
        assertThat(responder.isAvailable(), equalTo(matched.isAvailable()));
        assertThat(responder.isPerson(), equalTo(matched.isPerson()));
        assertThat(responder.isEnrolled(), equalTo(matched.isEnrolled()));
    }

    @Test
    public void testUpdateResponderAvailable() {

        Responder toUpdate = new Responder.Builder("1").available(false).build();

        ResponderEntity currentEntity = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();
        setField(currentEntity, "id", 1, null);

        ResponderEntity updatedEntity = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();

        when(responderDao.findById(1L)).thenReturn(currentEntity);
        when(responderDao.merge(any(ResponderEntity.class))).thenReturn(updatedEntity);

        Triple<Boolean, String, Responder> result = service.updateResponder(toUpdate);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(true));
        assertThat(result.getRight(), notNullValue());
        Responder updated = result.getRight();
        assertThat(updated.getId(), equalTo("1"));
        assertThat(updated.getName(), equalTo("John Doe"));
        assertThat(updated.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updated.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(updated.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(updated.getBoatCapacity(), equalTo(3));
        assertThat(updated.isMedicalKit(), equalTo(true));
        assertThat(updated.isAvailable(), equalTo(false));
        assertThat(updated.isPerson(), equalTo(true));
        assertThat(updated.isEnrolled(), equalTo(true));
        verify(responderDao).findById(1L);
        verify(responderDao).merge(entityCaptor.capture());
        ResponderEntity entity = entityCaptor.getValue();
        assertThat(entity, notNullValue());
        assertThat(entity.getId(), equalTo(1L));
        assertThat(entity.getName(), equalTo("John Doe"));
        assertThat(entity.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(entity.getCurrentPositionLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(entity.getCurrentPositionLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(entity.getBoatCapacity(), equalTo(3));
        assertThat(entity.getMedicalKit(), equalTo(true));
        assertThat(entity.isAvailable(), equalTo(false));
        assertThat(entity.isPerson(), equalTo(true));
        assertThat(entity.isEnrolled(), equalTo(true));
    }

    @Test
    public void testUpdateResponderEnrolled() {

        Responder toUpdate = new Responder.Builder("1").enrolled(true).build();

        ResponderEntity currentEntity = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(false)
                .build();
        setField(currentEntity, "id", 1, null);

        ResponderEntity updatedEntity = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderDao.findById(1L)).thenReturn(currentEntity);
        when(responderDao.merge(any(ResponderEntity.class))).thenReturn(updatedEntity);

        Triple<Boolean, String, Responder> result = service.updateResponder(toUpdate);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(true));
        assertThat(result.getRight(), notNullValue());
        Responder updated = result.getRight();
        assertThat(updated.getId(), equalTo("1"));
        assertThat(updated.getName(), equalTo("John Doe"));
        assertThat(updated.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updated.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(updated.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(updated.getBoatCapacity(), equalTo(3));
        assertThat(updated.isMedicalKit(), equalTo(true));
        assertThat(updated.isAvailable(), equalTo(true));
        assertThat(updated.isPerson(), equalTo(true));
        assertThat(updated.isEnrolled(), equalTo(true));
        verify(responderDao).findById(1L);
        verify(responderDao).merge(entityCaptor.capture());
        ResponderEntity entity = entityCaptor.getValue();
        assertThat(entity, notNullValue());
        assertThat(entity.getId(), equalTo(1L));
        assertThat(entity.getName(), equalTo("John Doe"));
        assertThat(entity.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(entity.getCurrentPositionLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(entity.getCurrentPositionLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(entity.getBoatCapacity(), equalTo(3));
        assertThat(entity.getMedicalKit(), equalTo(true));
        assertThat(entity.isAvailable(), equalTo(true));
        assertThat(entity.isPerson(), equalTo(true));
        assertThat(entity.isEnrolled(), equalTo(true));
    }

    @Test
    public void testUpdateResponderWhenAvailableStateHasNotChanged() {

        Responder toUpdate = new Responder.Builder("1").available(false).build();

        ResponderEntity currentEntity = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();

        when(responderDao.findById(1L)).thenReturn(currentEntity);

        Triple<Boolean, String, Responder> result = service.updateResponder(toUpdate);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(false));
        Responder updated = result.getRight();
        assertThat(updated.getId(), equalTo("1"));
        assertThat(updated.getName(), equalTo("John Doe"));
        assertThat(updated.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updated.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(updated.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(updated.getBoatCapacity(), equalTo(3));
        assertThat(updated.isMedicalKit(), equalTo(true));
        assertThat(updated.isAvailable(), equalTo(false));
        assertThat(updated.isPerson(), equalTo(true));
        verify(responderDao).findById(1L);
        verify(responderDao, never()).merge(any());
    }

    @Test
    public void testUpdateResponderWhenEnrolledStateHasNotChanged() {

        Responder toUpdate = new Responder.Builder("1").enrolled(true).build();

        ResponderEntity currentEntity = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .person(true)
                .enrolled(true)
                .build();

        when(responderDao.findById(1L)).thenReturn(currentEntity);

        Triple<Boolean, String, Responder> result = service.updateResponder(toUpdate);
        assertThat(result, notNullValue());
        assertThat(result.getLeft(), equalTo(false));
        Responder updated = result.getRight();
        assertThat(updated.getId(), equalTo("1"));
        assertThat(updated.getName(), equalTo("John Doe"));
        assertThat(updated.getPhoneNumber(), equalTo("111-222-333"));
        assertThat(updated.getLatitude(), equalTo(new BigDecimal("30.12345")));
        assertThat(updated.getLongitude(), equalTo(new BigDecimal("-70.98765")));
        assertThat(updated.getBoatCapacity(), equalTo(3));
        assertThat(updated.isMedicalKit(), equalTo(true));
        assertThat(updated.isAvailable(), equalTo(false));
        assertThat(updated.isPerson(), equalTo(true));
        assertThat(updated.isEnrolled(), equalTo(true));
        verify(responderDao).findById(1L);
        verify(responderDao, never()).merge(any());
    }

    @Test
    public void testCreateResponder() {

        Responder toCreate = new Responder.Builder(null)
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

        when(responderDao.create(any(ResponderEntity.class))).thenAnswer((Answer<ResponderEntity>) invocation -> {
            ResponderEntity entity = invocation.getArgument(0);
            setField(entity, "id", 100, null);
            return entity;
        });

        Responder created = service.createResponder(toCreate);

        verify(responderDao).create(entityCaptor.capture());
        ResponderEntity entity = entityCaptor.getValue();
        assertThat(entity.getName(), equalTo("John Doe"));

        assertThat(created, notNullValue());
        assertThat(created.getId(), equalTo("100"));
        assertThat(created.getName(), equalTo("John Doe"));
        assertThat(created.isAvailable(), equalTo(true));
        assertThat(created.isPerson(), equalTo(true));
        assertThat(created.isEnrolled(), equalTo(true));

        verify(kafkaTemplate).send(eq("test-topic"), eq("RespondersCreated"), respondersCreatedEventMessageCaptor.capture());
        Message<RespondersCreatedEvent> message = respondersCreatedEventMessageCaptor.getValue();
        assertThat(message.getId(), notNullValue());
        assertThat(message.getInvokingService(), equalTo("ResponderService"));
        assertThat(message.getBody(), notNullValue());
        RespondersCreatedEvent event = message.getBody();
        assertThat(event.getCreated(), equalTo(1));
        assertThat(event.getResponders(), notNullValue());
        assertThat(event.getResponders().length, equalTo(1));
        assertThat(event.getResponders()[0], equalTo(100L));
    }

    @Test
    public void testFindByName() {

        ResponderEntity found = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(true)
                .enrolled(true)
                .build();

        when(responderDao.findByName(any(String.class))).thenReturn(found);

        Responder responder = service.getResponderByName("John Doe");

        assertThat(responder, notNullValue());
        assertThat(responder.getId(), equalTo("1"));
        assertThat(responder.getName(), equalTo("John Doe"));
        assertThat(responder.isAvailable(), equalTo(true));
        assertThat(responder.isPerson(), equalTo(true));
        assertThat(responder.isEnrolled(), equalTo(true));

        verify(responderDao).findByName(eq("John Doe"));
    }

    @Test
    public void testFindByNameWhenNotFound() {

        when(responderDao.findByName(any(String.class))).thenReturn(null);

        Responder responder = service.getResponderByName("John Doe");

        assertThat(responder, nullValue());

        verify(responderDao).findByName(eq("John Doe"));
    }

    @Test
    public void testResponderStats() {
        when(responderDao.enrolledRespondersCount()).thenReturn(new Long(10));
        when(responderDao.activeRespondersCount()).thenReturn(new Long(5));

        ResponderStats stats = service.getResponderStats();

        assertThat(stats, notNullValue());
        assertThat(stats.getTotal(), equalTo(10));
        assertThat(stats.getActive(), equalTo(5));
        verify(responderDao).enrolledRespondersCount();
        verify(responderDao).activeRespondersCount();
    }

    @Test
    public void testCreateResponders() {

        Responder toCreate1 = new Responder.Builder(null)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .latitude(new BigDecimal("30.12345"))
                .longitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        Responder toCreate2 = new Responder.Builder(null)
                .name("John Foo")
                .phoneNumber("222-333-4443")
                .latitude(new BigDecimal("31.12345"))
                .longitude(new BigDecimal("-71.98765"))
                .boatCapacity(6)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        when(responderDao.create(any(ResponderEntity.class))).thenAnswer((Answer<ResponderEntity>) invocation -> {
            ResponderEntity entity = invocation.getArgument(0);
            setField(entity, "id", 1, null);
            return entity;
        }).thenAnswer((Answer<ResponderEntity>) invocation -> {
            ResponderEntity entity = invocation.getArgument(0);
            setField(entity, "id", 2, null);
            return entity;
        });

        service.createResponders(Arrays.asList(toCreate1, toCreate2));

        verify(responderDao, times(2)).create(entityCaptor.capture());

        verify(kafkaTemplate).send(eq("test-topic"), eq("RespondersCreated"), respondersCreatedEventMessageCaptor.capture());
        Message<RespondersCreatedEvent> message = respondersCreatedEventMessageCaptor.getValue();
        assertThat(message.getId(), notNullValue());
        assertThat(message.getInvokingService(), equalTo("ResponderService"));
        assertThat(message.getBody(), notNullValue());
        RespondersCreatedEvent event = message.getBody();
        assertThat(event.getCreated(), equalTo(2));
        assertThat(event.getResponders(), notNullValue());
        assertThat(event.getResponders().length, equalTo(2));
        assertThat(event.getResponders()[0], equalTo(1L));
        assertThat(event.getResponders()[1], equalTo(2L));
    }

    @Test
    public void testReset() {
        service.reset();
        verify(responderDao).reset();
    }

    @Test
    public void testClear() {

        ResponderEntity re1 = new ResponderEntity.Builder(1L, 0L)
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        ResponderEntity re2 = new ResponderEntity.Builder(2L, 0L)
                .name("John Foo")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .person(false)
                .enrolled(true)
                .build();

        List<ResponderEntity> reList = new ArrayList<>();
        reList.add(re1);
        reList.add(re2);

        when(responderDao.nonPersonResponders()).thenReturn(reList);

        service.clear();
        verify(responderDao).nonPersonResponders();
        verify(responderDao).clear();

        verify(kafkaTemplate).send(eq("test-topic"), eq("RespondersDeleted"), respondersDeletedEventMessageCaptor.capture());
        Message<RespondersDeletedEvent> message = respondersDeletedEventMessageCaptor.getValue();
        assertThat(message.getId(), notNullValue());
        assertThat(message.getMessageType(), equalTo("RespondersDeletedEvent"));
        assertThat(message.getInvokingService(), equalTo("ResponderService"));
        assertThat(message.getBody(), notNullValue());
        RespondersDeletedEvent event = message.getBody();
        assertThat(event.getDeleted(), equalTo(2));
        assertThat(event.getResponders(), notNullValue());
        assertThat(event.getResponders().length, equalTo(2));
        assertThat(event.getResponders()[0], equalTo(1L));
        assertThat(event.getResponders()[1], equalTo(2L));

    }

}
