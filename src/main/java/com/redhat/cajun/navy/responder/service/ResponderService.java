package com.redhat.cajun.navy.responder.service;

import java.util.List;
import java.util.stream.Collectors;

import com.redhat.cajun.navy.responder.dao.ResponderDao;
import com.redhat.cajun.navy.responder.entity.ResponderEntity;
import com.redhat.cajun.navy.responder.message.Message;
import com.redhat.cajun.navy.responder.message.RespondersCreatedEvent;
import com.redhat.cajun.navy.responder.message.RespondersDeletedEvent;
import com.redhat.cajun.navy.responder.model.Responder;
import com.redhat.cajun.navy.responder.model.ResponderStats;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class ResponderService {

    private static Logger log = LoggerFactory.getLogger(ResponderService.class);

    @Autowired
    private KafkaTemplate<String, Message<?>> kafkaTemplate;

    @Autowired
    private ResponderDao responderDao;

    @Value("${sender.destination.responders-created-event}")
    private String respondersCreatedDestination;

    @Value("${sender.destination.responders-deleted-event}")
    private String respondersDeletedDestination;

    @Transactional
    public ResponderStats getResponderStats() {
        ResponderStats stats = new ResponderStats();
        stats.setTotal(responderDao.enrolledRespondersCount().intValue());
        stats.setActive(responderDao.activeRespondersCount().intValue());
        return stats;
    }

    @Transactional
    public Responder getResponder(long id) {
        return toResponder(responderDao.findById(id));
    }

    @Transactional
    public Responder getResponderByName(String name) {
        return toResponder(responderDao.findByName(name));
    }

    @Transactional
    public List<Responder> availableResponders() {

        return responderDao.availableResponders().stream().map(this::toResponder)
                .collect(Collectors.toList());
    }

    @Transactional
    public List<Responder> availableResponders(int limit, int offset) {

        return responderDao.availableResponders(limit, offset).stream().map(this::toResponder)
                .collect(Collectors.toList());
    }

    @Transactional
    public List<Responder> allResponders() {

        return responderDao.allResponders().stream().map(this::toResponder)
                .collect(Collectors.toList());
    }

    @Transactional
    public List<Responder> allResponders(int limit, int offset) {
        return responderDao.allResponders(limit, offset).stream().map(this::toResponder)
                .collect(Collectors.toList());
    }

    @Transactional
    public List<Responder> personResponders() {

        return responderDao.personResponders().stream().map(this::toResponder)
                .collect(Collectors.toList());
    }

    @Transactional
    public Responder createResponder(Responder responder) {

        ResponderEntity entity = fromResponder(responder);
        responderDao.create(entity);

        Message<RespondersCreatedEvent> message = new Message.Builder<>("RespondersCreatedEvent", "ResponderService",
                new RespondersCreatedEvent.Builder(new Long[]{entity.getId()}).build()).build();

        ListenableFuture<SendResult<String, Message<?>>> future = kafkaTemplate.send(respondersCreatedDestination, "RespondersCreated", message);
        future.addCallback(
                result -> log.debug("Sent 'RespondersCreatedEvent' message for responder " + entity.getId()),
                ex -> log.error("Error sending 'RespondersCreatedEvent' message", ex));

        return toResponder(entity);
    }

    @Transactional
    public void createResponders(List<Responder> responders) {
        List<Long> responderIds = responders.stream()
                .map(this::fromResponder)
                .map(re -> responderDao.create(re).getId())
                .collect(Collectors.toList());

        Message<RespondersCreatedEvent> message = new Message.Builder<>("RespondersCreatedEvent", "ResponderService",
                new RespondersCreatedEvent.Builder(responderIds.toArray(new Long[0])).build()).build();

        ListenableFuture<SendResult<String, Message<?>>> future = kafkaTemplate.send(respondersCreatedDestination, "RespondersCreated", message);
        future.addCallback(
                result -> log.debug("Sent 'RespondersCreatedEvent' message for " + responderIds.size() +" responders created"),
                ex -> log.error("Error sending 'RespondersCreatedEvent' message", ex));
    }

    @Transactional
    public Triple<Boolean, String, Responder> updateResponder(Responder toUpdate) {

        ResponderEntity current = responderDao.findById(new Long(toUpdate.getId()));
        if (current == null) {
            log.warn("Responder with id '" + toUpdate.getId() + "' not found in the database");
            return new ImmutableTriple<>(false, "Responder with id + " + toUpdate.getId() + " not found.", null);
        }
        ResponderEntity toUpdateEntity = fromResponder(toUpdate, current);
        if (!stateChanged(current, toUpdateEntity)) {
            log.info("Responder with id '" + toUpdate.getId() + "' : state unchanged. Responder record is not updated.");
            return new ImmutableTriple<>(false, "Responder state not changed", toResponder(current));
        }
        try {
            ResponderEntity merged = responderDao.merge(toUpdateEntity);
            return new ImmutableTriple<>(true, "Responder updated", toResponder(merged));
        } catch (Exception e) {
            log.warn("Exception '" + e.getClass() + "' when updating Responder with id '" + toUpdate.getId() + "'. Responder record is not updated.");
            return new ImmutableTriple<>(false, "Exception '" + e.getClass() + "' when updating Responder", toResponder(current));
        }
    }

    @Transactional
    public void reset() {
        log.info("Reset called");
        responderDao.reset();
    }

    @Transactional
    public void clear() {
        log.info("Clear called");
        List<Long> responderIds = responderDao.nonPersonResponders().stream().map(ResponderEntity::getId).collect(Collectors.toList());
        responderDao.clear();

        Message<RespondersDeletedEvent> message = new Message.Builder<>("RespondersDeletedEvent", "ResponderService",
                new RespondersDeletedEvent.Builder(responderIds.toArray(new Long[0])).build()).build();

        ListenableFuture<SendResult<String, Message<?>>> future = kafkaTemplate.send(respondersDeletedDestination, "RespondersDeleted", message);
        future.addCallback(
                result -> log.debug("Sent 'RespondersDeletedEvent' message for " + responderIds.size() +" responders deleted"),
                ex -> log.error("Error sending 'RespondersDeletedEvent' message", ex));
    }

    private boolean stateChanged(ResponderEntity current, ResponderEntity updated) {

        if (updated.getName() != null && !updated.getName().equals(current.getName())) {
            return true;
        }
        if (updated.getPhoneNumber() != null && !updated.getPhoneNumber().equals(current.getPhoneNumber())) {
            return true;
        }
        if (updated.getCurrentPositionLatitude() != null && !updated.getCurrentPositionLatitude().equals(current.getCurrentPositionLatitude())) {
            return true;
        }
        if (updated.getCurrentPositionLongitude() != null && !updated.getCurrentPositionLongitude().equals(current.getCurrentPositionLongitude())) {
            return true;
        }
        if (updated.getBoatCapacity() != null && !updated.getBoatCapacity().equals(current.getBoatCapacity())) {
            return true;
        }
        if (updated.getMedicalKit() != null && !updated.getMedicalKit().equals(current.getMedicalKit())) {
            return true;
        }
        if (updated.isAvailable() != null && !updated.isAvailable().equals(current.isAvailable())) {
            return true;
        }
        if (updated.isPerson() != null && !updated.isPerson().equals(current.isPerson())) {
            return true;
        }
        if (updated.isEnrolled() != null && !updated.isEnrolled().equals(current.isEnrolled())) {
            return true;
        }
        return false;
    }

    private ResponderEntity fromResponder(Responder responder) {

        if (responder == null) {
            return null;
        }

        return new ResponderEntity.Builder(responder.getId() == null ? 0 : new Long(responder.getId()), 0L)
                .name(responder.getName())
                .phoneNumber(responder.getPhoneNumber())
                .currentPositionLatitude(responder.getLatitude())
                .currentPositionLongitude(responder.getLongitude())
                .boatCapacity(responder.getBoatCapacity())
                .medicalKit(responder.isMedicalKit())
                .available(responder.isAvailable())
                .enrolled(responder.isEnrolled())
                .person(responder.isPerson())
                .build();
    }

    private ResponderEntity fromResponder(Responder responder, ResponderEntity current) {

        if (responder == null) {
            return null;
        }

        return new ResponderEntity.Builder(new Long(responder.getId()), current.getVersion())
                .name(responder.getName() == null ? current.getName() : responder.getName())
                .phoneNumber(responder.getPhoneNumber() == null ? current.getPhoneNumber() : responder.getPhoneNumber())
                .currentPositionLatitude(responder.getLatitude() == null ? current.getCurrentPositionLatitude() : responder.getLatitude())
                .currentPositionLongitude(responder.getLongitude() == null ? current.getCurrentPositionLongitude() : responder.getLongitude())
                .boatCapacity(responder.getBoatCapacity() == null ? current.getBoatCapacity() : responder.getBoatCapacity())
                .medicalKit(responder.isMedicalKit() == null ? current.getMedicalKit() : responder.isMedicalKit())
                .available(responder.isAvailable() == null ? current.isAvailable() : responder.isAvailable())
                .person(responder.isPerson() == null ? current.isPerson() : responder.isPerson())
                .enrolled(responder.isEnrolled() == null ? current.isEnrolled() : responder.isEnrolled())
                .build();
    }

    private Responder toResponder(ResponderEntity responder) {

        if (responder == null) {
            return null;
        }

        return new Responder.Builder(Long.toString(responder.getId()))
                .name(responder.getName())
                .phoneNumber(responder.getPhoneNumber())
                .latitude(responder.getCurrentPositionLatitude())
                .longitude(responder.getCurrentPositionLongitude())
                .boatCapacity(responder.getBoatCapacity())
                .medicalKit(responder.getMedicalKit())
                .available(responder.isAvailable())
                .person(responder.isPerson())
                .enrolled(responder.isEnrolled())
                .build();
    }

}
