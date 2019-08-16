package com.redhat.cajun.navy.responder.dao;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.NonUniqueResultException;
import javax.persistence.PersistenceContext;

import com.redhat.cajun.navy.responder.entity.ResponderEntity;
import org.springframework.stereotype.Component;

@Component
public class ResponderDao {

    @PersistenceContext
    private EntityManager entityManager;

    public ResponderEntity create(ResponderEntity responder) {
        entityManager.persist(responder);
        return responder;
    }

    void deleteAll() {
        entityManager.createNamedQuery("Responder.deleteAll").executeUpdate();
    }

    public ResponderEntity findById(long id) {
        return entityManager.find(ResponderEntity.class, id, LockModeType.OPTIMISTIC);
    }

    public ResponderEntity findByName(String name) {
        List<ResponderEntity> results = entityManager.createNamedQuery("Responder.findByName", ResponderEntity.class)
                .setParameter("name", name).getResultList();
        if (results.isEmpty()) {
            return null;
        } else if (results.size() == 1) {
            return results.get(0);
        } else {
            throw new NonUniqueResultException("Found several Responders with name '" + name + "'");
        }
    }

    public ResponderEntity merge(ResponderEntity responder) {
        ResponderEntity r = entityManager.merge(responder);
        entityManager.flush();
        return r;
    }

    public List<ResponderEntity> availableResponders() {
        return entityManager.createNamedQuery("Responder.availableResponders", ResponderEntity.class).getResultList();
    }

    public List<ResponderEntity> nonPersonResponders() {
        return entityManager.createNamedQuery("Responder.nonPersons", ResponderEntity.class).getResultList();
    }

    public void reset() {
        List<ResponderEntity> results = entityManager.createNamedQuery("Responder.allResponders", ResponderEntity.class).getResultList();
        results.stream()
            .map(r -> {
                ResponderEntity.Builder rb = new ResponderEntity.Builder(r).available(true).enrolled(false);
                if (r.isPerson()) {
                    rb.currentPositionLatitude(null).currentPositionLongitude(null);
                }
                return rb.build();
            }).forEach(r -> entityManager.merge(r));
        entityManager.flush();
    }

    public void clear() {
        entityManager.createNamedQuery("Responder.deleteNonPersons").executeUpdate();
        List<ResponderEntity> results = entityManager.createNamedQuery("Responder.persons", ResponderEntity.class).getResultList();
        results.stream()
                .map(r -> new ResponderEntity.Builder(r).available(true).enrolled(false)
                        .currentPositionLatitude(null).currentPositionLongitude(null).build())
                .forEach(r -> entityManager.merge(r));
        entityManager.flush();
    }

    public Long enrolledRespondersCount() {
        return (Long) entityManager.createNamedQuery("Responder.countEnrolled").getSingleResult();
    }

    public Long activeRespondersCount() {
        return (Long) entityManager.createNamedQuery("Responder.countActive").getSingleResult();
    }
}
