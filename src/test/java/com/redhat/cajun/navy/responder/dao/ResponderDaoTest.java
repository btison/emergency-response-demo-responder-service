package com.redhat.cajun.navy.responder.dao;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.persistence.NonUniqueResultException;
import javax.persistence.OptimisticLockException;

import com.redhat.cajun.navy.responder.entity.ResponderEntity;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.transaction.TestTransaction;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@DataJpaTest(includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = ResponderDao.class))
public class ResponderDaoTest {

    @Autowired
    private ResponderDao responderDao;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    @Transactional
    public void testPersistResponderEntity() {

        assertThat(responderDao, notNullValue());

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        responderDao.create(responder);
        assertThat(responder.getId(), not(equalTo(0)));

    }

    @Test
    @Transactional
    public void testAvailableResponders() {

        responderDao.deleteAll();

        ResponderEntity responder1 = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        responderDao.create(responder1);
        responderDao.create(responder2);

        List<ResponderEntity> responders = responderDao.availableResponders();
        assertThat(responders.size(), equalTo(1));
        ResponderEntity responder = responders.get(0);
        assertThat(responder.getName(), equalTo("John Foo"));
    }

    @Test
    @Transactional
    public void testAvailableRespondersNotEnrolled() {

        responderDao.deleteAll();

        ResponderEntity responder1 = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(false)
                .build();

        responderDao.create(responder1);
        responderDao.create(responder2);

        List<ResponderEntity> responders = responderDao.availableResponders();
        assertThat(responders.size(), equalTo(0));
    }

    @Test
    @Transactional
    public void testAvailableRespondersNotAvailable() {

        responderDao.deleteAll();

        ResponderEntity responder1 = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .build();

        responderDao.create(responder1);
        responderDao.create(responder2);

        List<ResponderEntity> responders = responderDao.availableResponders();
        assertThat(responders.size(), equalTo(0));
    }

    @Test
    @Transactional
    public void testAllResponders() {

        responderDao.deleteAll();

        ResponderEntity responder1 = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .person(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        responderDao.create(responder1);
        responderDao.create(responder2);

        List<ResponderEntity> responders = responderDao.allResponders();
        assertThat(responders.size(), equalTo(2));
    }

    @Test
    @Transactional
    public void testPersonResponders() {

        responderDao.deleteAll();

        ResponderEntity responder1 = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .person(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        responderDao.create(responder1);
        responderDao.create(responder2);

        List<ResponderEntity> responders = responderDao.personResponders();
        assertThat(responders.size(), equalTo(1));
        ResponderEntity responder = responders.get(0);
        assertThat(responder.getName(), equalTo("John Doe"));
    }

    @Test
    @Transactional
    public void testNonPersonResponders() {

        responderDao.deleteAll();

        ResponderEntity responder1 = new ResponderEntity.Builder()
                .name("John Doe")
                .phoneNumber("111-222-333")
                .currentPositionLatitude(new BigDecimal("30.12345"))
                .currentPositionLongitude(new BigDecimal("-70.98765"))
                .boatCapacity(3)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .person(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        responderDao.create(responder1);
        responderDao.create(responder2);

        List<ResponderEntity> responders = responderDao.nonPersonResponders();
        assertThat(responders.size(), equalTo(1));
        ResponderEntity responder = responders.get(0);
        assertThat(responder.getName(), equalTo("John Foo"));
    }

    @Test
    public void testFindById() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findById(responder.getId());
            assertThat(r, notNullValue());
            assertThat(r.getName(), equalTo(responder.getName()));
            return null;
        });
    }

    @Test
    public void testFindByIdWhenNotFound() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findById(responder.getId()+1);
            assertThat(r, nullValue());
            return null;
        });
    }

    @Test
    public void testUpdateEntity() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findById(responder.getId());
            assertThat(r.isAvailable(), equalTo(true));
            ResponderEntity r2 = new ResponderEntity.Builder(r).available(false).build();
            responderDao.merge(r2);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findById(responder.getId());
            assertThat(r, notNullValue());
            assertThat(r.getName(), equalTo(responder.getName()));
            assertThat(r.isAvailable(), equalTo(false));
            return null;
        });
    }

    @Test
    public void testOptimisticLocking() throws Exception {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch thread2Latch = new CountDownLatch(1);

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            return null;
        });

        Runnable thread1 = () -> {
            TransactionTemplate template1 = new TransactionTemplate(transactionManager);
            template1.execute((TransactionStatus s) -> {
                try {
                    ResponderEntity r = responderDao.findById(responder.getId());
                    // wait for thread2 to finish
                    thread2Latch.await(10, TimeUnit.SECONDS);

                    ResponderEntity r2 = new ResponderEntity.Builder(r).currentPositionLatitude(new BigDecimal("20.12345")).available(false).build();
                    responderDao.merge(r2);
                    //OptimisticLockException is expected
                    Assert.fail();
                } catch (Exception e) {
                    assertThat(e, is(instanceOf(OptimisticLockException.class)));
                } finally {
                    latch.countDown();
                }
                return null;
            });
        };

        Runnable thread2 = () -> {
            TransactionTemplate template2 = new TransactionTemplate(transactionManager);
            template2.execute((TransactionStatus s) -> {
                try {
                    ResponderEntity r = responderDao.findById(responder.getId());
                    ResponderEntity r2 = new ResponderEntity.Builder(r).currentPositionLatitude(new BigDecimal("30.12345")).available(false).build();
                    responderDao.merge(r2);
                } finally {
                    thread2Latch.countDown();
                    latch.countDown();
                }
                return null;
            });
        };

        new Thread(thread1).start();
        new Thread(thread2).start();

        latch.await(10, TimeUnit.SECONDS);

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findById(responder.getId());
            assertThat(r.isAvailable(), equalTo(false));
            assertThat(r.getCurrentPositionLatitude(), equalTo(new BigDecimal("30.12345")));
            return null;
        });
    }

    @Test
    public void testFindByName() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findByName("John Foo");
            assertThat(r, notNullValue());
            assertThat(r.getName(), equalTo(responder.getName()));
            return null;
        });
    }

    @Test
    public void testFindByNameWhenNotFound() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findByName("John Doe");
            assertThat(r, nullValue());
            return null;
        });
    }

    @Test
    public void testFindByNameWhenNotMultipleResults() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            try {
                responderDao.findByName("John Foo");
                Assert.fail();
            } catch (NonUniqueResultException e) {
                // expected exception
            }
            return null;
        });
    }

    @Test
    public void testReset() {
        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .person(false)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo II")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .person(false)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder3 = new ResponderEntity.Builder()
                .name("John Foo III")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .person(true)
                .available(false)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            responderDao.create(responder3);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.reset();
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findByName("John Foo");
            assertThat(r, notNullValue());
            assertThat(r.isEnrolled(), equalTo(false));
            assertThat(r.isAvailable(), equalTo(true));
            assertThat(r.isPerson(), equalTo(false));
            assertThat(r.getCurrentPositionLatitude(), notNullValue());
            assertThat(r.getCurrentPositionLongitude(), notNullValue());

            r = responderDao.findByName("John Foo II");
            assertThat(r, notNullValue());
            assertThat(r.isEnrolled(), equalTo(false));
            assertThat(r.isAvailable(), equalTo(true));
            assertThat(r.isPerson(), equalTo(false));
            assertThat(r.getCurrentPositionLatitude(), notNullValue());
            assertThat(r.getCurrentPositionLongitude(), notNullValue());

            r = responderDao.findByName("John Foo III");
            assertThat(r, notNullValue());
            assertThat(r.isEnrolled(), equalTo(false));
            assertThat(r.isAvailable(), equalTo(true));
            assertThat(r.isPerson(), equalTo(true));
            assertThat(r.getCurrentPositionLatitude(), nullValue());
            assertThat(r.getCurrentPositionLongitude(), nullValue());
            return null;
        });
    }

    @Test
    public void testClear() {
        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .person(false)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo II")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .person(false)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder3 = new ResponderEntity.Builder()
                .name("John Foo III")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .person(true)
                .available(false)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            responderDao.create(responder3);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.clear();
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            ResponderEntity r = responderDao.findByName("John Foo");
            assertThat(r, nullValue());

            r = responderDao.findByName("John Foo II");
            assertThat(r, nullValue());

            r = responderDao.findByName("John Foo III");
            assertThat(r, notNullValue());
            assertThat(r.isEnrolled(), equalTo(false));
            assertThat(r.isAvailable(), equalTo(true));
            assertThat(r.isPerson(), equalTo(true));
            assertThat(r.getCurrentPositionLatitude(), nullValue());
            assertThat(r.getCurrentPositionLongitude(), nullValue());
            return null;
        });
    }

    @Test
    public void testEnrolledRespondersCount() {

        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo I")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo II")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder3 = new ResponderEntity.Builder()
                .name("John Foo III")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(false)
                .build();

        ResponderEntity responder4 = new ResponderEntity.Builder()
                .name("John Foo IV")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .build();

        ResponderEntity responder5 = new ResponderEntity.Builder()
                .name("John Foo IV")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            responderDao.create(responder3);
            responderDao.create(responder4);
            responderDao.create(responder5);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            Long count = responderDao.enrolledRespondersCount();
            assertThat(count, equalTo(3L));
            return null;
        });
    }

    @Test
    public void testActiveRespondersCount() {
        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo I")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo II")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .build();

        ResponderEntity responder3 = new ResponderEntity.Builder()
                .name("John Foo III")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(false)
                .build();

        ResponderEntity responder4 = new ResponderEntity.Builder()
                .name("John Foo IV")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(false)
                .build();

        ResponderEntity responder5 = new ResponderEntity.Builder()
                .name("John Foo IV")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            responderDao.create(responder3);
            responderDao.create(responder4);
            responderDao.create(responder5);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            Long count = responderDao.activeRespondersCount();
            assertThat(count, equalTo(2L));
            return null;
        });
    }

    @Test
    public void testAvailableRespondersOrdered() {
        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo I")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo II")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(true)
                .build();

        ResponderEntity responder3 = new ResponderEntity.Builder()
                .name("John Foo III")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        ResponderEntity responder4 = new ResponderEntity.Builder()
                .name("John Foo IV")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(true)
                .build();

        ResponderEntity responder5 = new ResponderEntity.Builder()
                .name("John Foo V")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .person(false)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            responderDao.create(responder3);
            responderDao.create(responder4);
            responderDao.create(responder5);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            List<ResponderEntity> responders = responderDao.availableResponders(100, 0);
            assertThat(responders.size(), equalTo(4));
            assertThat(responders.get(0).isPerson(), equalTo(true));
            assertThat(responders.get(1).isPerson(), equalTo(true));
            assertThat(responders.get(2).isPerson(), equalTo(false));
            return null;
        });
    }

    @Test
    public void testAvailableRespondersOrderedWithLimit() {
        responderDao.deleteAll();

        //stop the current transaction
        TestTransaction.end();

        ResponderEntity responder = new ResponderEntity.Builder()
                .name("John Foo I")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        ResponderEntity responder2 = new ResponderEntity.Builder()
                .name("John Foo II")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(true)
                .build();

        ResponderEntity responder3 = new ResponderEntity.Builder()
                .name("John Foo III")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(false)
                .build();

        ResponderEntity responder4 = new ResponderEntity.Builder()
                .name("John Foo IV")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(true)
                .enrolled(true)
                .person(true)
                .build();

        ResponderEntity responder5 = new ResponderEntity.Builder()
                .name("John Foo V")
                .phoneNumber("999-888-777")
                .currentPositionLatitude(new BigDecimal("35.12345"))
                .currentPositionLongitude(new BigDecimal("-75.98765"))
                .boatCapacity(2)
                .medicalKit(true)
                .available(false)
                .enrolled(true)
                .person(false)
                .build();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            responderDao.create(responder);
            responderDao.create(responder2);
            responderDao.create(responder3);
            responderDao.create(responder4);
            responderDao.create(responder5);
            return null;
        });

        template = new TransactionTemplate(transactionManager);
        template.execute((TransactionStatus s) -> {
            List<ResponderEntity> responders = responderDao.availableResponders(3, 0);
            assertThat(responders.size(), equalTo(3));
            assertThat(responders.get(0).isPerson(), equalTo(true));
            assertThat(responders.get(1).isPerson(), equalTo(true));
            assertThat(responders.get(2).isPerson(), equalTo(false));
            return null;
        });
    }
}
