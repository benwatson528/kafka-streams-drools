package uk.co.hadoopathome.kafkastreams.drools;

import org.junit.Test;
import org.kie.api.runtime.KieSession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DroolsSessionFactoryTest {

    @Test public void testValidSessionIsCreated() {
        KieSession kieSession = DroolsSessionFactory.createDroolsSession("IfContainsEPrepend0KS");
        assertNotNull(kieSession);
    }

    @Test public void testRuleNotTriggered() throws Exception {
        KieSession kieSession = DroolsSessionFactory.createDroolsSession("IfContainsEPrepend0KS");
        Message message = new Message("canal");
        kieSession.insert(message);
        kieSession.fireAllRules();

        assertEquals("canal", message.getContent());
    }

    @Test public void testRuleTriggered() throws Exception {
        KieSession kieSession = DroolsSessionFactory.createDroolsSession("IfContainsEPrepend0KS");
        Message message = new Message("camel");
        kieSession.insert(message);
        kieSession.fireAllRules();

        assertEquals("The rule isn't being applied", "0camel", message.getContent());
    }
}