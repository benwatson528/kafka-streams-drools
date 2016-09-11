package uk.co.hadoopathome.kafkastreams.drools;

import org.junit.Test;
import org.kie.api.runtime.KieSession;

import static org.junit.Assert.assertEquals;

public class DroolsSessionFactoryTest {

    @Test
    public void testDroolsSessionInstantiated() {
        KieSession kieSession = DroolsSessionFactory.createDroolsSession("IfContainsEPrepend0KS");
        Message message = new Message("hello");
        kieSession.insert(message);
        kieSession.fireAllRules();
        assertEquals("0hello", message.getContent());
    }
}