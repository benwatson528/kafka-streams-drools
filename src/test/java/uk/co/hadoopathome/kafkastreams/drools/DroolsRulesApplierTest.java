package uk.co.hadoopathome.kafkastreams.drools;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DroolsRulesApplierTest {

    @Test
    public void testValidSessionIsCreated() {
        DroolsRulesApplier rulesApplier = new DroolsRulesApplier("IfContainsEPrepend0KS");
        assertNotNull(rulesApplier);
    }

    @Test
    public void testRuleNotTriggered() throws Exception {
        DroolsRulesApplier rulesApplier = new DroolsRulesApplier("IfContainsEPrepend0KS");
        String output = rulesApplier.applyRule("canal");

        assertEquals("canal", output);
    }

    @Test
    public void testRuleTriggered() throws Exception {
        DroolsRulesApplier rulesApplier = new DroolsRulesApplier("IfContainsEPrepend0KS");
        String output = rulesApplier.applyRule("camel");

        assertEquals("The rule isn't being applied", "0camel", output);
    }
}