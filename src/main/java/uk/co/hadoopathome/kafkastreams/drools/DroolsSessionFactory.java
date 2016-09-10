package uk.co.hadoopathome.kafkastreams.drools;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

/**
 * Instantiates the Drools KieSession, responsible for applying rules.
 */
public class DroolsSessionFactory {

    /**
     * Creates a new KieSession instance, configured with the rules from the given session.
     *
     * @param sessionName the session name from which rules should be retrieved
     * @return the instantiated KieSession
     */
    protected static KieSession createDroolsSession(String sessionName) {
        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieContainer = kieServices.getKieClasspathContainer();
        return kieContainer.newKieSession(sessionName);
    }
}
