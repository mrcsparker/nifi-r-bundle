package org.apache.nifi.processors.r;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Logger;

public class RProcessorTest {
    private static final Logger log = Logger.getLogger(RProcessorTest.class.getName());

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(new RProcessor());
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testProcessor() {

    }

    @Test
    public void testScriptBody() {
        testRunner.setProperty(RProcessor.SCRIPT_BODY, "1 + 1");
        testRunner.assertValid();
    }

    @Test
    public void testSimpleScriptBody() {
        testRunner.setProperty(RProcessor.SCRIPT_BODY, "1 + 1");
        testRunner.assertValid();
        testRunner.run();
    }
}
