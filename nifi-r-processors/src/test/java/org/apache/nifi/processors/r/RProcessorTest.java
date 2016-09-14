package org.apache.nifi.processors.r;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
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
    public void testHelloWorldScript() {

        testRunner.setProperty(RProcessor.SCRIPT_FILE, rFile("/test_hello_world.r"));

        testRunner.assertValid();
        testRunner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = testRunner.getFlowFilesForRelationship("success");
        result.get(0).assertContentEquals("Hello world");
    }

    @Test
    public void testInputTransformation() {

        testRunner.setProperty(RProcessor.SCRIPT_FILE, rFile("/test_input_transformation.r"));

        testRunner.assertValid();
        testRunner.enqueue("name=foo;value=bar".getBytes(StandardCharsets.UTF_8));

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = testRunner.getFlowFilesForRelationship("success");
        MockFlowFile f = result.get(0);

        Assert.assertEquals("[{\"name\":\"foo\"},{\"value\":\"bar\"}]", new String(f.toByteArray(), StandardCharsets.UTF_8));
    }

    private String rFile(String fileName) {
        return this.getClass().getResource(fileName).getPath();
    }
}
