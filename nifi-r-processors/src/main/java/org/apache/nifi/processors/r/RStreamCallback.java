package org.apache.nifi.processors.r;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.io.StreamCallback;
import org.rosuda.REngine.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RStreamCallback implements StreamCallback {

    private REngine rEngine;
    private ComponentLog log;
    private String scriptToRun;

    public RStreamCallback(REngine rEngine, ComponentLog log, String scriptToRun) {
        this.rEngine = rEngine;
        this.log = log;
        this.scriptToRun = scriptToRun;
    }

    @Override
    public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
        try {
            rEngine.assign("inputStream", new REXPJavaReference(inputStream));
            rEngine.assign("outputStream", new REXPJavaReference(outputStream));
            rEngine.assign("log", new REXPJavaReference(log));
            rEngine.parseAndEval(scriptToRun);
        } catch (REngineException e) {
            e.printStackTrace();
            throw new IOException("REngineException: " + e.getMessage());
        } catch (REXPMismatchException e) {
            e.printStackTrace();
            throw new IOException("REXPMismatchException: " + e.getMessage());
        }
    }
}
