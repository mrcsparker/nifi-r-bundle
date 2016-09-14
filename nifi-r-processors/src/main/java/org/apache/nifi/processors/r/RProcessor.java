/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.r;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPJavaReference;
import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineStdOutput;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RProcessor extends AbstractSessionFactoryProcessor {

    protected BlockingQueue<REngine> engineQ = null;
    private Boolean showLineNumbers = false;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private String scriptToRun = null;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be processed")
            .build();

    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .description("Path to script file to execute. Only one of Script File or Script Body may be used")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("Script Body")
            .required(false)
            .description("Body of script to execute. Only one of Script File or Script Body may be used")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor R_HOME = new PropertyDescriptor.Builder()
            .name("R home")
            .description("Point to R home")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();

        descriptors.add(SCRIPT_FILE);
        descriptors.add(SCRIPT_BODY);
        descriptors.add(R_HOME);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        System.out.println(System.getProperty("java.library.path"));
        System.setProperty("java.library.path", "/usr/local/lib/R/3.3/site-library/rJava/jri");
        Field fieldSysPath = null;
        try {
            fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        fieldSysPath.setAccessible(true);
        try {
            fieldSysPath.set(null, null);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        String scriptBody = context.getProperty(SCRIPT_BODY).getValue();
        String scriptFile = context.getProperty(SCRIPT_FILE).getValue();

        scriptToRun = scriptBody;

        try {
            if (scriptToRun == null && scriptFile != null) {
                try (final FileInputStream scriptStream = new FileInputStream(scriptFile)) {
                    scriptToRun = IOUtils.toString(scriptStream);
                }
            }
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }

        int maxTasks = context.getMaxConcurrentTasks();

        setupScriptingContainers(context, maxTasks);
    }

    protected void setupScriptingContainers(final ProcessContext context, int numberOfContainers) {
        engineQ = new LinkedBlockingQueue<>(numberOfContainers);
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            ComponentLog log = getLogger();

            for (int i = 0; i < numberOfContainers; i++) {
                try {
                    REngine rEngine = setupScriptingContainer(context);
                    if (!engineQ.offer(rEngine)) {
                        log.error("Error adding R script container");
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    log.error("Error adding R script container: ClassNotFoundException");
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                    log.error("Error adding R script container: NoSuchMethodException");
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                    log.error("Error adding R script container: InvocationTargetException");
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    log.error("Error adding R script container: IllegalAccessException");
                }

            }
        } finally {
            // Restore original context class loader
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    protected REngine setupScriptingContainer(final ProcessContext context) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String[] args = {"--vanilla"};
        REngine rEngine = REngine.engineForClass("org.rosuda.REngine.JRI.JRIEngine", args, new REngineStdOutput(), false);

        return rEngine;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSessionFactory processSessionFactory) throws ProcessException {
        ProcessSession session = processSessionFactory.createSession();
        ComponentLog log = getLogger();

        REngine rEngine = engineQ.poll();

        try {
            try {

                rEngine.parseAndEval("{ library(rJava); .jinit() }");
                rEngine.assign("session", new REXPJavaReference(session));
                rEngine.assign("context", new REXPJavaReference(processContext));
                rEngine.assign("log", new REXPJavaReference(log));
                rEngine.assign("REL_SUCCESS", new REXPJavaReference(REL_SUCCESS));
                rEngine.assign("REL_FAILURE", new REXPJavaReference(REL_FAILURE));

                rEngine.parseAndEval(scriptToRun);

                session.commit();
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        } catch (final Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            session.rollback(true);
            throw t;
        } finally {
            engineQ.offer(rEngine);
        }

    }

    @OnStopped
    public void stop() {
        if (engineQ != null) {
            engineQ.clear();
        }
    }
}
