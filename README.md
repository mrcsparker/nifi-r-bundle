# nifi-r-bundle

**Work in progress**

R integration for [Apache NIFI](https://nifi.apache.org).

This library uses the [rJava/JRI](https://cran.r-project.org/web/packages/rJava/index.html) interface for fast access to R from NIFI.  It allows you to use R to write your NIFI processing code.

## Sample code

In order to get it to work, this can't act like a standard NIFI processor.  It implements a callback that gives you access to 3 interfaces:

1. inputStream
2. outputStream
3. log

This actually works out quite well.  You only have to care about reading, writing, and (optionally) logging.

```
ioutils <- .jnew("org.apache.commons.io.IOUtils")

input <- ioutils$toString(inputStream)

ioutils$write("[{\"name\":\"foo\"},{\"value\":\"bar\"}]", outputStream)
```

## Installing

This document is a work in progress, so this is just to get you started.

You are going to need NIFI, R, Java, and rJava installed.

To run the code, you are going to have to set:

1. R_HOME as an environment variable to point to your R install
2. -Djava.library.path or your CLASSPATH to point to the rJava jar files.
3. Install rJava from CRAN

## Status

I just wrote this code and it works here, on my machine, at my home.  Still lots of work to do, but it works for me right now at this moment.



