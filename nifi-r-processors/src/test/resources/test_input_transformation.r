ioutils <- .jnew("org.apache.commons.io.IOUtils")

input <- ioutils$toString(inputStream)

ioutils$write("[{\"name\":\"foo\"},{\"value\":\"bar\"}]", outputStream)