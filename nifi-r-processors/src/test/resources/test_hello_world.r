ioutils <- .jnew("org.apache.commons.io.IOUtils")

input <- ioutils$toString(inputStream)

ioutils$write("Hello world", outputStream)