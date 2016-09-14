flow_file <- session$get()

if (is.null(flow_file)) {
    return
}

flow_file <- session$putAttribute(flow_file, "from-content", "Hello world")
session$transfer(flow_file, REL_SUCCESS)