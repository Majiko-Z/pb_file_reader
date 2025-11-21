pub mod model;

pub mod timer;

pub fn init_logger_for_test() {
    let logger = ftlog::Builder::new()
        .print_omitted_count(true)
        .appender("stdout", std::io::stdout())
        .build()
        .expect("logger build failed");
    logger.init().expect("set logger failed"); // [for test]
}