use env_logger::Env;

pub fn init_logging() {
    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "trace,hyper_util=warn,hyper=warn")
        .write_style_or("MY_LOG_STYLE", "always");
    env_logger::init_from_env(env);
}
