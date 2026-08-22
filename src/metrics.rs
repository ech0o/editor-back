use prometheus::Encoder;

pub fn gather() ->String {
    let metric_families = prometheus::gather();
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}