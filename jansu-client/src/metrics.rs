use std::sync::LazyLock;

use opentelemetry::{
    InstrumentationScope, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;

use crate::Pool;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

pub(crate) fn status_update(pool: &Pool) {
    let status = pool.status();
    POOL_AVAILABLE.record(status.available as u64, &[]);
    POOL_CURRENT_SIZE.record(status.size as u64, &[]);
    POOL_MAX_SIZE.record(status.max_size as u64, &[]);
    POOL_WAITING.record(status.waiting as u64, &[]);
}

pub(crate) static TCP_CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_connect_duration")
        .with_unit("ms")
        .with_description("The TCP connect latencies in milliseconds")
        .build()
});

pub(crate) static TCP_CONNECT_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_connect_errors")
        .with_description("TCP connect errors")
        .build()
});

pub(crate) static TCP_SEND_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_send_duration")
        .with_unit("ms")
        .with_description("The TCP send latencies in milliseconds")
        .build()
});

pub(crate) static TCP_SEND_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_send_errors")
        .with_description("TCP send errors")
        .build()
});

pub(crate) static TCP_RECEIVE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_receive_duration")
        .with_unit("ms")
        .with_description("The TCP receive latencies in milliseconds")
        .build()
});

pub(crate) static TCP_RECEIVE_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_receive_errors")
        .with_description("TCP receive errors")
        .build()
});

pub(crate) static TCP_BYTES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_sent")
        .with_description("TCP bytes sent")
        .build()
});

pub(crate) static TCP_BYTES_RECEIVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_received")
        .with_description("TCP bytes received")
        .build()
});

pub(crate) static POOL_GET_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("pool_get_duration")
        .with_unit("ms")
        .with_description("The Pool Get latencies in milliseconds")
        .build()
});

static POOL_MAX_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_max_size")
        .with_description("The maximum size of the pool")
        .build()
});

static POOL_CURRENT_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_current_size")
        .with_description("The current size of the pool")
        .build()
});

static POOL_AVAILABLE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_available")
        .with_description("The number of available objects in the pool")
        .build()
});

static POOL_WAITING: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_waiting")
        .with_description("The number of waiting objects in the pool")
        .build()
});
