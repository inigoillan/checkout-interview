rootProject.name = "checkout"

include(
    "checkout-event-schema",
    "checkout-ingestor",
    "checkout-quality-check",
    "checkout-count",
    "checkout-event-emitter"
)
