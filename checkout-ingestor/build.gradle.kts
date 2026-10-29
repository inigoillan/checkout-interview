plugins {
    java
    application
}

application {
    mainClass.set("com.checkout.ingestor.IngestorApp")
}

repositories {
    // io.confluent artifacts are not published to Maven Central.
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation(project(":checkout-event-schema"))
    implementation(libs.kafka.streams)
    implementation(libs.kafka.clients)
    // S3 sink connector (Kafka Connect plugin). Not on Maven Central,
    // resolved from the Confluent repo declared below. Its POM pulls
    // Confluent's 7.7.11-ccs kafka-clients, which would downgrade the
    // module's Kafka 4.3.2 client out from under kafka-streams — excluded
    // so the declared 4.3.2 client stays (connect-api is provided-scope
    // in the connector POM, so nothing else leaks in).
    // Connect plugin API (provided-scope in the connector POM, so it must be
    // declared explicitly wherever connector classes are referenced).
    implementation(libs.connect.api)
    implementation(libs.connect.json)
    implementation(libs.kafka.connect.s3) {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation(libs.slf4j.api)
    implementation(libs.aws.sdk.s3)
    runtimeOnly(libs.log4j.core)
    runtimeOnly(libs.log4j.slf4j2.impl)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.localstack)
}

// Generates build/docker-context/libs/ containing this module's jar
// plus every runtime dependency jar. The Dockerfiles COPY this folder.
tasks.register<Copy>("prepareDockerContext") {
    dependsOn("jar")
    description = "Stages the app jar and all runtime dependencies for Docker."
    group = "docker"
    from(tasks.named("jar"))
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("docker-context/libs"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
