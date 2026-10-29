plugins {
    java
    application
}

application {
    mainClass.set("com.checkout.ingestor.IngestorApp")
}

dependencies {
    implementation(project(":checkout-event-schema"))
    implementation(libs.flink.streaming.java)
    implementation(libs.flink.clients)
    implementation(libs.flink.connector.kafka)
    implementation(libs.flink.connector.files)
    implementation(libs.flink.s3.fs.native)
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
    implementation(libs.aws.sdk.s3)
    runtimeOnly(libs.log4j.core)
    runtimeOnly(libs.log4j.slf4j2.impl)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit.jupiter)
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
