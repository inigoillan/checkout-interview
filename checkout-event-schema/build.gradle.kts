plugins {
    `java-library`
}

dependencies {
    api(libs.fory.json)
    api(libs.jackson.databind)
    api(libs.slf4j.api)

    runtimeOnly(libs.log4j.core)
    runtimeOnly(libs.log4j.slf4j2.impl)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
    testImplementation(libs.testcontainers.core)
}
