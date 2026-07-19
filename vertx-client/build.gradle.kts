/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

plugins {
    java
    `java-library`
}

tasks.withType<Test> {
    useJUnitPlatform()
}

java {
    targetCompatibility = JavaVersion.VERSION_17
    sourceCompatibility = JavaVersion.VERSION_17
}

tasks.compileJava {
    options.release.set(17)
}

dependencies {
    api(project(":java-client"))
    api("io.vertx:vertx-core:5.1.5")
    api("jakarta.json:jakarta.json-api:2.1.3")
    implementation("org.eclipse.parsson:parsson:1.1.7")
    implementation("commons-logging:commons-logging:1.3.5")

    val jacksonVersion = "2.17.0"
    testImplementation("com.fasterxml.jackson.core", "jackson-core", jacksonVersion)
    testImplementation("com.fasterxml.jackson.core", "jackson-databind", jacksonVersion)
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.0")
}

repositories {
    mavenCentral()
}
