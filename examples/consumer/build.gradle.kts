plugins { application }
repositories { maven { url = uri("../../build/local-repository") }; mavenCentral() }
dependencies {
    implementation("io.github.59alex:s3-file-streams:0.1.0")
    implementation("io.github.59alex:s3-file-streams-s3:0.1.0")
    implementation("io.github.59alex:s3-file-streams-webflux:0.1.0")
}
java { sourceCompatibility=JavaVersion.VERSION_17; targetCompatibility=JavaVersion.VERSION_17 }
tasks.withType<JavaCompile>().configureEach { options.release.set(17) }
application { mainClass.set("Consumer") }
