plugins { base }

val testJavaVersion = providers.gradleProperty("testJavaVersion").map(String::toInt)
    .getOrElse(JavaVersion.current().majorVersion.toInt())
subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    group = "io.github.59alex"
    version = "0.1.0"
    repositories { mavenCentral() }
    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(testJavaVersion))
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        withSourcesJar()
        withJavadocJar()
    }
    tasks.withType<JavaCompile>().configureEach { options.release.set(17); options.encoding = "UTF-8" }
    tasks.withType<Test>().configureEach { useJUnitPlatform(); maxHeapSize = if (providers.gradleProperty("memoryProbe").isPresent) "96m" else "192m"
        if (providers.gradleProperty("memoryProbe").isPresent) jvmArgs("-XX:MaxDirectMemorySize=64m") }
    tasks.withType<Jar>().configureEach {
        archiveBaseName.set(if (project.name == "core") "s3-file-streams" else "s3-file-streams-${project.name}")
        from(rootProject.file("LICENSE")) { into("META-INF") }
    }
    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:6.1.3"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "testImplementation"("io.projectreactor:reactor-test:3.8.8")
    }
    if (project.name != "demo") extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("library") {
                from(components["java"])
                artifactId = if (project.name == "core") "s3-file-streams" else "s3-file-streams-${project.name}"
                pom {
                    name.set("S3 File Streams ${project.name}")
                    description.set("Continuous reactive object uploads and downloads with explicit resource bounds")
                    url.set("https://github.com/59Alex/s3-file-streams")
                    licenses { license { name.set("MIT License"); url.set("https://opensource.org/license/mit") } }
                    developers { developer { id.set("59Alex"); name.set("Aleksandr Kashevarov") } }
                    scm { url.set("https://github.com/59Alex/s3-file-streams"); connection.set("scm:git:https://github.com/59Alex/s3-file-streams.git") }
                }
            }
        }
    }
}
