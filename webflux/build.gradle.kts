dependencies {
    "api"(project(":core"))
    "api"("org.springframework:spring-webflux:7.0.10")
    "implementation"("org.springframework:spring-context:7.0.10")
    "implementation"("tools.jackson.core:jackson-databind:3.2.4")
    "testImplementation"("org.springframework:spring-test:7.0.10")
    "testImplementation"("io.projectreactor.netty:reactor-netty-http:1.3.8")
}
