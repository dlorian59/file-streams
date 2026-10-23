dependencies {
    "api"(platform("io.netty:netty-bom:4.2.19.Final"))
    "api"(project(":core"))
    "api"("software.amazon.awssdk:s3:2.57.3")
    "implementation"("software.amazon.awssdk:netty-nio-client:2.57.3")
    "testRuntimeOnly"("org.slf4j:slf4j-simple:2.0.17")
}
