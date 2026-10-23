plugins { application }
dependencies {
    implementation(project(":s3"))
    implementation(project(":webflux"))
    implementation("io.projectreactor.netty:reactor-netty-http:1.3.8")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")
}
application { mainClass.set("io.github.alex59.filestreams.demo.DemoServer") }
