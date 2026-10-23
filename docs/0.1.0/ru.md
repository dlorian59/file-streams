# s3-file-streams 0.1.0 — руководство

[EN](en.md) · [RU](ru.md) · [Версии](../README.md) · [Java 17 / 21 / 25 / 27](../JAVA-SUPPORT.md)

## Что умеет библиотека

- Загружает файл одним потоком, с известным или неизвестным размером.
- Проверяет длину, максимальный объём и размер отдельного буфера; счётчики используют long.
- Передаёт данные через штатный AWS async multipart с ограничением буфера и числа передач.
- Различает прочитанные байты, подтверждённое сохранение и неизвестный результат.
- Скачивает весь объект или один диапазон, освобождает ресурсы при завершении и отмене.
- Предоставляет WebFlux-адаптер и независимый браузерный модуль с AbortSignal и прогрессом.

## 1. Подключение модулей

Сначала соберите локальные артефакты:

```bash
./gradlew build publishToMavenLocal
```

В Gradle Kotlin DSL проекта-потребителя:

```kotlin
repositories { mavenLocal(); mavenCentral() }
dependencies {
    implementation("io.github.59alex:s3-file-streams-s3:0.1.0")
    implementation("io.github.59alex:s3-file-streams-webflux:0.1.0")
}
```

Core подключается транзитивно. S3-модуль экспортирует AWS API и Netty BOM, чтобы версии Netty у AWS и Reactor не конфликтовали. В сборку входят sources, Javadoc и MIT-лицензия. Эти координаты не означают, что выпуск уже опубликован в Maven Central: публикация — отдельный этап.

## 2. Клиент и ограничения

Фрагменты ниже предназначены для метода приложения. Клиент должен жить до завершения всех асинхронных передач; нельзя закрывать его сразу после возврата ещё не подписанного Mono.

```java
import io.github.alex59.filestreams.*;
import io.github.alex59.filestreams.s3.*;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

var client = S3Clients.bounded(
    URI.create("http://127.0.0.1:19390"), Region.of("garage"),
    DefaultCredentialsProvider.create());
var limits = new StreamLimits(16L * 1024 * 1024 * 1024, 1024 * 1024);
var files = new S3ObjectStreams(client, "my-bucket", limits, 2);
```

В примере допускаются две одновременные передачи. Лишний запрос получает BUSY, а не встаёт в неограниченную очередь. Внутренняя часть S3 — 5 MiB, настройка буфера SDK — 10 MiB, одновременно отправляется одна часть. Это не лимит RSS всего процесса: память также расходуют TLS, Netty, JVM и приложение. Фабрика не наследует настройки HTTP-прокси из окружения; для явного прокси можно передать настроенный AWS-клиент с включённым multipart и теми же ограничениями. Credentials и bucket задаёт приложение.

## 3. Загрузка с неизвестной или известной длиной

```java
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import reactor.core.publisher.Flux;

var source = Flux.just(ByteBuffer.wrap(new byte[] {1, 2, 3}));
UploadResult result = files.upload(
    UploadRequest.unknownLength("example.bin"), source).block();
System.out.println(result.bytes()); // 3, после подтверждения S3.
```

Вместо маленького массива передайте Publisher<ByteBuffer>, который соблюдает спрос подписчика. block() здесь допустим на границе самостоятельного примера, но не в потоке Netty.

```java
var request = new UploadRequest("known.bin", OptionalLong.of(3),
    "application/octet-stream", Map.of("purpose", "example"));
files.upload(request, Flux.just(ByteBuffer.wrap(new byte[] {1, 2, 3}))).block();
```

Если заявлено три байта, два или четыре приводят к ошибке. Длина не используется для тихого обрезания потока. Пустой поток создаёт пустой объект после проверки EOF. Превышение maxBytes или maxBufferBytes останавливает загрузку до финализации. Не изменяйте переданные буферы до завершения передачи: core сохраняет position, но не копирует произвольную память вызывающего кода.

## 4. Отмена, срок передачи и неизвестный результат

```java
import java.time.Duration;

var freshSource = Flux.just(ByteBuffer.wrap(new byte[] {4, 5, 6}));
var subscription = files.upload(
    UploadRequest.unknownLength("deadline.bin"), freshSource)
    .timeout(Duration.ofMinutes(5))
    .subscribe(result -> System.out.println("Committed: " + result.key()),
               failure -> System.err.println("No confirmed acknowledgement"));
subscription.dispose(); // Например, по событию отмены в приложении.
```

До EOF отмена прекращает чтение и передаёт ошибку SDK, чтобы тот прервал multipart. При гонке с финализацией объект всё же может сохраниться. Библиотека не удаляет его для имитации отката. Потерянное подтверждение нужно сверять по идентификатору загрузки или ключу, прежде чем повторять запрос. Не применяйте retry() к уже потреблённому потоку. Для каждого нового запроса нужен новый источник. В S3 настройте удаление незавершённых multipart через lifecycle на случай падения процесса или недоступности хранилища. ETag не является контрольной суммой.

## 5. Скачивание и владение ресурсом

```java
import reactor.core.publisher.Mono;

Long bytes = Mono.usingWhen(files.download("example.bin", null),
    download -> download.body().map(ByteBuffer::remaining)
        .reduce(0L, (count, size) -> count + size),
    download -> Mono.fromRunnable(download::close)).block();
```

Пример считает байты постепенно, не собирая объект в памяти. Замените счётчик неблокирующим приёмником, соблюдающим backpressure. Открытый ObjectDownload нужно прочитать или закрыть: он занимает слот передачи. usingWhen управляет ресурсом до завершения асинхронной операции. Блокирующий OutputStream нельзя вызывать прямо из event loop.

## 6. Диапазоны

```java
Mono.usingWhen(files.download("example.bin", "bytes=1-2"),
    download -> {
        System.out.println(download.range().responseHeader()); // bytes 1-2/3
        return download.body().then();
    }, download -> Mono.fromRunnable(download::close)).block();
```

Поддерживаются также открытый конец и суффикс: bytes=1- и bytes=-2. Несколько диапазонов, переполнение числа, нулевой суффикс и недостижимое смещение отклоняются. HEAD определяет размер и ETag, затем условный GET защищает от изменения объекта между запросами. Старые метаданные не выдаются вместе с новым содержимым.

## 7. WebFlux

```java
import io.github.alex59.filestreams.webflux.ObjectStreamHandler;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;

var handler = WebHttpHandlerBuilder
    .webHandler(new ObjectStreamHandler(files, limits))
    .build();
```

Подключите HttpHandler к серверу и поставьте проверку доступа перед ним. PUT /objects/{key} принимает сырое тело, GET отдаёт поток. Входные DataBuffer копируются в ограниченные собственные буферы и освобождаются. Авторизационные заголовки и произвольные метаданные в S3 не переносятся. Multipart/form-data не поддерживается. Полный [HTTP-контракт](../HTTP-CONTRACT.md) описывает 201, committed:false/null, Range и CORS.

## 8. Браузер

В [руководстве браузерного модуля](../../browser/README.md) есть примеры загрузки, записи в WritableStream, прогресса, отмены и диапазонов. [Функциональный пример](../../browser/demo/index.html) соединяет браузер с Java-сервером.

```bash
cd browser
npm ci
npm test
npm run test:package
npm run demo
```

Успешная проверка конструктора Request ещё не доказывает поддержку передачи: важны браузер, TLS, версия HTTP и прокси. API не подменяет поток скрытым накоплением Blob. После отмены в целевом WritableStream могут остаться частично записанные данные.

## Локальная демонстрация

Из корня репозитория, с Docker и Java 17+:

```bash
scripts/start-garage.sh
./gradlew :demo:installDist
mkdir -p build/tls
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout build/tls/key.pem -out build/tls/cert.pem \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
DEMO_ORIGIN=https://127.0.0.1:9443 \
DEMO_TLS_CERT="$PWD/build/tls/cert.pem" DEMO_TLS_KEY="$PWD/build/tls/key.pem" \
  demo/build/install/demo/bin/demo
```

Сервер слушает https://127.0.0.1:19443, использует публичные тестовые credentials Garage, разрешает выбранный origin и не сжимает HTTP-ответы. Авторизации здесь нет. Доверяйте созданному сертификату только в отдельном тестовом браузерном окружении. Запустите browser demo на 9443 и выберите Java integration check. Не публикуйте эти тестовые серверы наружу. При занятом порте или имени контейнера задайте GARAGE_PORT/GARAGE_RUN_ID и S3_ENDPOINT.

Остановите только контейнер, созданный скриптом:

```bash
docker rm -f "$(cat build/garage/container-id)"
```

## 9. Проверки и диагностика

```bash
S3_TEST_ENDPOINT=http://127.0.0.1:19390 ./gradlew build
./gradlew publishToMavenLocal -Dmaven.repo.local="$PWD/build/local-repository"
./gradlew -p examples/consumer run
S3_TEST_ENDPOINT=http://127.0.0.1:19390 S3_MEMORY_BYTES=2147483649 \
  ./gradlew :s3:test --tests '*MemoryProbeTest' -PmemoryProbe
```

Storage-тесты предназначены только для отдельного тестового bucket. 502 и потерянный ответ не доказывают отсутствие объекта. BUSY означает занятые слоты: уменьшите конкуренцию или закройте брошенные скачивания. Смешивание Netty 4.1 и 4.2 может приводить к ошибкам во время выполнения; сохраняйте экспортируемый BOM и проверяйте итоговые зависимости. AbortMultipartUpload требует разрешений, а очистка после падения — lifecycle. [Отчёт](../VERIFICATION.md) отделяет выполненные проверки от непроверенных сред.
