# Java support — 0.1.0

[← Back to documentation and versions / Назад к документации и версиям](index.md)

[English guide](0.1.0/en.md) · [Русское руководство](0.1.0/ru.md)

| Runtime | Library features used | Compilation target |
| --- | --- | --- |
| Java 17 | Records for transfer values and limits; instanceof pattern matching; Reactive Streams through Reactor | Java 17, class major 61 |
| Java 21 LTS | Runs the same asynchronous Java 17 API. Virtual threads are not needed by the Netty transport | Java 17 |
| Java 25 LTS | Same API; no ScopedValue or version-specific adapter | Java 17 |
| Java 27 | Same API; no preview features, Vector API or Java 27-only methods | Java 17 |

Runtime support does not imply that the library uses every feature introduced by that runtime. JVM improvements remain properties of the selected JVM; this project makes no unmeasured performance claims about them. See actual runs in [verification](VERIFICATION.md).

## По-русски

Минимальная версия — Java 17. Records описывают результат, ограничения и диапазон; pattern matching упрощает разбор ошибок. На Java 21, 25 и 27 используется тот же API и байткод Java 17. Виртуальные потоки, ScopedValue и preview API библиотеке не требуются: передача выполняется асинхронно через Reactor и Netty. Поддержка JVM не означает внедрение всех её новых возможностей. Результаты фактических запусков приведены в отчёте проверки.

[← Back to documentation and versions / Назад к документации и версиям](index.md)
