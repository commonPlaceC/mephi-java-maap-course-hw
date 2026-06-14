# Курсовая работа — Агарков А.В.

Проект по дисциплине «Многопоточное и асинхронное программирование на Java». Два независимых модуля: собственный пул потоков и учебная реактивная библиотека

## Модули

| Модуль | Содержание | Проверка |
|--------|------------|----------|
| [lab1-executor](lab1-executor/README.md) | `ManagedExecutorPool`, Round Robin, демо `ExecutorLab` | `./gradlew :lab1-executor:run` |
| [lab2-reactive](lab2-reactive/README.md) | `Observable`, операторы, schedulers, 18 тестов | `./gradlew :lab2-reactive:test` |

## Требования

- JDK 21+
- Gradle wrapper (`./gradlew`)

## Быстрый старт

```bash
# полная сборка
./gradlew build

# демонстрация пула потоков
./gradlew :lab1-executor:run

# только бенчмарк пула
./gradlew :lab1-executor:run --args="benchmark"

# тесты реактивной библиотеки
./gradlew :lab2-reactive:test
```

## Структура

```
kursovaya-alexey/
├── lab1-executor/     # задание 1
├── lab2-reactive/     # задание 2
├── build.gradle.kts
└── settings.gradle.kts
```

Подробные отчеты – смотри в README каждой лабы
