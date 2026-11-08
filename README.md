# TCP Chat Server

Многопользовательский чат-сервер на Go без внешних зависимостей. Клиенты общаются по TCP, состояние сервера отдаётся по HTTP в формате JSON для систем мониторинга.

## Возможности

- Рассылка сообщений всем подключённым клиентам в реальном времени
- История последних сообщений для новых участников
- Команды в чате: список пользователей, время сервера, статистика
- Ограничение числа одновременных подключений
- Отключение неактивных клиентов по таймауту
- Перехват паник: сбой у одного клиента не останавливает сервер
- Корректное завершение по Ctrl+C и `SIGTERM` с предупреждением клиентов
- HTTP-мониторинг: `/health` и `/stats`
- Настройка через флаги командной строки

## Установка

Нужен Go 1.22 или новее.

```
git clone https://github.com/egorka-hub/tcp-chat.git
cd tcp-chat
go build ./cmd/server
```

Или сразу установить бинарник в `$GOPATH/bin`:

```
go install github.com/egorka-hub/tcp-chat/cmd/server@latest
```

## Запуск

```
go run ./cmd/server
```

С собранным бинарником:

```
./server --port 8080 --max-connections 100
```

### Флаги

| Флаг | По умолчанию | Описание |
|---|---|---|
| `--port` | `8080` | порт чата |
| `--monitor-port` | `8081` | порт HTTP-мониторинга |
| `--max-connections` | `500` | максимум одновременных подключений |
| `--log-level` | `info` | уровень логирования: `debug`, `info`, `warn`, `error` |
| `--history-size` | `50` | сколько последних сообщений показывать новым клиентам |

Полный список: `./server --help`.

## Примеры использования

### Подключение к чату

```
nc localhost 8080
```

или

```
telnet localhost 8080
```

### Сеанс в чате

```
Welcome, User_002! 2 users online. Type /help for commands.
[14:30:15] <User_001>: Hello everyone!
Hi!
/users
[14:30:20] *** Online users (2): User_001, User_002
/quit
[14:30:25] *** Goodbye!
```

### Команды

| Команда | Действие |
|---|---|
| `/users` | список пользователей онлайн |
| `/time` | время сервера |
| `/stats` | статистика сервера |
| `/help` | список команд |
| `/quit` | выйти из чата |

### Мониторинг

```
$ curl http://localhost:8081/health
{"status":"healthy","active_connections":2,"uptime_seconds":120}

$ curl http://localhost:8081/stats
{
  "active_connections": 2,
  "total_messages_processed": 57,
  "uptime_seconds": 120,
  "error_count": 0,
  "message_rate": 0.48
}
```

Во время завершения `/health` возвращает `"status":"shutting_down"`.

### Лог сервера

```
[TCP-CHAT] 2026/11/08 14:30:00 INFO  TCP Chat Server listening on :8080
[TCP-CHAT] 2026/11/08 14:30:10 INFO  Client User_001 connected from 127.0.0.1:52344
[TCP-CHAT] 2026/11/08 14:30:15 INFO  Client User_001: Hello everyone!
```

## Завершение работы

По Ctrl+C или `SIGTERM` сервер перестаёт принимать подключения, предупреждает клиентов и даёт им 10 секунд на выход. После этого он закрывает оставшиеся соединения. Повторный Ctrl+C завершает процесс сразу.

## Структура проекта

```
cmd/server/main.go       точка входа
internal/app/            запуск, сигналы, завершение
internal/config/         флаги и проверка конфигурации
internal/domain/         структуры данных
internal/hub/            рассылка сообщений, клиенты, команды, история, статистика
internal/server/         TCP-сервер и HTTP-мониторинг
```
