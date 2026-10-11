# Trip Service

HTTP-микросервис поездок на Go. Принимает запросы по OpenAPI-контракту
`contracts/openapi/trip-service.openapi.yaml`, хранит данные в PostgreSQL,
поддерживает идемпотентность `POST /api/v1/trips` по заголовку `Idempotency-Key`.

## Требования

- Go **1.26+**
- Docker (для локального окружения)
- `tripgoctl` (утилита курса) — поднимает PostgreSQL
- `psql` — опционально, для отладки БД
- `golangci-lint` — опционально, для линта

## Быстрый старт

```bash
# 1) поднять окружение (PostgreSQL в Docker)
tripgoctl cluster start
tripgoctl environment start
tripgoctl connect

# 2) накатить миграции
make migrate

# 3) запустить сервис
make run

# 4) проверить готовность
curl -sS localhost:8080/health | jq
curl -sS localhost:8080/ready  | jq
```

## Переменные окружения

Все параметры читаются из env. Значения по умолчанию — в `.env.example`.
Реальный `.env` генерируется `tripgoctl` и **не коммитится**.

### Сервер

| Переменная | Описание |
|------------|----------|
| `HTTP_ADDR`  | Адрес и порт HTTP-сервера |
| `LOG_LEVEL`  | Уровень логирования (`debug`, `info`, `warn`, `error`) |
| `SHUTDOWN_TIMEOUT`  | Таймаут graceful shutdown |
| `HTTP_READ_TIMEOUT`  | `http.Server.ReadTimeout` |
| `HTTP_READ_HEADER_TIMEOUT` | `http.Server.ReadHeaderTimeout` |
| `HTTP_WRITE_TIMEOUT` | `http.Server.WriteTimeout` |
| `HTTP_IDLE_TIMEOUT` | `http.Server.IdleTimeout` |

### PostgreSQL

| Переменная | Описание |
|------------|----------|
| `DATABASE_URL`| Строка подключения  |
| `DATABASE_MAX_CONNS` | Максимум соединений в пуле |
| `DATABASE_MIN_CONNS` | Минимум соединений в пуле |
| `DATABASE_MAX_CONN_LIFETIME` | Время жизни соединения |
| `DATABASE_CONNECT_TIMEOUT` | Таймаут подключения |
| `DATABASE_QUERY_TIMEOUT` | Таймаут запроса |


## API

Источник — `contracts/openapi/trip-service.openapi.yaml`. Реализованы:

| Метод | Путь | Ответы |
|-------|------|--------|
| `POST` | `/api/v1/trips` | 201, 400, 409, 500 |
| `GET` | `/api/v1/trips/{tripId}` | 200, 400, 404, 500 |
| `POST` | `/api/v1/trips/{tripId}/finish` | 200, 400, 404, 409, 500 |
| `GET` | `/health` | 200 |
| `GET` | `/ready` | 200, 503 |

Ошибки отдаются в `application/problem+json` (RFC 9457) с расширением `code`.

### Примеры

```bash
# создание
curl -sS -X POST localhost:8080/api/v1/trips \
  -H 'content-type: application/json' \
  -d '{"user_id":"5cb72c04-7650-45c9-a79b-bcdba0631e0c",
       "driver_id":"8860b315-ec86-42eb-a17c-7c163d721ff5",
       "start_point":{"latitude":59.9398,"longitude":30.3146},
       "end_point":{"latitude":59.9290,"longitude":30.3626},
       "price":1450}'

# получение
curl -sS localhost:8080/api/v1/trips/$TRIP_ID | jq

# завершение
curl -sS -X POST localhost:8080/api/v1/trips/$TRIP_ID/finish | jq
```

## Makefile

| Цель | Что делает |
|------|------------|
| `make generate` | Генерирует `internal/generated/api.gen.go` из OpenAPI |
| `make migrate` | Накатывает миграции |
| `make migrate-down` | Откатывает последнюю миграцию |
| `make migrate-status` | Показывает статус миграций |
| `make run` | Запускает сервис |
| `make build` | Собирает бинарник в `bin/trip-service` |
| `make lint` | Запускает `golangci-lint` |

## Решения

### Уровень изоляции — `READ COMMITTED`

По умолчанию PostgreSQL использует `READ COMMITTED`. Корректность
бизнес-правил держится **не изоляцией**, а явными механизмами:

- запрет двух активных поездок — **частичным уникальным индексом**;
- двойное завершение — **условным `UPDATE`** с проверкой статуса в `WHERE`.

Почему не `SERIALIZABLE`: он дал бы нам сериализационные ошибки `40001`
под нагрузкой и требовал бы retry-логики. Наши ограничения уже
обеспечиваются индексами и условными апдейтами, поэтому более строгий
уровень не нужен.

### Менеджер транзакций

Интерфейс:

```go
type TxManager interface {
    Do(ctx context.Context, fn func(ctx context.Context) error) error
}
```

Реализация (`internal/txmanager`):

1. `Do` открывает транзакцию через `pool.BeginTx`, кладёт её в `context`
   под приватным ключом и вызывает `fn` с обогащённым контекстом.
2. `fn` вернула `nil` → `COMMIT`; вернула ошибку или панику → `ROLLBACK`
   и проброс наружу. Паника обрабатывается через `defer` + `recover`.
3. Вложенный `Do` внутри `Do` **переиспользует ту же транзакцию**: если в
   контексте уже лежит `pgx.Tx`, новый `Begin` не открывается.
4. Уровень изоляции задаётся один раз при старте сервиса
   (`txmanager.New(pool, pgx.ReadCommitted)`).

Репозитории не принимают транзакцию аргументом. Они достают исполнителя из
контекста через `Manager.Querier(ctx)`:

```go
func (m *Manager) Querier(ctx context.Context) Querier {
    if tx, ok := ctx.Value(ctxKey{}).(pgx.Tx); ok {
        return tx   // есть транзакция — работаем через неё
    }
    return m.pool   // нет — работаем через пул
}
```

Если репозиторий вызван вне `Do`, он работает через пул (без транзакции).
Бизнес-код не знает про `pgx`, `BEGIN`, `COMMIT`, `ROLLBACK`.

### Запрет двух активных поездок

Частичный уникальный индекс в `trips`:

```sql
CREATE UNIQUE INDEX trips_driver_active_uidx
    ON trips (driver_id)
    WHERE status = 'active';
```

При гонке второй `INSERT` получает `23505`, репозиторий по имени
индекса (`trips_driver_active_uidx`) превращает ошибку в `ErrDriverBusy`,
хендлер — в `409 driver_busy`.

Завершение поездки **автоматически** освобождает водителя: строка
перестаёт попадать под `WHERE status = 'active'`, и следующий `INSERT`
для того же водителя проходит.

Проверка `SELECT` перед `INSERT` не годится: два параллельных запроса
пройдут проверку раньше, чем любой из них вставит строку. Уникальный
индекс сериализует вставки на уровне БД.

### Защита от двойного finish

Условный `UPDATE`:

```sql
UPDATE trips
SET status = 'completed', finished_at = $1, updated_at = $1
WHERE id = $2 AND status = 'active'
RETURNING ...
```

Второй параллельный `UPDATE` ждёт блокировку строки, после коммита
первого перепроверяет `WHERE`, не находит активной строки и затрагивает
**0 строк**. Тогда дополнительным `SELECT` различаем случаи:

- поездки нет → `404 trip_not_found`;
- поездка уже завершена → `409 trip_completed`.

`finished_at` не перезаписывается.

### Идемпотентность `POST /api/v1/trips`

Поддерживается заголовок `Idempotency-Key` (UUID, генерирует клиент).

| Запрос | Ответ |
|--------|-------|
| Первый с ключом | `201`, поездка создана |
| Повтор с тем же ключом и телом | `200`, та же поездка |
| Тот же ключ, другое тело | `409 idempotency_conflict` |
| Без заголовка | как обычно, `201` |

**Хранение.** Ключи лежат в таблице `idempotency_keys`:

```sql
CREATE TABLE idempotency_keys (
    key          UUID PRIMARY KEY,
    request_hash TEXT NOT NULL,
    trip_id      UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE
                 DEFERRABLE INITIALLY DEFERRED,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Срок жизни ключа — 24 часа.** Записи старше удаляются SQL-очисткой
(фоновая задача, если запущена, или ручной запрос).

**Сравнение тела.** Считается SHA-256 от канонизированного JSON
(`json.Unmarshal` → `json.Marshal map[string]any`). Порядок ключей и
пробелы не влияют на хеш. При совпадении хеша возвращается существующая
поездка; при несовпадении — `409 idempotency_conflict`.

**Порядок операций.** Всё в одной транзакции:

1. `INSERT INTO idempotency_keys` (ключ + хеш + `trip_id`).
2. `INSERT INTO trips`.
3. `INSERT INTO trip_status_history`.

Внешний ключ `trip_id` объявлен как `DEFERRABLE INITIALLY DEFERRED`:
проверка ссылочной целостности откладывается до `COMMIT`, поэтому ключ
можно вставить раньше поездки.

**Гонка.** Два параллельных запроса с одним ключом:

- первый вставляет ключ и создаёт поездку;
- второй блокируется на уникальном индексе `PRIMARY KEY` до коммита
  первого, затем получает `23505` → `ErrKeyAlreadyExists`;
- транзакция второго откатывается, а ключ читается **в новой транзакции**
  (вне упавшей) — так избегаем ошибки `25P02 current transaction is aborted`;
- если хеш совпал — возвращаем поездку первого с кодом `200`.

Итог: одна поездка, ни одного дубля.

## Проверка

### Гонка create — 20 параллельных на одного водителя

```bash
DRIVER=$(cat /proc/sys/kernel/random/uuid)
cat > /tmp/trip_race.json <<EOF
{"user_id":"5cb72c04-7650-45c9-a79b-bcdba0631e0c",
 "driver_id":"$DRIVER",
 "start_point":{"latitude":59.9398,"longitude":30.3146},
 "end_point":{"latitude":59.9290,"longitude":30.3626},
 "price":1450}
EOF

seq 20 | xargs -P20 -I{} curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST localhost:8080/api/v1/trips \
  -H 'content-type: application/json' -d @/tmp/trip_race.json \
  | sort | uniq -c
```

Ожидаешь: `1×201 + 19×409`.

### Гонка finish — 2 параллельных

```bash
TRIP_ID=$(curl -sS -X POST localhost:8080/api/v1/trips \
  -H 'content-type: application/json' \
  -d '{"user_id":"5cb72c04-7650-45c9-a79b-bcdba0631e0c","driver_id":"'$(cat /proc/sys/kernel/random/uuid)'","start_point":{"latitude":1,"longitude":1},"end_point":{"latitude":2,"longitude":2},"price":100}' \
  | jq -r .id)

( curl -s -o /dev/null -w 'A:%{http_code}\n' -X POST localhost:8080/api/v1/trips/$TRIP_ID/finish & \
  curl -s -o /dev/null -w 'B:%{http_code}\n' -X POST localhost:8080/api/v1/trips/$TRIP_ID/finish & \
  wait )
```

Ожидаешь: один `200`, один `409`.

### Идемпотентность

```bash
KEY=$(cat /proc/sys/kernel/random/uuid)
DRIVER=$(cat /proc/sys/kernel/random/uuid)
BODY="{\"user_id\":\"5cb72c04-7650-45c9-a79b-bcdba0631e0c\",\"driver_id\":\"$DRIVER\",\"start_point\":{\"latitude\":1,\"longitude\":1},\"end_point\":{\"latitude\":2,\"longitude\":2},\"price\":100}"

# 201
curl -sS -X POST localhost:8080/api/v1/trips -H 'content-type: application/json' -H "Idempotency-Key: $KEY" -d "$BODY" | jq .id

# 200, тот же id
curl -sS -X POST localhost:8080/api/v1/trips -H 'content-type: application/json' -H "Idempotency-Key: $KEY" -d "$BODY" | jq .id
```

### `/ready` при недоступной БД

```bash
tripgoctl environment stop
curl -i -sS localhost:8080/ready    # 503
curl -i -sS localhost:8080/health   # 200
tripgoctl environment start
curl -i -sS localhost:8080/ready    # 200
```

## Откат миграций

```bash
make migrate-down   # откатит последнюю
make migrate-status # проверить
make migrate        # накатить заново
```