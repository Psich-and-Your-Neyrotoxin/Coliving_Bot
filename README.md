# Coliving Bot

Runtime-конфігурований Telegram-бот для колівінгу. Його можна налаштувати під інший дім без редагування коду: через `setup wizard`, runtime config і import/export повного instance bundle.

## Зміст

- [Що Вміє Бот](#що-вміє-бот)
- [Швидкий Старт](#швидкий-старт)
- [Запуск Будь-Де](#запуск-будь-де)
- [Режими Роботи](#режими-роботи)
- [Початкове Налаштування](#початкове-налаштування)
- [Зони І Ротації](#зони-і-ротації)
- [Import / Export](#import--export)
- [Тести І Команди](#тести-і-команди)
- [Структура Проєкту](#структура-проєкту)
- [Продовий Чекліст](#продовий-чекліст)
- [FAQ](#faq)

## Що Вміє Бот

- довільні runtime-зони, не тільки кухня/ванна/общак
- довільні цикли ротації
- команди будь-якого розміру: `1`, `2`, `3+`
- патерни типу `2,3,2`
- дедлайни, звіти, приватні нагадування, групові оголошення
- оплати й персональні папки
- штрафи й персональні продовження дедлайнів
- owner / deputies / гнучкі права
- календар винятків
- setup wizard
- import/export повного instance bundle в `JSON` і `YAML`

## Швидкий Старт

### 1. Клонування і залежності

```bash
git clone <your-repo>
cd coliving_bot
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Створи `.env`

```bash
cp .env.example .env
```

Заповни мінімум:

```env
TOKEN=your_bot_token
GROUP_ID=-1001234567890
ADMIN_ID=123456789
BOT_ENABLED=true
BOT_MODE=polling
DB_PATH=coliving.db
```

### 3. Запуск

```bash
python main.py
```

### 4. Перший вхід owner-а

Після запуску відкрий бота owner-акаунтом і натисни `/start`.

Якщо `setup` ще не завершений, бот покаже окремий onboarding-екран і не пустить у звичайні розділи, поки власник не завершить початкове налаштування.

### 5. Пройти wizard

Шлях у боті:

`⚙️ Керування -> 🛠 Система -> ⚙️ Runtime config -> 🧩 Setup wizard`

Після завершення wizard познач `setup complete`, і бот відкриє звичайний режим роботи.

## Запуск Будь-Де

Потрібно лише:

- Python `3.12+`
- доступ до інтернету для Telegram API
- можливість зберігати SQLite-файл

Тобто бот можна запустити:

- локально на ноуті
- на VPS
- у Docker
- на Railway
- на Render
- на будь-якому Linux-сервері
- хоч на “калькуляторі”, якщо на ньому є Python, мережа і файлова система

### Локально

```bash
python main.py
```

### У `tmux` / `screen`

```bash
tmux new -s coliving
python main.py
```

### Через Docker Compose

```bash
docker compose up --build -d
```

### На Railway / Render / PaaS

Використовуй `BOT_MODE=webhook`, задай публічний URL і стандартні env-змінні.

## Режими Роботи

Бот підтримує два режими запуску.

### Polling

Найпростіший режим для локального запуску, VPS або dev-середовища.

```env
BOT_MODE=polling
```

Після цього достатньо:

```bash
python main.py
```

### Webhook

Підходить для публічного хостингу й reverse proxy.

```env
BOT_MODE=webhook
WEBHOOK_HOST=0.0.0.0
WEBHOOK_PORT=8080
WEBHOOK_PATH=/webhook
WEBHOOK_BASE_URL=https://your-domain.example.com
```

У цьому режимі бот:

- піднімає HTTP-сервер
- реєструє webhook у Telegram
- слухає `WEBHOOK_PATH`

### Сумісність

Старий прапорець:

```env
WEBHOOK_ENABLED=1
```

ще працює як fallback, але для нових інстансів краще використовувати саме `BOT_MODE`.

### Тимчасово вимкнути бота

```env
BOT_ENABLED=false
```

## Початкове Налаштування

Перший запуск тепер працює як onboarding flow.

Поки `setup_complete = false`:

- owner бачить setup-екран з прогресом
- звичайні розділи приховані
- інші користувачі бачать, що бот ще налаштовується

### Що збирає setup wizard

1. назву coliving
2. timezone
3. group id
4. мешканців
5. зони, учасників і правила
6. модулі
7. завершення setup

### Формат мешканців у wizard

Надсилай построково:

```text
telegram_id | Повне ім'я | @username
123456789 | Іван Петренко | @ivan
987654321 | Олена Іваненко
```

Якщо username нема, його можна пропустити.

## Зони І Ротації

Кожна runtime-зона має:

- `code`
- назву
- `enabled`
- `report_required`
- `report_deadline_time`
- `private_reminder_time`
- `group_reminder_enabled`
- `private_reminder_enabled`
- `rotation_enabled`
- `rotation_every_days`
- `team_pattern`
- `member_order` або `member_groups`

Це дозволяє робити:

- 1 людину в зоні
- 2-3-4 людей у зоні
- патерни типу `2,3,2`
- цикл раз на 10 днів
- довільну кількість зон

### Покрокове створення нової зони

Через:

`⚙️ Керування -> 🛠 Система -> ⚙️ Runtime config -> 🗂 Зони -> ➕ Додати зону`

бот послідовно збирає:

- `code`
- назву
- `team pattern`
- `rotation_every_days`
- дедлайн звіту
- час приватного нагадування
- Telegram ID учасників

Після цього зона одразу готова до роботи.

## Import / Export

Бот уміє експортувати й імпортувати повний `instance bundle`.

У bundle входять:

- settings
- feature flags
- residents
- zones
- zone rules

### Формати

- JSON
- YAML

### Імпорт

Бот приймає:

- текстове повідомлення
- `.json`
- `.yaml`

### Де це в адмінці

`⚙️ Керування -> 🛠 Система -> ⚙️ Runtime config`

Там є:

- `📤 JSON`
- `📤 YAML`
- `📥 Імпорт bundle`

### Готовий шаблон

У репозиторії є:

- [instance.bundle.example.yaml](/home/yaro/Стільниця/coliving_bot/instance.bundle.example.yaml)

Це безпечний стартовий шаблон для нового owner-а.

## Тести І Команди

Це секція, яку зручно тримати відкритою прямо на GitHub.

### Швидка перевірка синтаксису

```bash
python -m py_compile main.py config.py database.py instance_config.py scheduler.py handlers/*.py middlewares/*.py
```

### Усі тести

```bash
python -m unittest discover -s tests -v
```

### Один файл тестів

```bash
python -m unittest tests.test_runtime_schedule -v
python -m unittest tests.test_scheduler -v
python -m unittest tests.test_deadline_policy -v
```

### Що зараз покривається тестами

- runtime config
- bundle import/export
- rotation engine
- runtime schedule
- scheduler deadlines
- permissions
- backup service
- legacy logic compatibility

### Що тестувати руками в Telegram

1. `/start` як owner до завершення setup
2. `Setup wizard`
3. створення зони
4. ручне нагадування
5. здачу звіту
6. approve / reject
7. export bundle
8. import bundle назад

## Структура Проєкту

- [main.py](/home/yaro/Стільниця/coliving_bot/main.py) — старт бота
- [config.py](/home/yaro/Стільниця/coliving_bot/config.py) — env і валідація режиму
- [database.py](/home/yaro/Стільниця/coliving_bot/database.py) — SQLite і таблиці
- [instance_config.py](/home/yaro/Стільниця/coliving_bot/instance_config.py) — runtime config / bundle
- [rotation_engine.py](/home/yaro/Стільниця/coliving_bot/rotation_engine.py) — engine ротацій
- [runtime_schedule.py](/home/yaro/Стільниця/coliving_bot/runtime_schedule.py) — runtime assignments
- [scheduler.py](/home/yaro/Стільниця/coliving_bot/scheduler.py) — нагадування, дедлайни, follow-up
- [handlers/admin.py](/home/yaro/Стільниця/coliving_bot/handlers/admin.py) — адмінка, setup, import/export
- [handlers/core.py](/home/yaro/Стільниця/coliving_bot/handlers/core.py) — головне меню і setup guard

## Продовий Чекліст

1. Заповнити `.env`
2. Перевірити `TOKEN`, `ADMIN_ID`, `GROUP_ID`
3. Обрати режим: `polling` або `webhook`
4. Запустити `python -m unittest discover -s tests -v`
5. Запустити бота
6. Пройти `Setup wizard`
7. Додати хоча б 2-3 мешканців
8. Створити хоча б одну runtime-зону
9. Перевірити ручне нагадування
10. Перевірити здачу звіту
11. Перевірити export/import bundle

Після цього бот уже можна віддавати іншому owner-у як готову основу.

## FAQ

### Де тепер живуть мешканці і зони?

У БД. `residents.json` лишився як bootstrap / legacy fallback / sample.

### Чи потрібно редагувати код для нового дому?

Ні. Базовий сценарій тепер проходиться через setup wizard або import bundle.

### Чи можна тримати кілька режимів запуску?

Так:

- `BOT_MODE=polling`
- `BOT_MODE=webhook`

### Чи можна перенести інстанс на інший сервер?

Так. Для цього є `instance bundle` в `JSON` або `YAML`.
