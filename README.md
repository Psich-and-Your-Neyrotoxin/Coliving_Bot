# Coliving Bot

Telegram-бот для колівінгу: чергування, нагадування, фото-звіти, дедлайни, оплати, штрафи, обміни, права для заступників і бекапи.

## Як запустити на сервері

Нижче один нормальний сценарій для чистого Linux-сервера.

### 1. Встанови потрібне

Для Ubuntu/Debian:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip
```

Перевір:

```bash
python3 --version
git --version
```

### 2. Скачай код

```bash
git clone https://github.com/Psich-and-Your-Neyrotoxin/Coliving_Bot.git
cd Coliving_Bot
```

### 3. Створи віртуальне середовище

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Встанови залежності

```bash
pip install -r requirements.txt
```

### 5. Створи файл налаштувань

```bash
cp .env.example .env
```

### 6. Відредагуй `.env`

Мінімально впиши це:

```env
TOKEN=тут_токен_бота
GROUP_ID=-1001234567890
ADMIN_ID=123456789
BOT_ENABLED=true
BOT_MODE=polling
```

Що це означає:

- `TOKEN` — токен бота від `@BotFather`
- `GROUP_ID` — ID Telegram-групи
- `ADMIN_ID` — твій Telegram ID
- `BOT_ENABLED=true` — бот увімкнений
- `BOT_MODE=polling` — найпростіший режим запуску

### 7. Запусти бота

```bash
python main.py
```

Якщо все добре, бот стартує і в консолі підуть логи.

## Що робити після запуску

1. Відкрий бота в Telegram
2. Напиши `/start`
3. Відкрий:

`⚙️ Керування -> 🛠 Система -> ⚙️ Runtime config -> 🧩 Setup wizard`

4. Пройди кроки:
- назва колівінгу
- часовий пояс
- ID групи
- мешканці
- зони
- модулі
- завершення setup

Після цього бот уже готовий до роботи.

## Якщо хочеш запуск у фоні

### Варіант через `tmux`

```bash
sudo apt install -y tmux
tmux new -s coliving
cd Coliving_Bot
source .venv/bin/activate
python main.py
```

Вийти з `tmux`, не зупиняючи бота:

```bash
Ctrl+B, потім D
```

Повернутись:

```bash
tmux attach -t coliving
```

## Якщо хочеш Docker

```bash
cp .env.example .env
docker compose up --build -d
```

## Режими запуску

### Звичайний режим

Найпростіше:

```env
BOT_MODE=polling
```

### Webhook

Якщо є домен і публічний сервер:

```env
BOT_MODE=webhook
WEBHOOK_BASE_URL=https://your-domain.example.com
WEBHOOK_HOST=0.0.0.0
WEBHOOK_PORT=8080
WEBHOOK_PATH=/webhook
```

## Що вміє бот

Для мешканців:

- дивитись, хто за що чергує
- отримувати нагадування
- здавати фото-звіти
- дивитись штрафи
- робити обміни
- відкривати папку для оплати

Для owner-а:

- створювати зони
- задавати правила ротації
- вмикати або вимикати модулі
- переглядати й модерати звіти
- змінювати графік вручну
- керувати заступниками
- робити бекапи
- імпортувати й експортувати повний конфіг

## Де зберігаються дані

- налаштування запуску: у `.env`
- основні дані бота: у SQLite базі
- мешканці, зони, правила після setup: теж у базі

`residents.json` тут лишається як стартовий шаблон.

## Бекапи

Бекапи можна зберігати:

- тільки локально на сервері
- тільки адміну в Telegram
- і локально, і адміну

Через `.env`:

```env
BACKUP_ENABLED=1
BACKUP_DESTINATION=both
BACKUP_LOCAL_DIR=backups
```

Або прямо в боті:

`⚙️ Керування -> 🛠 Система -> ⚙️ Бекапи`

## Як перевірити, що все працює

### Автотести

```bash
python -m unittest discover -s tests -v
```

### Ручна перевірка

1. бот запускається без traceback
2. owner бачить `/start`
3. setup wizard відкривається
4. можна додати мешканців
5. можна створити зону
6. можна відкрити адмінку

## Якщо не запускається

Перевір:

1. чи правильний `TOKEN`
2. чи правильний `ADMIN_ID`
3. чи правильний `GROUP_ID`
4. чи встановились залежності
5. чи активоване `.venv`

Потім ще раз:

```bash
python main.py
```
