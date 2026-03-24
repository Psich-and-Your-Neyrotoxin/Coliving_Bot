# Coliving Bot

Простий Telegram-бот для колівінгу. Його можна запустити без редагування коду: вставив токен, запустив, пройшов налаштування в самому боті.

## Найпростіший запуск

### 1. Скачай проєкт

```bash
git clone <your-repo>
cd coliving_bot
```

### 2. Встанови Python-залежності

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Створи `.env`

```bash
cp .env.example .env
```

### 4. Впиши у `.env` тільки це

```env
TOKEN=тут_токен_бота
GROUP_ID=-1001234567890
ADMIN_ID=123456789
BOT_ENABLED=true
BOT_MODE=polling
```

### 5. Запусти

```bash
python main.py
```

### 6. Відкрий бота в Telegram

Напиши боту `/start`.

Якщо це перший запуск, бот сам покаже екран початкового налаштування.

## Що робити далі в боті

Зайди:

`⚙️ Керування -> 🛠 Система -> ⚙️ Runtime config -> 🧩 Setup wizard`

І пройди кроки:

1. назва колівінгу
2. часовий пояс
3. ID групи
4. мешканці
5. зони
6. модулі
7. завершити setup

Після цього бот уже працює як звичайно.

## Якщо зовсім коротко

Ось увесь мінімум:

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

## Що вміє бот

- графік чергувань
- звіти з фото
- дедлайни
- нагадування в особисті повідомлення
- групові оголошення
- штрафи
- обміни
- оплати
- заступники з правами
- import/export налаштувань

## Де зберігаються дані

- основні дані бота: у SQLite базі
- налаштування запуску: у `.env`
- мешканці й зони після налаштування: теж у базі

`residents.json` лишився як стартовий шаблон, а не як головне джерело істини.

## Бекапи

Можна вибрати, куди зберігати бекапи.

Це можна налаштувати:

- через `.env`
- або прямо в боті: `⚙️ Керування -> 🛠 Система -> ⚙️ Бекапи`

### Тільки на сервері

```env
BACKUP_ENABLED=1
BACKUP_DESTINATION=local
BACKUP_LOCAL_DIR=backups
```

### Тільки адміну в Telegram

```env
BACKUP_ENABLED=1
BACKUP_DESTINATION=admin
```

### І на сервері, і адміну

```env
BACKUP_ENABLED=1
BACKUP_DESTINATION=both
BACKUP_LOCAL_DIR=backups
```

Це найзручніший варіант.

## Два режими запуску

### Звичайний режим

Найпростіший варіант:

```env
BOT_MODE=polling
```

Потім:

```bash
python main.py
```

### Webhook режим

Якщо у тебе є сервер з доменом:

```env
BOT_MODE=webhook
WEBHOOK_BASE_URL=https://your-domain.example.com
WEBHOOK_HOST=0.0.0.0
WEBHOOK_PORT=8080
WEBHOOK_PATH=/webhook
```

## Запустити можна будь-де

Підійде:

- ноутбук
- домашній сервер
- VPS
- Docker
- будь-який Linux-сервер
- будь-який хостинг, де є Python і доступ у мережу

Головне, щоб були:

- Python `3.12+`
- інтернет
- можливість зберігати файл бази

## Як перевірити, що все працює

### Швидка перевірка

```bash
python -m unittest discover -s tests -v
```

### Перевірка вручну

1. запусти бота
2. owner пише `/start`
3. проходиш `Setup wizard`
4. додаєш 1-2 мешканців
5. додаєш 1 зону
6. перевіряєш нагадування
7. перевіряєш здачу звіту

## Корисні файли

- [main.py](/home/yaro/Стільниця/coliving_bot/main.py) — запуск бота
- [config.py](/home/yaro/Стільниця/coliving_bot/config.py) — env-змінні
- [database.py](/home/yaro/Стільниця/coliving_bot/database.py) — база
- [instance_config.py](/home/yaro/Стільниця/coliving_bot/instance_config.py) — runtime-конфіг
- [scheduler.py](/home/yaro/Стільниця/coliving_bot/scheduler.py) — нагадування і дедлайни
- [handlers/admin.py](/home/yaro/Стільниця/coliving_bot/handlers/admin.py) — адмінка

## Якщо щось не запускається

Перевір:

1. чи правильний `TOKEN`
2. чи правильний `ADMIN_ID`
3. чи правильний `GROUP_ID`
4. чи є інтернет
5. чи встановились залежності

І ще раз запусти:

```bash
python main.py
```
