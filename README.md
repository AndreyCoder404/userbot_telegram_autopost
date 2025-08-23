# Telegram Realty Bot

Автоматический бот для репоста объявлений о недвижимости из Telegram-канала в группы.

## Установка
1. Установите зависимости: `pip install telethon apscheduler pytz`
2. Создайте config.ini с вашими API-ключами (см. пример в create_default_config).
3. Запустите: `python telegram_realty_bot.py`

## Функции
- Репост постов за последние N дней.
- Группировка альбомов (фото/видео).
- Планировщик на 4-6 раз в день.
- Логирование и статистика.

## Конфигурация
Редактируйте config.ini: api_id, api_hash, source_channel, groups и т.д.

## Безопасность
Не коммитьте config.ini или сессионные файлы!
