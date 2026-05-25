from __future__ import annotations

import re


def normalize_parser_error(
    platform: str | None,
    value: str | None,
    reason: str | None,
) -> dict[str, str]:
    """Convert raw parser/API errors into operator-friendly UI messages."""
    platform_name = (platform or "unknown").strip().lower()
    account = (value or "").strip()
    raw_reason = (reason or "unknown error").strip()
    text = raw_reason.casefold()
    status = _extract_status(text)

    if _has_billing_error(text, status):
        service = "ScrapeCreators" if platform_name in {"youtube", "tiktok"} else "внешнего API"
        return _result(
            title=f"Закончились кредиты {service}",
            message=(
                f"{_platform_label(platform_name)} не смог получить данные по {account_label(account)}: "
                "внешний API вернул ошибку оплаты или закончились кредиты."
            ),
            action="Пополните баланс API и повторите запуск.",
            details=raw_reason,
            kind="billing",
        )

    if _has_auth_error(text, status):
        return _result(
            title="Нет доступа к внешнему API",
            message=(
                f"{_platform_label(platform_name)} не смог получить данные по {account_label(account)}: "
                "ключ API не принят или у него нет доступа."
            ),
            action="Проверьте API-ключи в переменных окружения.",
            details=raw_reason,
            kind="auth",
        )

    if _has_rate_limit(text, status):
        return _result(
            title="Сработал лимит внешнего API",
            message=(
                f"{_platform_label(platform_name)} временно не смог получить данные по {account_label(account)} "
                "из-за лимита запросов."
            ),
            action="Подождите несколько минут и повторите запуск.",
            details=raw_reason,
            kind="rate_limit",
        )

    if _has_not_found(text, status):
        return _not_found_result(platform_name, account, raw_reason)

    if "unable to resolve channel_id" in text:
        return _result(
            title="YouTube-канал не найден",
            message=(
                f"Не удалось определить channel_id для {account_label(account)}. "
                "Обычно это значит, что логин, ссылка или handle в таблице указан неверно."
            ),
            action="Проверьте YouTube-профиль в Google Sheets.",
            details=raw_reason,
            kind="not_found",
        )

    if "unable to resolve user_id" in text:
        return _result(
            title="TikTok-аккаунт не найден",
            message=(
                f"Не удалось определить user_id для {account_label(account)}. "
                "Обычно это значит, что логин в таблице указан неверно."
            ),
            action="Проверьте TikTok-профиль в Google Sheets.",
            details=raw_reason,
            kind="not_found",
        )

    if _has_timeout(text):
        return _result(
            title="Внешний API не ответил вовремя",
            message=(
                f"{_platform_label(platform_name)} не дождался ответа по {account_label(account)}. "
                "Это похоже на временный сбой сети или сервиса."
            ),
            action="Повторите запуск позже.",
            details=raw_reason,
            kind="temporary",
        )

    return _result(
        title="Ошибка парсинга",
        message=(
            f"{_platform_label(platform_name)} не смог обработать {account_label(account)}. "
            "Техническая причина сохранена ниже."
        ),
        action="Откройте технические детали и проверьте источник ошибки.",
        details=raw_reason,
        kind="unknown",
    )


def _result(title: str, message: str, action: str, details: str, kind: str) -> dict[str, str]:
    return {
        "title": title,
        "message": message,
        "action": action,
        "details": details,
        "kind": kind,
    }


def _not_found_result(platform: str, account: str, raw_reason: str) -> dict[str, str]:
    label = _platform_label(platform)
    if platform == "instagram":
        title = "Instagram-аккаунт не найден"
        action = "Проверьте логин Instagram в Google Sheets."
        hint = "аккаунт мог быть переименован, удален, закрыт или написан с ошибкой"
    elif platform == "youtube":
        title = "YouTube-канал не найден"
        action = "Проверьте YouTube-профиль в Google Sheets."
        hint = "канал мог быть переименован, удален или ссылка написана с ошибкой"
    elif platform == "tiktok":
        title = "TikTok-аккаунт не найден"
        action = "Проверьте TikTok-профиль в Google Sheets."
        hint = "аккаунт мог быть переименован, удален или логин написан с ошибкой"
    else:
        title = "Аккаунт не найден"
        action = "Проверьте аккаунт в Google Sheets."
        hint = "аккаунт мог быть переименован, удален или написан с ошибкой"

    return _result(
        title=title,
        message=f"{label} не нашел {account_label(account)}: {hint}.",
        action=action,
        details=raw_reason,
        kind="not_found",
    )


def _extract_status(text: str) -> int | None:
    patterns = (
        r"\bstatus[=: ]+(\d{3})\b",
        r"\bapi error (\d{3})\b",
        r"\berror (\d{3})\b",
        r"\b(\d{3}) not found\b",
        r"\bclient error '(\d{3})",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def _has_billing_error(text: str, status: int | None) -> bool:
    return status == 402 or any(
        marker in text
        for marker in (
            "out of credits",
            "buy more",
            "payment required",
            "insufficient credits",
            "недостаточно кредит",
            "закончились кредит",
        )
    )


def _has_auth_error(text: str, status: int | None) -> bool:
    return status in {401, 403} or any(
        marker in text
        for marker in (
            "invalid api key",
            "unauthorized",
            "forbidden",
            "not subscribed",
            "access denied",
        )
    )


def _has_rate_limit(text: str, status: int | None) -> bool:
    return status == 429 or "rate limit" in text or "too many requests" in text


def _has_not_found(text: str, status: int | None) -> bool:
    return status == 404 or any(
        marker in text
        for marker in (
            "not found",
            "accountdoesnotexist",
            "deleted",
            "удален",
            "не найден",
        )
    )


def _has_timeout(text: str) -> bool:
    return any(
        marker in text
        for marker in (
            "timeout",
            "timed out",
            "server disconnected",
            "cannot connect",
            "connection reset",
        )
    )


def _platform_label(platform: str) -> str:
    return {
        "instagram": "Instagram",
        "youtube": "YouTube",
        "tiktok": "TikTok",
    }.get(platform, "Парсер")


def account_label(value: str) -> str:
    if not value:
        return "аккаунт без имени"
    if value.startswith("@") or value.startswith("http"):
        return value
    return f"@{value}"
