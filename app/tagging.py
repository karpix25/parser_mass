# Shared utilities for matching tags across platforms.
import re
from typing import Mapping, Sequence

TagRecord = Mapping[str, object]


def normalize_text(text: str | None) -> str:
    """
    Приводит текст к унифицированному виду для поиска тегов.
    """
    s = (text or "").strip()
    s = re.sub(r"[^a-zA-Z0-9а-яА-Я#]", " ", s)
    s = re.sub(r"\s+", " ", s).lower()
    return s


def match_tags(text: str | None, tags: Sequence[TagRecord]):
    """
    Универсальный подбор тегов/компаний/продуктов.
    Возвращает (client_tag, company, product, matched_tags_list).
    """
    matched_tags: list[str] = []
    matched_companies: list[str] = []
    matched_products: list[str] = []
    clean_text = normalize_text(text)

    for t in tags:
        tag_raw = t.get("tag") if isinstance(t, Mapping) else None
        tag = (tag_raw or "").strip().lower()
        if not tag:
            continue
        pattern = rf"#?[a-z0-9_-]*{re.escape(tag)}[a-z0-9_-]*"
        if re.search(pattern, clean_text, flags=re.IGNORECASE):
            matched_tags.append(f"#{tag}")
            company = t.get("company") if isinstance(t, Mapping) else None
            product = t.get("product") if isinstance(t, Mapping) else None
            if company:
                matched_companies.append(str(company))
            if product:
                matched_products.append(str(product))

    client_tag = ", ".join(matched_tags) if matched_tags else None
    company = matched_companies[0] if matched_companies else None
    product = matched_products[0] if matched_products else None
    return client_tag, company, product, matched_tags
