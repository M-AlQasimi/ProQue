"""Shared text safety helpers for Discord message and embed limits."""

ZERO_WIDTH_SPACE = "\u200b"


def fit_discord_content(content, limit=2000, suffix="\n\n[message shortened]"):
    text = str(content or "")
    if len(text) <= limit:
        return text
    suffix = str(suffix or "")
    if len(suffix) >= limit:
        return text[: max(0, limit - 1)] + "…"
    return text[: limit - len(suffix)].rstrip() + suffix


def embed_value(text, limit=1024, empty="None."):
    value = str(text if text is not None else empty)
    if not value:
        value = str(empty)
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


def joined_embed_value(lines, empty="None.", limit=1024):
    clean_lines = [str(line) for line in lines if str(line)]
    if not clean_lines:
        return empty
    return embed_value("\n".join(clean_lines), limit=limit, empty=empty)


def fit_embed_value(value, limit=1024):
    text = str(value or "")
    if not text:
        return ZERO_WIDTH_SPACE
    if len(text) <= limit:
        return text
    suffix = "\n[shortened]"
    if len(suffix) >= limit:
        return text[: max(0, limit - 1)] + "…"
    return text[: limit - len(suffix)].rstrip() + suffix
