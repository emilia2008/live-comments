"""Banned word filter: replaces whole banned words with '***', ignoring case."""

import re


class BannedWordFilter:
    """Compiles the banned word list once into a single regular expression."""

    def __init__(self, banned_words: list[str]):
        words = [re.escape(word) for word in banned_words if word]
        if words:
            # (?<!\w) and (?!\w) mean "not glued to another letter or digit",
            # so banning "ass" does not touch "class".
            self._pattern = re.compile(r"(?<!\w)(?:" + "|".join(words) + r")(?!\w)", re.IGNORECASE)
        else:
            self._pattern = None

    def clean(self, text: str) -> str:
        if self._pattern is None:
            return text
        return self._pattern.sub("***", text)
