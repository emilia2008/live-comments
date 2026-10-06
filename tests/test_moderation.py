from app.moderation import BannedWordFilter


def test_replaces_banned_word():
    word_filter = BannedWordFilter(["scam"])
    assert word_filter.clean("this is a scam") == "this is a ***"


def test_ignores_case():
    word_filter = BannedWordFilter(["scam"])
    assert word_filter.clean("SCAM and Scam and sCaM") == "*** and *** and ***"


def test_does_not_touch_words_that_contain_a_banned_word():
    word_filter = BannedWordFilter(["ass"])
    assert word_filter.clean("first class assignment") == "first class assignment"


def test_replaces_word_next_to_punctuation():
    word_filter = BannedWordFilter(["idiot"])
    assert word_filter.clean("idiot! (idiot) idiot,idiot") == "***! (***) ***,***"


def test_replaces_several_different_words():
    word_filter = BannedWordFilter(["idiot", "stupid"])
    assert word_filter.clean("stupid idiot") == "*** ***"


def test_banned_words_with_regex_characters_are_matched_literally():
    word_filter = BannedWordFilter(["a.b"])
    assert word_filter.clean("a.b axb") == "*** axb"


def test_empty_list_keeps_text_unchanged():
    word_filter = BannedWordFilter([])
    assert word_filter.clean("anything goes") == "anything goes"
