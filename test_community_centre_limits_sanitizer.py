"""Unit tests for community_centre_limits.sanitize_telegram_html — the
server-side, authoritative Telegram-HTML allowlist sanitizer that every
Community Centre rich-text editor submission is normalized/validated
through, regardless of what the frontend sends.

Run with: pytest test_community_centre_limits_sanitizer.py
"""

import community_centre_limits as limits


def _san(raw):
    html, err = limits.sanitize_telegram_html(raw)
    assert err is None, f"unexpected error {err!r} for input {raw!r}"
    return html


def test_bold_passthrough():
    assert _san("<b>bold</b>") == "<b>bold</b>"


def test_tag_aliases_normalized_to_canonical_short_names():
    assert _san("<strong>bold</strong>") == "<b>bold</b>"
    assert _san("<em>italic</em>") == "<i>italic</i>"
    assert _san("<ins>under</ins>") == "<u>under</u>"
    assert _san("<strike>strike1</strike>") == "<s>strike1</s>"
    assert _san("<del>strike2</del>") == "<s>strike2</s>"


def test_inline_code_and_blockquote_passthrough():
    assert _san("<code>x = 1</code>") == "<code>x = 1</code>"
    assert _san("<blockquote>quoted</blockquote>") == "<blockquote>quoted</blockquote>"


def test_multiline_blockquote_preserved():
    raw = "<blockquote>line one\nline two</blockquote>"
    assert _san(raw) == raw


def test_nested_blockquote_flattened_not_nested():
    html = _san("<blockquote>outer <blockquote>inner</blockquote> tail</blockquote>")
    assert html == "<blockquote>outer inner tail</blockquote>"
    assert html.count("<blockquote") == 1


def test_link_https_kept():
    assert _san('<a href="https://example.com">link</a>') == '<a href="https://example.com">link</a>'


def test_link_tg_scheme_kept():
    assert _san('<a href="tg://resolve?domain=x">link</a>') == '<a href="tg://resolve?domain=x">link</a>'


def test_link_javascript_scheme_stripped():
    html = _san('<a href="javascript:alert(1)">link</a>')
    assert "javascript:" not in html
    assert "<a>link</a>" == html


def test_link_data_scheme_stripped():
    html = _san('<a href="data:text/html,evil">link</a>')
    assert "data:" not in html


def test_spoiler_span_kept():
    html = _san('<span class="tg-spoiler">hidden</span>')
    assert html == '<span class="tg-spoiler">hidden</span>'


def test_arbitrary_span_class_stripped():
    html = _san('<span class="evil-class">x</span>')
    assert "evil-class" not in html
    assert html == "x"


def test_span_class_stripped_does_not_break_sibling_spoiler():
    html = _san('<span class="junk">plain</span><span class="tg-spoiler">hidden</span>')
    assert html == 'plain<span class="tg-spoiler">hidden</span>'


def test_script_tag_stripped_content_kept():
    html = _san("<script>alert(1)</script>hello<b>bold</b>")
    assert "<script" not in html
    assert "alert(1)hello<b>bold</b>" == html


def test_style_and_img_and_iframe_stripped():
    html = _san('<style>.x{}</style><img src="x.png"><iframe src="evil"></iframe>visible')
    assert "<style" not in html and "<img" not in html and "<iframe" not in html
    assert "visible" in html


def test_arbitrary_html_not_supported():
    html = _san('<div class="foo"><table><tr><td>x</td></tr></table></div>')
    assert "<div" not in html and "<table" not in html
    assert "x" in html


def test_plain_text_unaffected():
    assert _san("Big reward tonight") == "Big reward tonight"


def test_bold_portion_of_sentence():
    assert _san("<b>Big reward</b> tonight") == "<b>Big reward</b> tonight"


def test_control_characters_rejected():
    html, err = limits.sanitize_telegram_html("bad\x07text")
    assert err == "control_characters"
    assert html == ""


def test_none_input_returns_empty():
    html, err = limits.sanitize_telegram_html(None)
    assert html == "" and err is None
