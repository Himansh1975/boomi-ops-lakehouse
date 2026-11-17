from boomi_ops.fingerprint import fingerprint, template


def test_same_error_different_values_same_fingerprint():
    a = 'Map shape failed at row 17: Unparseable date: "31-02-2026" for field PunchDate'
    b = 'Map shape failed at row 802: Unparseable date: "" for field PunchDate'
    assert fingerprint(a) == fingerprint(b)
    assert template(a) == "Map shape failed at row <n>: Unparseable date: <str> for field PunchDate"


def test_ids_emails_urls_are_masked():
    msg = ("run execution-107306ba-a299-46e8-954a-a9ec48f0f11f-2024.09.25 by ops@example.com "
           "calling https://api.example.com/v1/x?id=9 for 5c94d512-4dfe-404b-b32a-8063259436f7")
    assert template(msg) == "run <exec_id> by <email> calling <url> for <uuid>"


def test_http_status_and_port_are_kept():
    assert fingerprint("HTTP 401 Unauthorized") != fingerprint("HTTP 500 Unauthorized")
    assert "example.com:443" in template("Connection refused to host example.com:443")


def test_empty_message_has_no_fingerprint():
    assert fingerprint("") is None and fingerprint(None) is None
