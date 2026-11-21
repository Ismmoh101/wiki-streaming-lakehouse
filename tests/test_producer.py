import json

from producer.producer import Stats, parse_sse, publish, to_record


def sse(*events):
    out = []
    for eid, data in events:
        out += [f"id: {eid}", "event: message", f"data: {data}", ""]
    return out


def event(etype="edit", domain="en.wikipedia.org", eid="abc", wiki="enwiki"):
    return json.dumps({"type": etype, "wiki": wiki, "meta": {"id": eid, "domain": domain}})


def test_parse_sse_basic_and_comments():
    lines = [":ok", ""] + sse(("1", "a"), ("2", "b"))
    assert list(parse_sse(lines)) == [("1", "a"), ("2", "b")]


def test_parse_sse_multiline_data_and_trailing_event():
    lines = ["id: 9", "data: line1", "data: line2"]
    assert list(parse_sse(lines)) == [("9", "line1\nline2")]


def test_to_record_filters():
    assert to_record(event())[0] == b"enwiki"
    assert to_record(event(etype="log")) is None
    assert to_record(event(domain="canary")) is None
    assert to_record("{not json") is None
    assert to_record(json.dumps({"type": "edit", "meta": {}})) is None


class FakeProducer:
    def __init__(self, full_first=False):
        self.sent, self.full_first, self.polls = [], full_first, 0

    def produce(self, topic, key, value, on_delivery):
        if self.full_first:
            self.full_first = False
            raise BufferError
        self.sent.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, _t):
        self.polls += 1


def test_publish_counts_and_resume_id():
    stream = parse_sse(sse(("1", event()), ("2", event(etype="log")), ("3", event(eid="x"))))
    prod, stats = FakeProducer(full_first=True), Stats()
    last = publish(stream, prod, "t", stats)
    assert last == "3"
    assert (stats.seen, stats.sent, stats.skipped, stats.failed) == (3, 2, 1, 0)
    assert all(t == "t" for t, _, _ in prod.sent)
