from mipdb.event import EventEmitter


def test_register_handler():
    emitter = EventEmitter()

    def func(data):
        return sum(data)

    emitter.handle("an_event", func)
    assert emitter._handlers["an_event"] == [func]


def test_register_handler_decorator():
    emitter = EventEmitter()

    @emitter.handle("an_event")
    def func(data):
        return sum(data)

    assert emitter._handlers["an_event"] == [func]


def test_emit_event(capsys):
    emitter = EventEmitter()

    @emitter.handle("an_event")
    def func(*data):
        print(sum(data))

    emitter.emit("an_event", 1, 2, 3)
    captured = capsys.readouterr()
    assert "6" in captured.out


def test_emit_multiple_emitions(capsys):
    emitter = EventEmitter()

    @emitter.handle("an_event")
    def func(*data):
        print(sum(data))

    emitter.emit("an_event", 1, 2, 3)
    emitter.emit("an_event", 2, 4, 6)
    captured = capsys.readouterr()
    assert "6" in captured.out
    assert "12" in captured.out


def test_emit_multiple_events(capsys):
    emitter = EventEmitter()

    @emitter.handle("an_event")
    def func(*data):
        print(sum(data))

    @emitter.handle("another_event")
    def func(*data):
        print(-sum(data))

    emitter.emit("an_event", 1, 2, 3)
    emitter.emit("another_event", 1, 2, 3)
    captured = capsys.readouterr()
    assert "6" in captured.out
    assert "-6" in captured.out


def test_multiple_handlers(capsys):
    emitter = EventEmitter()

    @emitter.handle("an_event")
    def func(*data):
        print(sum(data))

    @emitter.handle("an_event")
    def func(*data):
        print(2 * sum(data))

    emitter.emit("an_event", 1, 2, 3)
    captured = capsys.readouterr()
    assert "6" in captured.out
    assert "12" in captured.out
