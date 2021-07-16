from collections import defaultdict


class EventEmitter:
    """Simple implementation of an event system, used to implement the Observer
    pattern. Imitates https://pypi.org/project/pymitter/ .

    Examples
    --------
    >>> emitter = EventEmitter()                    # Create event emitter
    >>> @emmiter.handle("some_event")               # Register handler for some_event
    ... def hanlder(data):
    ...     do_stuff(data)
    >>> emmiter.emit("some_event", data=[1, 2, 3])  # Emit some_event with some data

    """

    def __init__(self) -> None:
        self._handlers = defaultdict(list)

    def emit(self, event_name, *args, **kwargs) -> None:
        for fn in self._handlers[event_name]:
            fn(*args, **kwargs)

    def handle(self, event_name, func=None):
        if func:
            self._handlers[event_name].append(func)
        else:

            def wrapper(fn):
                self._handlers[event_name].append(fn)
                return fn

            return wrapper
