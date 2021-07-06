import click as cl


@cl.group()
def entry():
    pass


@entry.command()
def init():
    pass


@entry.command()
def add():
    pass


@entry.command()
def validate():
    pass


@entry.command()
def delete():
    pass


@entry.command()
def enable():
    pass


@entry.command()
def disable():
    pass


@entry.command()
def tag():
    pass


@entry.command()
def list():
    pass
