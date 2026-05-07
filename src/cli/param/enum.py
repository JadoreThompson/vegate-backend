import click

class EnumParam(click.ParamType):
    def __init__(self, enum_cls):
        self.enum_cls = enum_cls
        self.name = enum_cls.__name__

    def convert(self, value, param, ctx):
        try:
            return self.enum_cls(value)
        except ValueError:
            self.fail(f"Invalid value: {value}", param, ctx)
