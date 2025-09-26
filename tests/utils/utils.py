from __future__ import annotations

import inspect
from typing import Any

import pytest


def get_full_instantiation(cls_: type[Any], **arguments) -> Any:
    """
    Instantiates a class using the provided arguments, checking whether the resulting
    instance has values that differ from the class's default values.

    All class arguments are mandatory in this instantiation. Moreover, values for parameters that have
    default values must be explicitly provided and must differ from those defaults.

    :param cls_: The class to instantiate.
    :param arguments: A mapping of argument names to values used for instantiation.
    :returns: An instance of the class, created using the provided arguments.
    """
    sig = inspect.signature(cls_.__init__)

    if new_args := {k for k in sig.parameters.keys() if k != "self"} - set(
        arguments.keys()
    ):
        pytest.fail(f"Object instantiation failed due to missing arguments: {new_args}")
    new_args = []
    for name, param in sig.parameters.items():
        if (
            name != "self"
            and param.default != inspect.Parameter.empty
            and arguments[name] == param.default
        ):
            new_args.append(name)
    if new_args:
        pytest.fail(
            f"Cannot create the object because default values were used for the following arguments: {new_args}"
        )
    return cls_(**arguments)
