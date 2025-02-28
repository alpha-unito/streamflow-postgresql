from __future__ import annotations

from collections.abc import MutableMapping, MutableSequence
from typing import Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Port, Step, Token, Workflow
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.persistence.utils import load_dependee_tokens, load_depender_tokens
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import TerminationToken


def _contains_id(id_: int, token_list: MutableSequence[Token]) -> bool:
    for t in token_list:
        if id_ == t.persistent_id:
            return True
    return False


async def general_test(
    context: StreamFlowContext,
    workflow: Workflow,
    in_port: Port,
    out_port: Port,
    step_cls: type[Step],
    kwargs_step: MutableMapping[str, Any],
    token_list: MutableSequence[Token],
    port_name: str = "test",
    save_input_token: bool = True,
) -> Step:
    step = workflow.create_step(cls=step_cls, **kwargs_step)
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    await put_tokens(token_list, in_port, context, save_input_token)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    return step


async def put_tokens(
    token_list: MutableSequence[Token],
    in_port: Port,
    context: StreamFlowContext,
    save_input_token: bool = True,
) -> None:
    for t in token_list:
        if save_input_token and not isinstance(t, TerminationToken):
            await t.save(context, in_port.persistent_id)
        in_port.put(t)
    in_port.put(TerminationToken())


async def verify_dependency_tokens(
    token: Token,
    port: Port,
    context: StreamFlowContext,
    expected_depender: MutableSequence[Token] | None = None,
    expected_dependee: MutableSequence[Token] | None = None,
    alternative_expected_dependee: MutableSequence[Token] | None = None,
):
    loading_context = DefaultDatabaseLoadingContext()
    expected_depender = expected_depender or []
    expected_dependee = expected_dependee or []

    token_reloaded = await context.database.get_token(token_id=token.persistent_id)
    assert token_reloaded["port"] == port.persistent_id

    depender_list = await load_depender_tokens(
        token.persistent_id, context, loading_context
    )
    print(
        "depender:",
        {token.persistent_id: [t.persistent_id for t in depender_list]},
    )
    print(
        "expected_depender",
        [t.persistent_id for t in expected_depender],
    )
    assert len(depender_list) == len(expected_depender)
    for t1 in depender_list:
        assert _contains_id(t1.persistent_id, expected_depender)

    dependee_list = await load_dependee_tokens(
        token.persistent_id, context, loading_context
    )
    print(
        "dependee:",
        {token.persistent_id: [t.persistent_id for t in dependee_list]},
    )
    print(
        "expected_dependee",
        [t.persistent_id for t in expected_dependee],
    )
    try:
        assert len(dependee_list) == len(expected_dependee)
        for t1 in dependee_list:
            assert _contains_id(t1.persistent_id, expected_dependee)
    except AssertionError as err:
        if alternative_expected_dependee is None:
            raise err
        else:
            assert len(dependee_list) == len(alternative_expected_dependee)
            for t1 in dependee_list:
                assert _contains_id(t1.persistent_id, alternative_expected_dependee)
