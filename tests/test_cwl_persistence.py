from __future__ import annotations

from typing import Any

import pytest
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow
from streamflow.cwl.command import CWLCommand, CWLCommandToken
from streamflow.workflow.step import ExecuteStep
from tests.conftest import save_load_and_test


def _create_cwl_command_token(
    cls: type[CWLCommandToken] = CWLCommandToken, value: Any | None = None
):
    return cls(
        is_shell_command=False,
        item_separator="&",
        name="test",
        position=2,
        prefix="--test",
        separate=True,
        shell_quote=True,
        token_type="string",
        value=value,
    )


@pytest.mark.asyncio
async def test_cwl_command_token(context: StreamFlowContext):
    """Test saving and loading CWLCommand with CWLCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    step.command = CWLCommand(
        step=step,
        command_tokens=[
            _create_cwl_command_token(value=DoubleQuotedScalarString("60")),
            _create_cwl_command_token(
                value=LiteralScalarString("${ return 10 + 20 - (5 * 4) }")
            ),
        ],
    )
    await save_load_and_test(step, context)
