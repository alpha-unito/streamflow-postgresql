import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow
from streamflow.workflow.port import JobPort
from streamflow.workflow.step import ExecuteStep


@pytest.mark.asyncio
async def test_get_steps_queries(context: StreamFlowContext):
    """Test get_input_steps and get_output_steps queries"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    ports = [workflow.create_port() for _ in range(3)]
    job_ports = [workflow.create_port(JobPort) for _ in range(2)]
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=job_ports[0]
    )
    step_2 = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=job_ports[1]
    )
    step.add_input_port("in", ports[0])
    step.add_output_port("out", ports[1])
    step_2.add_input_port("in2", ports[1])
    step_2.add_output_port("out2", ports[2])
    await workflow.save(context)

    input_steps_port_a = await context.database.get_input_steps(ports[0].persistent_id)
    assert len(input_steps_port_a) == 0
    output_steps_port_a = await context.database.get_output_steps(
        ports[0].persistent_id
    )
    assert len(output_steps_port_a) == 1
    assert output_steps_port_a[0]["step"] == step.persistent_id

    input_steps_port_b = await context.database.get_input_steps(ports[1].persistent_id)
    assert len(input_steps_port_b) == 1
    assert input_steps_port_b[0]["step"] == step.persistent_id
    output_steps_port_b = await context.database.get_output_steps(
        ports[1].persistent_id
    )
    assert len(output_steps_port_b) == 1
    assert output_steps_port_b[0]["step"] == step_2.persistent_id

    input_steps_port_c = await context.database.get_input_steps(ports[2].persistent_id)
    assert len(input_steps_port_c) == 1
    assert input_steps_port_c[0]["step"] == step_2.persistent_id
    output_steps_port_c = await context.database.get_output_steps(
        ports[2].persistent_id
    )
    assert len(output_steps_port_c) == 0
