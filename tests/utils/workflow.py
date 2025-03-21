from __future__ import annotations

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Port, Workflow
from streamflow.cwl.workflow import CWLWorkflow

CWL_VERSION = "v1.2"


async def create_workflow(
    context: StreamFlowContext,
    num_port: int = 2,
    type: str = "cwl",
) -> tuple[Workflow, tuple[Port, ...]]:
    if type == "cwl":
        workflow = CWLWorkflow(
            context=context,
            name=utils.random_name(),
            config={},
            cwl_version=CWL_VERSION,
        )
    else:
        workflow = Workflow(context=context, name=utils.random_name(), config={})
    ports = []
    for _ in range(num_port):
        ports.append(workflow.create_port())
    await workflow.save(context)
    return workflow, tuple(ports)
