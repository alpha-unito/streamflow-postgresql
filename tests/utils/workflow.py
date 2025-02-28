from __future__ import annotations

import posixpath
from collections.abc import MutableSequence

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.core.scheduling import HardwareRequirement
from streamflow.core.workflow import Port, Workflow
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLScheduleStep
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import DeployStep, ScheduleStep

from tests.utils.deployment import get_docker_deployment_config

CWL_VERSION = "v1.2"


def create_deploy_step(workflow, deployment_config=None):
    connector_port = workflow.create_port(cls=ConnectorPort)
    if not deployment_config:
        deployment_config = get_docker_deployment_config()
    return workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )


def create_schedule_step(
    workflow: Workflow,
    deploy_steps: MutableSequence[DeployStep],
    binding_config: BindingConfig = None,
    hardware_requirement: HardwareRequirement = None,
):
    # It is necessary to pass in the correct order biding_config.targets and deploy_steps for the mapping
    if not binding_config:
        binding_config = BindingConfig(
            targets=[
                Target(
                    deployment=deploy_step.deployment_config,
                )
                for deploy_step in deploy_steps
            ]
        )
    return workflow.create_step(
        cls=(
            CWLScheduleStep
            if isinstance(hardware_requirement, CWLHardwareRequirement)
            else ScheduleStep
        ),
        name=posixpath.join(utils.random_name(), "__schedule__"),
        job_prefix="something",
        connector_ports={
            target.deployment.name: deploy_step.get_output_port()
            for target, deploy_step in zip(binding_config.targets, deploy_steps)
        },
        binding_config=binding_config,
        hardware_requirement=hardware_requirement,
    )


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
