from __future__ import annotations

from typing import TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.workflow import Port, Workflow
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.step import CombinatorStep

from tests.utils.utils import get_full_instantiation

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext

CWL_VERSION = "v1.2"


async def create_workflow(
    context: StreamFlowContext,
    num_port: int = 2,
    type_: str = "cwl",
    save: bool = True,
) -> tuple[Workflow, tuple[Port, ...]]:
    if type_ == "cwl":
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
    if save:
        await workflow.save(context)
    return workflow, tuple(ports)


def get_cartesian_product_combinator(
    workflow: Workflow, name: str | None = None
) -> CartesianProductCombinator:
    return get_full_instantiation(
        cls_=CartesianProductCombinator,
        name=name or utils.random_name(),
        workflow=workflow,
        depth=2,
    )


def get_combinator_step(
    workflow: Workflow, combinator_type: str, inner_combinator: bool = False
) -> CombinatorStep:
    combinator_step_cls = CombinatorStep
    name = utils.random_name()
    match combinator_type:
        case "cartesian_product_combinator":
            combinator = get_cartesian_product_combinator(workflow, name)
        case "dot_combinator":
            combinator = get_dot_combinator(workflow, name)
        case "loop_combinator":
            combinator = get_loop_combinator(workflow, name)
        case "loop_termination_combinator":
            combinator = get_loop_terminator_combinator(workflow, name)
        case "nested_crossproduct":
            combinator = get_nested_crossproduct(workflow, name)
        case _:
            raise ValueError(
                f"Invalid input combinator type: {combinator_type} is not supported"
            )
    if inner_combinator:
        if combinator_type == "nested_crossproduct":
            raise ValueError("Nested crossproduct already has inner combinators")
        combinator.add_combinator(get_dot_combinator(workflow, name), {"test_name_1"})
        combinator.add_combinator(
            get_cartesian_product_combinator(workflow, name), {"test_name_2"}
        )
    step = get_full_instantiation(
        cls_=combinator_step_cls,
        name=name + "-combinator",
        combinator=combinator,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    return step


def get_dot_combinator(
    workflow: Workflow, name: str | None = None
) -> DotProductCombinator:
    return get_full_instantiation(
        cls_=DotProductCombinator,
        name=name or utils.random_name(),
        workflow=workflow,
    )


def get_loop_combinator(workflow: Workflow, name: str | None = None) -> LoopCombinator:
    return get_full_instantiation(
        cls_=LoopCombinator, name=name or utils.random_name(), workflow=workflow
    )


def get_loop_terminator_combinator(
    workflow: Workflow, name: str | None = None
) -> LoopTerminationCombinator:
    c = get_full_instantiation(
        cls_=LoopTerminationCombinator,
        name=name or utils.random_name(),
        workflow=workflow,
    )
    c.add_output_item("test1")
    c.add_output_item("test2")
    return c


def get_nested_crossproduct(
    workflow: Workflow, name: str | None = None
) -> DotProductCombinator:
    combinator = get_full_instantiation(
        cls_=DotProductCombinator, name=name or utils.random_name(), workflow=workflow
    )
    c1 = get_full_instantiation(
        cls_=CartesianProductCombinator,
        name=name or utils.random_name(),
        workflow=workflow,
        depth=2,
    )
    c1.add_item("ext")
    c1.add_item("inn")
    items = c1.get_items(False)
    combinator.add_combinator(c1, items)
    return combinator
