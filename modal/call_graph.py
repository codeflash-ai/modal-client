# Copyright Modal Labs 2022
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

from modal_proto import api_pb2


class InputStatus(IntEnum):
    """Enum representing status of a function input."""

    PENDING = 0
    SUCCESS = api_pb2.GenericResult.GENERIC_STATUS_SUCCESS
    FAILURE = api_pb2.GenericResult.GENERIC_STATUS_FAILURE
    INIT_FAILURE = api_pb2.GenericResult.GENERIC_STATUS_INIT_FAILURE
    TERMINATED = api_pb2.GenericResult.GENERIC_STATUS_TERMINATED
    TIMEOUT = api_pb2.GenericResult.GENERIC_STATUS_TIMEOUT

    @classmethod
    def _missing_(cls, value):
        return cls.PENDING


@dataclass
class InputInfo:
    """Simple data structure storing information about a function input."""

    input_id: str
    function_call_id: str
    task_id: str
    status: InputStatus
    function_name: str
    module_name: str
    children: list["InputInfo"]


def _reconstruct_call_graph(ser_graph: api_pb2.FunctionGetCallGraphResponse) -> list[InputInfo]:
    function_calls_by_id: dict[str, api_pb2.FunctionCallCallGraphInfo] = {
        f.function_call_id: f for f in ser_graph.function_calls
    }
    inputs_by_id: dict[str, api_pb2.InputCallGraphInfo] = {i.input_id: i for i in ser_graph.inputs}

    input_info_by_id: dict[str, InputInfo] = {}
    result = []

    def _reconstruct(input_id: str) -> Optional[InputInfo]:
        if input_id in input_info_by_id:
            return input_info_by_id[input_id]

        # Input info can be missing, because input retention is limited.
        input = inputs_by_id.get(input_id)
        if input is None:
            return None

        function_call = function_calls_by_id[input.function_call_id]
        info = InputInfo(
            input_id,
            input.function_call_id,
            input.task_id,
            InputStatus(input.status),
            function_call.function_name,
            function_call.module_name,
            [],
        )
        input_info_by_id[input_id] = info

        parent_input_id = function_call.parent_input_id
        if parent_input_id:
            parent = _reconstruct(parent_input_id)
            if parent:
                parent.children.append(info)
        else:
            result.append(info)

        return info

    for input_id in inputs_by_id:
        _reconstruct(input_id)

    return result
