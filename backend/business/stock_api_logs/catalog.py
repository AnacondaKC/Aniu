"""User-visible data-interface tools eligible for invocation logging."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from backend.business.settings.mx_interfaces import MX_INTERFACE_CATALOG
from backend.business.settings.public_stock_interfaces import (
    AGGREGATE_PUBLIC_STOCK_TOOL_NAMES,
    PUBLIC_STOCK_TOOL_CATALOG,
)

type StockApiToolSource = Literal["aggregate", "mx", "public"]

STOCK_API_TOOL_SOURCES: tuple[StockApiToolSource, ...] = (
    "public",
    "aggregate",
    "mx",
)


@dataclass(frozen=True, slots=True)
class StockApiToolDescriptor:
    """One catalog card and the direct Agent tools that implement it."""

    tool_source: StockApiToolSource
    tool_id: str
    tool_name: str
    agent_tool_names: tuple[str, ...]


def _direct_mx_tool_name(operation_id: str) -> str:
    return operation_id.partition(".")[0]


_PUBLIC_TOOL_DESCRIPTORS = tuple(
    StockApiToolDescriptor(
        tool_source=(
            "aggregate"
            if item.tool_name in AGGREGATE_PUBLIC_STOCK_TOOL_NAMES
            else "public"
        ),
        tool_id=item.tool_name,
        tool_name=item.name,
        agent_tool_names=(item.tool_name,),
    )
    for item in PUBLIC_STOCK_TOOL_CATALOG
)
_MX_TOOL_DESCRIPTORS = tuple(
    StockApiToolDescriptor(
        tool_source="mx",
        tool_id=item.interface_id,
        tool_name=item.name,
        agent_tool_names=tuple(
            dict.fromkeys(
                _direct_mx_tool_name(operation_id)
                for operation_id in item.operation_ids
            )
        ),
    )
    for item in MX_INTERFACE_CATALOG
)
STOCK_API_TOOL_CATALOG = (*_PUBLIC_TOOL_DESCRIPTORS, *_MX_TOOL_DESCRIPTORS)


def stock_api_tool_descriptor(
    agent_tool_name: str,
) -> StockApiToolDescriptor | None:
    return next(
        (
            item
            for item in STOCK_API_TOOL_CATALOG
            if agent_tool_name in item.agent_tool_names
        ),
        None,
    )


def stock_api_tool_descriptor_by_id(
    tool_source: StockApiToolSource,
    tool_id: str,
) -> StockApiToolDescriptor | None:
    return next(
        (
            item
            for item in STOCK_API_TOOL_CATALOG
            if item.tool_source == tool_source and item.tool_id == tool_id
        ),
        None,
    )


__all__ = [
    "STOCK_API_TOOL_CATALOG",
    "STOCK_API_TOOL_SOURCES",
    "StockApiToolDescriptor",
    "StockApiToolSource",
    "stock_api_tool_descriptor",
    "stock_api_tool_descriptor_by_id",
]
