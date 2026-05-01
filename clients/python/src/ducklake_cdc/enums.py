"""Typed string enums for the ducklake-cdc SQL surface."""

from __future__ import annotations

from enum import StrEnum


class ScopeKind(StrEnum):
    CATALOG = "catalog"
    SCHEMA = "schema"
    TABLE = "table"


class EventCategory(StrEnum):
    ALL = "*"
    DML = "dml"
    DDL = "ddl"


class ChangeType(StrEnum):
    ALL = "*"
    INSERT = "insert"
    UPDATE_PREIMAGE = "update_preimage"
    UPDATE_POSTIMAGE = "update_postimage"
    DELETE = "delete"


class SubscriptionStatus(StrEnum):
    ACTIVE = "active"
    RENAMED = "renamed"
    DROPPED = "dropped"


class DdlEventKind(StrEnum):
    CREATED = "created"
    ALTERED = "altered"
    DROPPED = "dropped"


class DdlObjectKind(StrEnum):
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"


class DiagnosticSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
