"""Storage repository protocol seams for PostgreSQL readiness (RC.14 / 0.2.0).

This module defines stable abstract repository protocols that decouple Core domain
and API logic from SQLite-specific implementation details.

Live execution continues to use SQLiteCoreRepository wrapping CoreStore.
"""

from __future__ import annotations

from typing import Any, Iterable, Protocol, runtime_checkable

from .store import CoreStore


@runtime_checkable
class AccountRepositoryProtocol(Protocol):
    def account(self, account_id: str) -> dict[str, Any] | None: ...
    def list_accounts(self) -> list[dict[str, Any]]: ...
    def upsert_account(
        self,
        account_id: str,
        display_name: str,
        *,
        state: str,
        runtime: dict[str, Any] | None = None,
        sync: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...


@runtime_checkable
class MessageRepositoryProtocol(Protocol):
    def message(self, account_id: str, message_id: str) -> dict[str, Any] | None: ...
    def list_messages(
        self,
        account_id: str,
        chat_id: str,
        *,
        cursor: str = "",
        limit: int = 100,
        wechat_identity_uuid: str | None = None,
    ) -> dict[str, Any]: ...
    def identity_list_messages(
        self,
        wechat_identity_uuid: str,
        *,
        chat_id: str = "",
        cursor: str = "",
        limit: int = 100,
    ) -> dict[str, Any]: ...
    def upsert_message(self, account_id: str, message: dict[str, Any]) -> dict[str, Any]: ...


@runtime_checkable
class EventRepositoryProtocol(Protocol):
    def poll_events(self, *, after: str, limit: int, account_id: str = "") -> dict[str, Any]: ...
    def ack_events(self, consumer_id: str, event_ids: Iterable[str]) -> dict[str, Any]: ...
    def checkpoint_consumer(
        self,
        consumer_id: str,
        processed_through_cursor: int,
        *,
        last_event_id: str = "",
        subscription_account_id: str = "",
    ) -> dict[str, Any]: ...
    def get_checkpoint(self, consumer_id: str) -> dict[str, Any] | None: ...


@runtime_checkable
class CoreStorageRepositoryProtocol(
    AccountRepositoryProtocol,
    MessageRepositoryProtocol,
    EventRepositoryProtocol,
    Protocol,
):
    def storage_capabilities(self) -> dict[str, Any]: ...


class SQLiteCoreRepository:
    """SQLite implementation of CoreStorageRepositoryProtocol wrapping CoreStore."""

    def __init__(self, store: CoreStore) -> None:
        self.store = store

    def account(self, account_id: str) -> dict[str, Any] | None:
        return self.store.account(account_id)

    def list_accounts(self) -> list[dict[str, Any]]:
        return self.store.list_accounts()

    def upsert_account(
        self,
        account_id: str,
        display_name: str,
        *,
        state: str,
        runtime: dict[str, Any] | None = None,
        sync: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.store.upsert_account(
            account_id, display_name, state=state, runtime=runtime, sync=sync
        )

    def message(self, account_id: str, message_id: str) -> dict[str, Any] | None:
        return self.store.message(account_id, message_id)

    def list_messages(
        self,
        account_id: str,
        chat_id: str,
        *,
        cursor: str = "",
        limit: int = 100,
        wechat_identity_uuid: str | None = None,
    ) -> dict[str, Any]:
        return self.store.list_messages(
            account_id, chat_id, cursor=cursor, limit=limit, wechat_identity_uuid=wechat_identity_uuid
        )

    def identity_list_messages(
        self,
        wechat_identity_uuid: str,
        *,
        chat_id: str = "",
        cursor: str = "",
        limit: int = 100,
    ) -> dict[str, Any]:
        return self.store.identity_list_messages(
            wechat_identity_uuid, chat_id=chat_id, cursor=cursor, limit=limit
        )

    def upsert_message(self, account_id: str, message: dict[str, Any]) -> dict[str, Any]:
        return self.store.upsert_message(account_id, message)

    def poll_events(self, *, after: str, limit: int, account_id: str = "") -> dict[str, Any]:
        return self.store.poll_events(after=after, limit=limit, account_id=account_id)

    def ack_events(self, consumer_id: str, event_ids: Iterable[str]) -> dict[str, Any]:
        return self.store.ack_events(consumer_id, event_ids)

    def checkpoint_consumer(
        self,
        consumer_id: str,
        processed_through_cursor: int,
        *,
        last_event_id: str = "",
        subscription_account_id: str = "",
    ) -> dict[str, Any]:
        return self.store.checkpoint_consumer(
            consumer_id,
            processed_through_cursor,
            last_event_id=last_event_id,
            subscription_account_id=subscription_account_id,
        )

    def get_checkpoint(self, consumer_id: str) -> dict[str, Any] | None:
        return self.store.get_checkpoint(consumer_id)

    def storage_capabilities(self) -> dict[str, Any]:
        return {
            "backend": "sqlite",
            "durable_checkpoints": True,
            "transactional_compaction": True,
            "identity_v2_stamped": True,
            "postgres_ready": True,
            "version": "0.1.0-rc.14",
        }
