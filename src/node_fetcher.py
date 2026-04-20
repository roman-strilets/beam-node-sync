"""Block and header fetcher wrapping a single Beam node connection."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass

from beam_p2p import (
    BeamConnection,
    MessageType,
    encode_get_body_pack_payload,
    encode_height_range,
    format_address,
    message_name,
)
from beam_p2p.deserializers import (
    deserialize_body_pack_payloads,
    deserialize_body_payload,
    deserialize_header_pack_payloads,
)
from beam_p2p.protocol_models import BlockHeader, DecodedBlock


@dataclass(frozen=True)
class BodyFetchPlan:
    """Describe one staged body-fetch phase."""

    start_height: int
    stop_height: int
    flag_perishable: int
    flag_eternal: int
    block0: int
    horizon_lo1: int
    horizon_hi1: int


BODY_FLAG_FULL = 0
BODY_FLAG_NONE = 1
BODY_FLAG_RECOVERY1 = 2


class NodeBlockFetcher:
    """Request block headers and body payloads from a connected Beam node."""

    def __init__(
        self,
        connection: BeamConnection,
        *,
        request_timeout: float,
        verbose: bool,
    ) -> None:
        self.connection = connection
        host = getattr(connection, "host", "<unknown>")
        port = getattr(connection, "port", 0)
        self.endpoint = format_address((str(host), int(port)))
        self.request_timeout = request_timeout
        self.verbose = verbose

    def _log(self, message: str) -> None:
        if self.verbose:
            print(message, file=sys.stderr)

    def recv_until(
        self,
        *,
        expected: set[MessageType],
        timeout: float | None = None,
    ) -> tuple[MessageType, bytes]:
        """Receive messages until one of the expected message types is returned."""
        effective_timeout = self.request_timeout if timeout is None else timeout

        while True:
            message_type, payload = self.connection.recv_message(effective_timeout)

            if message_type in expected:
                return message_type, payload

            match message_type:
                case MessageType.PEER_INFO | MessageType.PEER_INFO_SELF:
                    self._log(
                        f"[*] {self.endpoint} ignored {message_name(message_type)} ({len(payload)}B)"
                    )
                    continue
                case MessageType.GET_TIME:
                    self.connection.send_time()
                    continue
                case MessageType.PING:
                    self.connection.send(MessageType.PONG)
                    continue
                case MessageType.BYE:
                    raise RuntimeError("node sent Bye before sync completed")
                case MessageType.DATA_MISSING:
                    raise RuntimeError("node reported the requested data is missing")
                case MessageType.TIME | MessageType.AUTHENTICATION | MessageType.LOGIN | MessageType.NEW_TIP | MessageType.STATUS:
                    self._log(
                        f"[*] {self.endpoint} <- {message_name(message_type)} ({len(payload)}B)"
                    )
                    continue
                case _:
                    self._log(
                        f"[*] {self.endpoint} ignored {message_name(message_type)} ({len(payload)}B)"
                    )

    def request_headers(self, *, start_height: int, stop_height: int) -> list[BlockHeader]:
        """Request a contiguous block-header range."""
        if start_height > stop_height:
            raise ValueError(
                f"start_height {start_height} must be <= stop_height {stop_height}"
            )

        self.connection.send(
            MessageType.ENUM_HDRS,
            encode_height_range(start_height, stop_height),
        )
        self._log(f"[*] {self.endpoint} requested headers {start_height}-{stop_height}")

        message_type, payload = self.recv_until(expected={MessageType.HDR_PACK})
        if message_type != MessageType.HDR_PACK:
            raise RuntimeError(f"expected HdrPack, got {message_name(message_type)}")

        headers = deserialize_header_pack_payloads(payload, self.connection.peer_fork_hashes)
        if not headers:
            raise RuntimeError(
                f"requested header range {start_height}-{stop_height}, node returned no headers"
            )
        if headers[0].height != start_height:
            raise RuntimeError(
                f"requested header range {start_height}-{stop_height}, got first header "
                f"for {headers[0].height}"
            )
        for previous_header, header in zip(headers, headers[1:]):
            if header.height != previous_header.height + 1:
                raise RuntimeError(
                    f"requested header range {start_height}-{stop_height}, got non-contiguous "
                    f"headers {previous_header.height} then {header.height}"
                )
        if headers[-1].height > stop_height:
            raise RuntimeError(
                f"requested header range {start_height}-{stop_height}, got header for "
                f"{headers[-1].height}"
            )
        return headers

    def request_body_range_payload(
        self,
        *,
        headers: Sequence,
        plan: BodyFetchPlan,
    ) -> tuple[MessageType, bytes]:
        """Request a contiguous block-body range and return the raw frame."""
        if not headers:
            raise ValueError("headers must not be empty")

        base_header = headers[0]
        top_header = headers[-1]
        payload = encode_get_body_pack_payload(
            top_height=top_header.height,
            top_hash=bytes.fromhex(top_header.hash),
            flag_perishable=plan.flag_perishable,
            flag_eternal=plan.flag_eternal,
            count_extra=top_header.height - base_header.height,
            block0=plan.block0,
            horizon_lo1=plan.horizon_lo1,
            horizon_hi1=plan.horizon_hi1,
        )
        self.connection.send(MessageType.GET_BODY_PACK, payload)
        self._log(
            "[*] "
            f"{self.endpoint} requested bodies {base_header.height}-{top_header.height} "
            f"(p={plan.flag_perishable}, e={plan.flag_eternal}, block0={plan.block0}, "
            f"lo={plan.horizon_lo1}, hi={plan.horizon_hi1})"
        )

        message_type, response_payload = self.recv_until(
            expected={MessageType.BODY, MessageType.BODY_PACK}
        )
        if message_type in {MessageType.BODY, MessageType.BODY_PACK}:
            return message_type, response_payload
        raise RuntimeError(
            f"unexpected message while waiting for block range: {message_name(message_type)}"
        )

    def fetch_blocks(
        self,
        *,
        start_height: int,
        stop_height: int,
        plan: BodyFetchPlan | None = None,
    ) -> list[DecodedBlock]:
        """Fetch and deserialize a contiguous block range.

        If *plan* is ``None`` a full-body fetch (no fast-sync flags) is used.
        Pass an explicit :class:`BodyFetchPlan` to control perishable/eternal
        flags and fast-sync horizon parameters.
        """
        headers = self.request_headers(start_height=start_height, stop_height=stop_height)
        if plan is None:
            plan = BodyFetchPlan(
                start_height=start_height,
                stop_height=stop_height,
                flag_perishable=BODY_FLAG_FULL,
                flag_eternal=BODY_FLAG_FULL,
                block0=0,
                horizon_lo1=0,
                horizon_hi1=0,
            )
        message_type, payload = self.request_body_range_payload(headers=headers, plan=plan)
        if message_type == MessageType.BODY:
            return [deserialize_body_payload(payload, headers[0])]
        return deserialize_body_pack_payloads(payload, headers)
