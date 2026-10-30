"""Pure lifecycle operations for Phase 1 contracts."""

from dataclasses import replace
from datetime import datetime

from ftx_paper.strategies.orderflow.canonical.ict.domain.enums import EpisodeLifecycle, TransitionResolution
from ftx_paper.strategies.orderflow.canonical.ict.domain.records import BalanceEpisode, TransitionAttempt


def close_balance(*, episode: BalanceEpisode, at: datetime, reason: str) -> BalanceEpisode:
    if episode.lifecycle is EpisodeLifecycle.ENDED:
        if episode.ended_at == at and episode.end_reason == reason:
            return episode
        raise ValueError("ended balance cannot be closed again with different details")
    if not reason:
        raise ValueError("balance closure requires a reason")
    if episode.lifecycle is not EpisodeLifecycle.ACTIVE:
        raise ValueError("only an active balance can be closed")
    if at < episode.formation_end:
        raise ValueError("balance cannot end before formation ends")
    if episode.confirmed_at is not None and at < episode.confirmed_at:
        raise ValueError("balance cannot end before confirmation")
    return replace(
        episode,
        lifecycle=EpisodeLifecycle.ENDED,
        ended_at=at,
        end_reason=reason,
    )


def resolve_transition(
    *, attempt: TransitionAttempt, resolution: TransitionResolution, at: datetime
) -> TransitionAttempt:
    if attempt.resolution is not TransitionResolution.UNRESOLVED:
        return attempt
    if resolution is TransitionResolution.UNRESOLVED:
        raise ValueError("resolve_transition requires a terminal resolution")
    if at < attempt.available_at:
        raise ValueError("transition cannot resolve before evidence is available")
    unavailable = [evidence.name for evidence in attempt.evidence if evidence.available_at > at]
    if unavailable:
        raise ValueError(f"transition evidence is not yet available: {', '.join(unavailable)}")
    return replace(attempt, resolution=resolution, resolved_at=at)
