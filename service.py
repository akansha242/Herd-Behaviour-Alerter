from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DetectionThresholds:
    volume_spike_sigma: float = 2.0
    momentum_window: int = 5
    min_series: int = 20


class HerdBehaviorService:
    def __init__(self, thresholds: Optional[DetectionThresholds] = None) -> None:
        self.thresholds = thresholds or DetectionThresholds()

    def analyze_series(
        self,
        timestamps: List[int],
        prices: List[float],
        volumes: Optional[List[float]] = None,
        entities: Optional[List[str]] = None,
    ) -> Dict:
        if not (len(timestamps) == len(prices)):
            raise ValueError("timestamps and prices length mismatch")
        if len(timestamps) < self.thresholds.min_series:
            # Allow small series but reduce confidence
            pass

        score, details, reason = self._compute_score(prices, volumes, entities)
        is_alert = score >= 0.65
        return {
            "is_alert": is_alert,
            "score": round(score, 4),
            "reason": reason,
            "details": details,
        }

    def _compute_score(
        self,
        prices: List[float],
        volumes: Optional[List[float]],
        entities: Optional[List[str]],
    ):
        import math

        def momentum(series: List[float], window: int) -> float:
            if len(series) < window + 1:
                return 0.0
            gains = 0.0
            for i in range(-window + 1, 1):
                gains += series[i] - series[i - 1]
            return max(0.0, gains) / (abs(gains) + 1e-9)

        def stddev(series: List[float]) -> float:
            if len(series) == 0:
                return 0.0
            m = sum(series) / len(series)
            return math.sqrt(sum((x - m) ** 2 for x in series) / len(series))

        window = min(self.thresholds.momentum_window, max(2, len(prices) // 5))
        mom = momentum(prices, window)

        vol_spike_score = 0.0
        vol_detail = None
        if volumes and len(volumes) == len(prices):
            base = volumes[:-window]
            recent = volumes[-window:]
            base_std = stddev(base)
            base_mean = sum(base) / max(1, len(base))
            recent_mean = sum(recent) / max(1, len(recent))
            if base_std > 0:
                z = (recent_mean - base_mean) / base_std
                vol_spike_score = min(1.0, max(0.0, z / self.thresholds.volume_spike_sigma))
                vol_detail = {
                    "name": "volume_spike_z",
                    "value": float(round(z, 3)),
                    "threshold": float(self.thresholds.volume_spike_sigma),
                    "severity": "high" if z >= self.thresholds.volume_spike_sigma else "low",
                }

        entity_bonus = 0.0
        if entities and len(entities) >= 3:
            # simplistic: more participants => more herd-likeness
            entity_bonus = min(0.2, 0.05 * (len(entities) - 2))

        score = 0.55 * mom + 0.35 * vol_spike_score + entity_bonus
        score = max(0.0, min(1.0, score))

        details = []
        details.append({
            "name": "momentum_recent",
            "value": float(round(mom, 3)),
            "threshold": 0.6,
            "severity": "medium" if mom >= 0.6 else "low",
        })
        if vol_detail:
            details.append(vol_detail)
        if entity_bonus > 0:
            details.append({
                "name": "entity_participation",
                "value": float(round(entity_bonus, 3)),
                "threshold": 0.05,
                "severity": "info",
            })

        reason = (
            "Momentum and volume suggest coordinated activity"
            if score >= 0.65
            else "No strong evidence of herd behavior"
        )
        return score, details, reason

    def example_signal(self) -> Dict:
        prices = [100, 101, 102, 103, 104, 106, 109, 113, 118, 124, 131]
        volumes = [10, 11, 10, 12, 11, 15, 18, 22, 27, 33, 40]
        entities = ["fundA", "fundB", "deskX", "deskY"]
        return self.analyze_series(
            timestamps=list(range(len(prices))),
            prices=prices,
            volumes=volumes,
            entities=entities,
        )


