"""Leakage-safe charge-off risk baseline for SBA Capital Watch.

This package trains and evaluates an interpretable charge-off classifier
using only fields available at loan approval time. See docs/MODEL_CARD.md
for the target definition, feature policy, and limitations.
"""

from __future__ import annotations

RANDOM_SEED = 42
