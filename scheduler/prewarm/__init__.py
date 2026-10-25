"""Predictive pre-warming scheduler (CORE-09, CORE-10).

forecast (contracts/forecast.example.json) -> bounded policy -> decision
(contracts/decision.example.json) -> actuator (dry-run, Knative min-scale, or warm-up).
"""
