"""Explicit strategy entry points.

The package separates strategy policy selection from the shared Paper runtime.
The two modules below are the only composition-facing strategy namespaces:

* :mod:`ftx_paper.strategies.research` for ``ftx-paper-research``
* :mod:`ftx_paper.strategies.orderflow` for the canonical orderflow strategy

Market feeds, execution, persistence, and portfolio ownership remain shared
runtime services and are intentionally not duplicated here.
"""
