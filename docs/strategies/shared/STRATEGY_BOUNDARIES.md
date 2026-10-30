# Strategy Boundaries

`ftx-paper` contains two independent strategy policies. They are not two names
for one strategy and must not be compared as if they were the same
implementation.

## Strategy Entry Points

| Strategy | Public entry point | Stable strategy name | Runtime strategy ID |
| --- | --- | --- | --- |
| FTX research | `ftx_paper.strategies.research` | `ftx-paper-research` | `ftx-paper-research` |
| Orderflow | `ftx_paper.strategies.orderflow` | `ftx-paper-orderflow` | `ftx-paper-orderflow` |

The implementation paths are now separate:

- Research policy: `src/ftx_paper/strategies/research/`
- Orderflow policy: `src/ftx_paper/strategies/orderflow/`
- Shared contracts and compatibility/support modules: `src/ftx_paper/strategies/shared/`

The shared modules under `src/ftx_paper/strategies/shared/` provide common
contracts and coordination. The Paper orderflow path is not
`ftx-paper/src/orderflow/`; that path belongs to the separate NiftyZoning
repository and must never be created in this project.

## Ownership

The research and orderflow policies own different market hypotheses and
strategy-local state. The shared Paper runtime owns feeds, decision timing,
portfolio/account state, simulated execution, persistence, and attribution.
`MultiStrategyEngine` may run both policies for comparison, but it does not
share policy state between them.

Orderflow behavior is an independent Paper implementation informed by the
canonical NiftyZoning orderflow contracts. It must not import NiftyZoning code,
databases, runtime state, or generated artifacts. The research strategy must
not receive orderflow-specific features or lifecycle state.
