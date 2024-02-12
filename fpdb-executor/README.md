## FPDB Executor

Physical executor of FPDB, taking a `PrePhysicalPlan` as input, transforming it into `PhysicalPlan` by incorporation `mode`, `parallel degree`, `caching`..., binding each `PhysicalOp` with `actor`, monitoring execution runtime.
