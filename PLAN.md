## Implementation status

See CODE_CLEANUP_1.md ... CODE_CLEANUP_6.md for the per-stage
implementation plans. Stages 1-6 are prerequisites; each must
build clean before the next starts. Stage 2 is the only one
allowed to leave the build broken (it sets up the deletions
that stage 3 wires around).
