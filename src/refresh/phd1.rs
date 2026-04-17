// ARCH-1: PH-D1 phantom-cleanup sub-module for the refresh pipeline.
//
// This module will contain the PH-D1 DELETE+INSERT strategy for handling
// join phantom rows:
// - PH-D1 strategy selection logic (currently around line 4991 in mod.rs)
// - DELETE+INSERT execution path (currently around line 5151 in mod.rs)
// - Co-deletion detection helpers
// - EC-01 convergence validation
//
// Currently all code lives in `super` (mod.rs).  This file is the landing
// zone for the phantom-cleanup layer during the ongoing ARCH-1 migration.
