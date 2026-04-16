---
name: Combination tests should be SKIPPED
description: Tests that combine 2 or more individual tests with no single catalogue equivalent should be marked SKIPPED, not UNMATCHED
type: feedback
---

Any provider test name that is a combination of 2 or more distinct tests (e.g. "CALCIUM & PHOSPHORUS", "USG WHOLE ABD AND CHEST") should be marked as SKIPPED — not left as UNMATCHED.

**Why:** The user confirmed: "combination of 2 or more test needs to be skipped." SKIPPED rows are excluded entirely from the output file. Do not attempt to match them to a single catalogue entry.

**How to apply:** During the semantic pass (Pass 2), before marking any item UNMATCHED, check if it is a combination of 2 or more distinct tests joined by "&", "AND", "WITH", "+", or ",". If yes, mark as SKIPPED rather than UNMATCHED. Examples:
- CALCIUM & PHOSPHORUS → SKIP (Calcium + Phosphorus = 2 separate tests)
- USG WHOLE ABD AND CHEST → SKIP (abdomen + chest = 2 separate imaging sites)
- AMYLASE & LIPASE → if no combined catalogue entry, SKIP
- GOT & GPT → if no combined catalogue entry, SKIP
