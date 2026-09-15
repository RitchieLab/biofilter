"""
Reports awaiting rewrite. Reference material, not running code.

These were written against a relational database and the 4.2.x variant
schema. Several no longer run at all — they select columns like
`variant_masters.variant_id` that 4.3.0 bundles do not carry — and none
of them is reachable from the CLI or the Python facade any more.

They are kept because rewriting a report is easier with the original in
front of you: what it returned, which edge cases it handled, what its
columns meant. Read them; do not import them.

A report leaves this directory when its replacement lands in
`biofilter/modules/report/reports/`. The directory goes when it empties.
"""
