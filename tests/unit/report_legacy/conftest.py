"""
The frozen report layer's tests are parked.

`biofilter.modules.report_legacy` is frozen (ADR-004 §2.2): bug fixes
only, no new reports, and deletion when its last report has moved. Its
tests are skipped rather than deleted, because the module is still
serving reports and the tests are the record of what it did.

Two things make them misleading to keep running:

- They exercise SQLite fixtures whose schema the bundles no longer
  carry. `variant_masters` is `position`/`variant_key` now; these
  fixtures declare the 4.2.x shape and pass by agreeing with themselves,
  which is not evidence about anything.
- Reports migrated to the native module are covered by their own tests
  under tests/unit/report/, against a fixture bundle.

Remove this file to bring them back. It goes when report_legacy does.
"""


collect_ignore_glob = ["test_*.py"]
