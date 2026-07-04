"""Back-compat shim: external consumers still import ``landline.telegram_fmt``.

The formatter now lives at ``landline.telegram.fmt``. This shim re-exports
its public surface so ~/.mineru cron scripts (and any other external
importer of the old path) keep working.
"""

from landline.telegram.fmt import *  # noqa: F401,F403
from landline.telegram.fmt import md_to_telegram_html  # noqa: F401
