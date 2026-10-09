"""Turn a job-run request into a command, run it with an executor, and report what came out.

This is the library the orchestrators (the UI today, a Kubernetes operator later) call; it knows nothing about who asks.
The `process` executor lives here; the other executors (kubernetes, ssh, spcs-job) are separate modules with the same
`run(request, operator, environment, repo) -> Result` shape.
"""

from .core import (  # noqa: F401
    JobError,
    Result,
    check_capabilities,
    load_operator,
    render_command,
    validate_request,
)
from .process import run_process  # noqa: F401
