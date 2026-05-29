from __future__ import annotations

import uvicorn

from msk_mcp.config import load_settings
from msk_mcp.server import build_app, build_context


def main() -> None:
    settings = load_settings()
    ctx = build_context(settings)
    app = build_app(ctx)
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_level=settings.log_level.lower())


if __name__ == "__main__":
    main()
