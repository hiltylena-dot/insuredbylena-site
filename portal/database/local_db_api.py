#!/usr/bin/env python3
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer

import api_support as _api_support

for _name in dir(_api_support):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_api_support, _name)
from api_handler_base import BackendBaseHandlerMixin
from api_handler_calendar import CalendarHandlerMixin
from api_handler_content import ContentHandlerMixin
from api_handler_documents import DocumentHandlerMixin
from api_handler_leads import LeadHandlerMixin


class LeadSyncHandler(
    BackendBaseHandlerMixin,
    ContentHandlerMixin,
    LeadHandlerMixin,
    CalendarHandlerMixin,
    DocumentHandlerMixin,
    BaseHTTPRequestHandler,
):
    pass

def main() -> None:
    server = HTTPServer((HOST, PORT), LeadSyncHandler)
    print(f"Local DB API listening on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
