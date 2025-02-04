import asyncio
from typing import Any

import httpx
import structlog
import uuid
import curlify2
from json import JSONDecodeError
from swagger_coverage_py.request_schema_handler import RequestSchemaHandler
from swagger_coverage_py.uri import URI
from restclient.configuration import Configuration
from restclient.utilities import allure_attach


class RestClient:
    def __init__(self, configuration: Configuration):
        self.host = configuration.host
        self.set_headers(configuration.headers)
        self.disable_log = configuration.disable_log
        self.session = httpx.AsyncClient(verify=False)
        self.log = structlog.get_logger(__name__).bind(service="api")

    def set_headers(self, headers: dict | None) -> None:
        if headers:
            self.session.headers.update(headers)

    async def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self._send_request(method="POST", path=path, **kwargs)

    async def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self._send_request(method="GET", path=path, **kwargs)

    async def put(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self._send_request(method="PUT", path=path, **kwargs)

    async def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self._send_request(method="DELETE", path=path, **kwargs)

    @allure_attach
    async def _send_request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        log = self.log.bind(event_id=str(uuid.uuid4()))
        full_url = self.host + path

        if self.disable_log:
            rest_response = await self.session.request(method=method, url=full_url, **kwargs)
            rest_response.raise_for_status()
            return rest_response

        log.msg(
            event="Request",
            method=method,
            full_url=full_url,
            params=kwargs.get("params"),
            headers=kwargs.get("headers"),
            json=kwargs.get("json"),
            data=kwargs.get("data"),
        )
        rest_response = await self.session.request(method=method, url=full_url, **kwargs)
        curl = curlify2.Curlify(rest_response.request).to_curl()

        uri = URI(
            host=self.host,
            base_path="",
            unformatted_path=path,
            uri_params=kwargs.get("params"),
        )
        handler = RequestSchemaHandler(uri, method.lower(), rest_response, kwargs)
        await asyncio.to_thread(handler.write_schema)

        print(curl)
        log.msg(
            event="Response",
            status_code=rest_response.status_code,
            headers=rest_response.headers,
            json=self._get_json(rest_response),
        )
        rest_response.raise_for_status()
        return rest_response

    @staticmethod
    def _get_json(rest_response: httpx.Response) -> dict:
        try:
            return rest_response.json()
        except JSONDecodeError:
            return {}
