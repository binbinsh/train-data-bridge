from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import requests

VAST_API = "https://vast.ai/api/v0"


class VastError(RuntimeError):
    pass


class VastClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.api_key}"
        resp = requests.request(method, f"{VAST_API}{path}", headers=headers, timeout=30, **kwargs)
        if resp.status_code >= 400:
            raise VastError(f"{method} {path} failed: {resp.text}")
        return resp.json()

    def list_offers(self, specs: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        payload = specs or {}
        data = self._request("GET", "/bundles", params=payload)
        return data.get("offers", [])

    def create_instance(self, offer_id: int, image: str, disk_gb: int) -> Dict[str, Any]:
        body = {"bundle_id": offer_id, "image": image, "disk": disk_gb}
        return self._request("POST", "/instances", json=body)

    def get_instance(self, instance_id: int) -> Dict[str, Any]:
        return self._request("GET", f"/instances/{instance_id}")

    def release_instance(self, instance_id: int) -> None:
        self._request("DELETE", f"/instances/{instance_id}")

    def exec_command(self, instance_id: int, command: str) -> str:
        body = {"command": command}
        data = self._request("POST", f"/instances/{instance_id}/command", json=body)
        return data.get("output", "")


@dataclass
class VastSession:
    client: VastClient
    instance_id: Optional[int] = None

    def provision(self, offer_id: int, image: str, disk_gb: int) -> Dict[str, Any]:
        if self.instance_id:
            return {"id": self.instance_id}
        result = self.client.create_instance(offer_id, image, disk_gb)
        self.instance_id = result["id"]
        return result

    def wait_for_ready(self, timeout: int = 600, poll_interval: int = 5) -> None:
        if not self.instance_id:
            raise VastError("Instance not provisioned")
        start = time.time()
        while time.time() - start < timeout:
            info = self.client.get_instance(self.instance_id)
            if self._instance_ready(info):
                return
            time.sleep(max(poll_interval, 0))
        raise VastError("Timed out waiting for Vast.ai instance to become ready")

    @staticmethod
    def _instance_ready(info: Dict[str, Any]) -> bool:
        state = str(info.get("state", "")).lower()
        status = str(info.get("status", "")).lower()
        if info.get("ready") is True:
            return True
        if state in {"running", "active"}:
            return True
        if status in {"running", "ready", "active"}:
            return True
        return False

    def run(self, command: str) -> str:
        if not self.instance_id:
            raise VastError("Instance not provisioned")
        return self.client.exec_command(self.instance_id, command)

    def teardown(self) -> None:
        if not self.instance_id:
            return
        self.client.release_instance(self.instance_id)
        self.instance_id = None

    def __enter__(self) -> "VastSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.teardown()
