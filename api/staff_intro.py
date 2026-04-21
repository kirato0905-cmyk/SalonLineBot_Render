import os
import json
import logging
from typing import List, Dict, Any, Optional

from linebot.v3.messaging import (
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    FlexMessage,
    FlexCarousel,
    FlexBubble,
    FlexImage,
    FlexText,
    FlexButton,
    FlexBox,
    MessageAction,
)

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "data", "config.json")
DEFAULT_STAFF_IMAGE = "https://img.icons8.com/?size=96&id=7819&format=png"


def _load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"config.json not found: {CONFIG_FILE}")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("config.json root must be an object")

    return data


def _load_service_name_map(config: Dict[str, Any]) -> Dict[str, str]:
    """
    config.json の services から
    {service_id: service_name} の辞書を作る
    """
    services_raw = config.get("services", {})
    service_name_map: Dict[str, str] = {}

    if not isinstance(services_raw, dict):
        return service_name_map

    for _, service_data in services_raw.items():
        if not isinstance(service_data, dict):
            continue

        service_id = service_data.get("id")
        service_name = service_data.get("name")

        if service_id and service_name:
            service_name_map[str(service_id)] = str(service_name)

    return service_name_map


def _resolve_strength_styles(
    staff: Dict[str, Any],
    service_name_map: Dict[str, str],
) -> List[str]:
    """
    優先順位:
    1. service_ids から services を引いて表示名化
    2. strength_styles があればそれを使う
    """
    service_ids = staff.get("service_ids")
    if isinstance(service_ids, list) and service_ids:
        resolved = []
        for service_id in service_ids:
            name = service_name_map.get(str(service_id))
            if name:
                resolved.append(name)
        if resolved:
            return resolved

    strength_styles = staff.get("strength_styles")
    if isinstance(strength_styles, list):
        return [str(s) for s in strength_styles if s is not None]

    return []


def _load_staffs() -> List[Dict[str, Any]]:
    """
    config.json の staff を読み込んで
    activeなスタッフを order順で返す
    """
    config = _load_config()
    service_name_map = _load_service_name_map(config)

    staffs_raw = config.get("staff", {})
    if not isinstance(staffs_raw, dict):
        return []

    staffs: List[Dict[str, Any]] = []

    for staff_key, staff_data in staffs_raw.items():
        if not isinstance(staff_data, dict):
            continue

        is_active = staff_data.get("is_active", True)
        if is_active is False:
            continue

        name = staff_data.get("name", "")
        if not name:
            logging.warning(f"Skipping staff without name: key={staff_key}")
            continue

        normalized = dict(staff_data)
        normalized["staff_id"] = normalized.get("staff_id", staff_key)
        normalized["_resolved_strength_styles"] = _resolve_strength_styles(
            normalized,
            service_name_map,
        )

        staffs.append(normalized)

    staffs.sort(
        key=lambda s: (
            s.get("order", 999),
            str(s.get("name", "")),
        )
    )

    return staffs


def _safe_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value)


def _build_staff_bubble(staff: Dict[str, Any]) -> FlexBubble:
    """
    1人分のスタッフ紹介Flex bubbleを作る
    """
    name = _safe_text(staff.get("name"), "")
    role = _safe_text(staff.get("role"), "")
    image_url = _safe_text(staff.get("image_url"), DEFAULT_STAFF_IMAGE) or DEFAULT_STAFF_IMAGE
    strength_styles = staff.get("_resolved_strength_styles") or []
    comment = _safe_text(staff.get("comment"), "")
    sns = staff.get("sns") or {}
    instagram = _safe_text(sns.get("instagram"), "")
    staff_id = _safe_text(staff.get("staff_id"), "")

    hero = FlexImage(
        url=image_url,
        size="full",
        aspectRatio="20:13",
        aspectMode="cover",
    )

    body_contents: List[Any] = []

    title_text = f"{role} {name}" if role else name
    body_contents.append(
        FlexText(
            text=title_text,
            weight="bold",
            size="lg",
            wrap=True,
        )
    )

    if strength_styles:
        body_contents.append(
            FlexText(
                text="得意なメニュー",
                weight="bold",
                size="sm",
                margin="md",
            )
        )
        body_contents.append(
            FlexText(
                text=" / ".join(strength_styles),
                size="sm",
                wrap=True,
                margin="xs",
            )
        )

    if comment:
        body_contents.append(
            FlexText(
                text=comment,
                size="sm",
                wrap=True,
                margin="md",
            )
        )

    if instagram:
        body_contents.append(
            FlexText(
                text=f"Instagram: {instagram}",
                size="sm",
                color="#555555",
                wrap=True,
                margin="md",
            )
        )

    body = FlexBox(
        layout="vertical",
        contents=body_contents,
        spacing="sm",
    )

    reserve_text = f"このスタッフで予約する:{staff_id}"
    footer = FlexBox(
        layout="vertical",
        contents=[
            FlexButton(
                style="primary",
                color="#1DB446",
                action=MessageAction(
                    label="このスタッフで予約する",
                    text=reserve_text,
                ),
            )
        ],
        spacing="sm",
        paddingAll="md",
    )

    return FlexBubble(
        hero=hero,
        body=body,
        footer=footer,
    )


def send_staff_intro(reply_token: str, configuration) -> None:
    """
    config.json の staff を使って
    スタッフ紹介の Flex carousel を返す
    """
    staffs = _load_staffs()

    if not staffs:
        logging.warning("No active staffs found in config.json")

        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[
                        FlexMessage(
                            alt_text="スタッフ情報が見つかりませんでした。",
                            contents=FlexBubble(
                                body=FlexBox(
                                    layout="vertical",
                                    contents=[
                                        FlexText(
                                            text="スタッフ情報が見つかりませんでした。",
                                            wrap=True,
                                        )
                                    ],
                                )
                            ),
                        )
                    ],
                )
            )
        return

    bubbles = [_build_staff_bubble(staff) for staff in staffs]
    carousel = FlexCarousel(contents=bubbles[:10])

    flex_message = FlexMessage(
        alt_text="スタッフ紹介",
        contents=carousel,
    )

    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[flex_message],
            )
        )
