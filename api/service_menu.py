import json
import logging
import os
from typing import Dict, List, Any, Optional

from linebot.v3.messaging import (
    ApiClient,
    FlexBox,
    FlexBubble,
    FlexButton,
    FlexCarousel,
    FlexImage,
    FlexMessage,
    FlexSeparator,
    FlexText,
    MessagingApi,
    PostbackAction,
    ReplyMessageRequest,
)

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "data", "config.json")
DEFAULT_IMAGE = "https://img.icons8.com/?size=48&id=12245&format=png"


def _load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"config.json not found: {CONFIG_FILE}")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("config.json root must be an object")

    return payload


def _safe_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value)


def _format_price(price: Optional[int]) -> str:
    if price is None:
        return "-"
    if isinstance(price, (int, float)):
        return f"¥{int(price):,}"
    return str(price)


def _load_services() -> List[Dict[str, Any]]:
    payload = _load_config()
    services_raw = payload.get("services", {})
    services: List[Dict[str, Any]] = []

    if not isinstance(services_raw, dict):
        return []

    for service_key, service_data in services_raw.items():
        if not isinstance(service_data, dict):
            continue
        if service_data.get("is_active", True) is False:
            continue
        if not service_data.get("id") or not service_data.get("name"):
            continue
        normalized = dict(service_data)
        normalized["_config_key"] = service_key
        services.append(normalized)

    services.sort(key=lambda s: (str(s.get("category", "")), int(s.get("display_order", 999)), str(s.get("name", ""))))
    return services


def _load_categories() -> List[Dict[str, Any]]:
    payload = _load_config()
    categories = payload.get("service_categories", [])
    if not isinstance(categories, list):
        return []
    return sorted(
        [c for c in categories if isinstance(c, dict) and c.get("id") and c.get("name")],
        key=lambda c: int(c.get("display_order", 999)),
    )


def _load_featured_sets() -> List[Dict[str, Any]]:
    payload = _load_config()
    featured_sets = payload.get("featured_sets", [])
    services_by_id = {s.get("id"): s for s in _load_services()}
    valid_sets: List[Dict[str, Any]] = []

    if not isinstance(featured_sets, list):
        return []

    for featured_set in featured_sets:
        if not isinstance(featured_set, dict):
            continue
        if featured_set.get("is_active", True) is False:
            continue
        service_ids = featured_set.get("services", [])
        if not isinstance(service_ids, list) or not service_ids:
            continue
        if any(service_id not in services_by_id for service_id in service_ids):
            continue
        valid_sets.append(featured_set)

    valid_sets.sort(key=lambda x: int(x.get("display_order", 999)))
    return valid_sets


def _create_featured_set_bubble(featured_set: Dict[str, Any]) -> FlexBubble:
    name = _safe_text(featured_set.get("name"), "人気セット")
    description = _safe_text(featured_set.get("description"), "人気セットメニューです。")
    duration = featured_set.get("duration")
    price = featured_set.get("price")
    set_id = _safe_text(featured_set.get("id"), "")

    body_contents: List[Any] = [
        FlexText(text="人気セット", size="sm", color="#888888"),
        FlexText(text=name, weight="bold", size="xl", wrap=True, margin="sm"),
        FlexText(text=description, size="sm", color="#666666", wrap=True, margin="md"),
        FlexSeparator(margin="md"),
        FlexBox(
            layout="vertical",
            spacing="sm",
            margin="md",
            contents=[
                FlexBox(
                    layout="baseline",
                    spacing="sm",
                    contents=[
                        FlexText(text="所要時間", size="sm", color="#888888"),
                        FlexText(text=f"{duration}分", size="sm", weight="bold", color="#111111"),
                    ],
                ),
                FlexBox(
                    layout="baseline",
                    spacing="sm",
                    contents=[
                        FlexText(text="料金", size="sm", color="#888888"),
                        FlexText(text=_format_price(price), size="sm", weight="bold", color="#111111"),
                    ],
                ),
            ],
        ),
    ]

    return FlexBubble(
        hero=FlexImage(url=DEFAULT_IMAGE, size="full", aspectRatio="20:13", aspectMode="cover"),
        body=FlexBox(layout="vertical", spacing="md", paddingAll="16px", contents=body_contents),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
                    action=PostbackAction(
                        label="このセットで予約する",
                        displayText=name,
                        data=f"action=select_featured_set&set_id={set_id}",
                    ),
                )
            ],
        ),
    )


def _create_category_entry_bubble() -> FlexBubble:
    categories = _load_categories()
    lines = [f"・{category.get('name')}" for category in categories]
    return FlexBubble(
        body=FlexBox(
            layout="vertical",
            spacing="md",
            paddingAll="16px",
            contents=[
                FlexText(text="単品メニュー", weight="bold", size="xl", wrap=True),
                FlexText(text="カテゴリから選びたい方はこちら", size="sm", color="#666666", wrap=True),
                FlexSeparator(margin="md"),
                FlexText(text="\n".join(lines), size="sm", wrap=True, margin="md"),
            ],
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="secondary",
                    height="sm",
                    action=PostbackAction(
                        label="単品メニューを見る",
                        displayText="単品メニューを見る",
                        data="action=view_single_menu",
                    ),
                )
            ],
        ),
    )


def send_service_menu(reply_token, configuration) -> None:
    try:
        featured_sets = _load_featured_sets()
        bubbles: List[FlexBubble] = []

        for featured_set in featured_sets[:9]:
            bubbles.append(_create_featured_set_bubble(featured_set))

        bubbles.append(_create_category_entry_bubble())

        if not bubbles:
            raise ValueError("No valid service cards could be generated")

        container = bubbles[0] if len(bubbles) == 1 else FlexCarousel(contents=bubbles[:10])
        message = FlexMessage(alt_text="サービス一覧はこちら", contents=container)

        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(reply_token=reply_token, messages=[message])
            )

    except Exception as e:
        logging.error(f"Failed to send service menu: {e}", exc_info=True)
        raise

