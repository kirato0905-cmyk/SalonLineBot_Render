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


def _load_services() -> List[Dict[str, Any]]:
    payload = _load_config()
    services_raw = payload.get("services", {})

    if not isinstance(services_raw, dict) or not services_raw:
        raise ValueError("config.json does not contain any services")

    services: List[Dict[str, Any]] = []

    for service_key, service_data in services_raw.items():
        if not isinstance(service_data, dict):
            continue

        service_id = service_data.get("id")
        name = service_data.get("name")

        if not service_id or not name:
            logging.warning(
                f"Skipping invalid service entry: key={service_key}, data={service_data}"
            )
            continue

        is_active = service_data.get("is_active", True)
        if is_active is False:
            continue

        normalized = dict(service_data)
        normalized["_config_key"] = service_key
        services.append(normalized)

    if not services:
        raise ValueError("config.json contains no valid active services")

    services.sort(
        key=lambda s: (
            s.get("display_order", 999),
            s.get("order", 999),
            str(s.get("name", "")),
        )
    )

    return services


def _load_featured_sets() -> List[Dict[str, Any]]:
    payload = _load_config()
    featured_sets = payload.get("featured_sets", [])
    services_by_id = {service["id"]: service for service in _load_services()}

    normalized_sets: List[Dict[str, Any]] = []
    for item in featured_sets:
        if not isinstance(item, dict):
            continue
        if item.get("is_active", True) is False:
            continue

        service_ids = item.get("services", [])
        if not isinstance(service_ids, list) or not service_ids:
            continue

        resolved = []
        is_valid = True
        for service_id in service_ids:
            service = services_by_id.get(service_id)
            if not service:
                is_valid = False
                break
            resolved.append(service)

        if not is_valid:
            continue

        normalized = dict(item)
        normalized["resolved_services"] = resolved
        normalized_sets.append(normalized)

    normalized_sets.sort(key=lambda x: (x.get("display_order", 999), str(x.get("name", ""))))
    return normalized_sets


def _load_categories() -> List[Dict[str, Any]]:
    payload = _load_config()
    categories = payload.get("service_categories", [])
    normalized: List[Dict[str, Any]] = []

    for item in categories:
        if not isinstance(item, dict):
            continue
        if not item.get("id") or not item.get("name"):
            continue
        normalized.append(dict(item))

    normalized.sort(key=lambda x: (x.get("display_order", 999), str(x.get("name", ""))))
    return normalized


def _format_price(price: Optional[int]) -> str:
    if price is None:
        return "-"
    if isinstance(price, (int, float)):
        return f"¥{int(price):,}"
    return str(price)


def _safe_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value)


def _create_featured_set_bubble(featured_set: Dict[str, Any]) -> FlexBubble:
    title = _safe_text(featured_set.get("name"), "人気セット")
    resolved_services = featured_set.get("resolved_services", [])
    description = " / ".join(service.get("name", "") for service in resolved_services if isinstance(service, dict))
    price = featured_set.get("price")
    duration = featured_set.get("duration")

    body_contents: List[Any] = [
        FlexText(text="人気セット", size="xs", color="#888888"),
        FlexText(text=title, weight="bold", size="xl", wrap=True, margin="sm"),
    ]

    if description:
        body_contents.append(
            FlexText(text=description, size="sm", color="#666666", wrap=True, margin="sm")
        )

    body_contents.extend(
        [
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
    )

    return FlexBubble(
        hero=FlexImage(
            url=DEFAULT_IMAGE,
            size="full",
            aspectRatio="20:13",
            aspectMode="cover",
        ),
        body=FlexBox(
            layout="vertical",
            spacing="md",
            paddingAll="16px",
            contents=body_contents,
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
                    action=PostbackAction(
                        label="このセットで予約する",
                        displayText=title,
                        data=f"action=select_featured_set&set_id={featured_set.get('id')}",
                    ),
                )
            ],
        ),
    )


def _create_single_menu_entry_bubble(categories: List[Dict[str, Any]]) -> FlexBubble:
    category_text = " / ".join(category.get("name", "") for category in categories[:6])
    if len(categories) > 6:
        category_text += " / ..."

    body_contents: List[Any] = [
        FlexText(text="単品メニュー", size="xs", color="#888888"),
        FlexText(text="単品メニューを見る", weight="bold", size="xl", wrap=True, margin="sm"),
        FlexText(
            text=category_text or "カテゴリから選択できます",
            size="sm",
            color="#666666",
            wrap=True,
            margin="sm",
        ),
        FlexSeparator(margin="md"),
        FlexText(
            text="カット / カラー / パーマ / ストレート / トリートメント / ヘッドスパ など",
            size="sm",
            wrap=True,
            margin="md",
        ),
    ]

    return FlexBubble(
        hero=FlexImage(
            url=DEFAULT_IMAGE,
            size="full",
            aspectRatio="20:13",
            aspectMode="cover",
        ),
        body=FlexBox(
            layout="vertical",
            spacing="md",
            paddingAll="16px",
            contents=body_contents,
        ),
        footer=FlexBox(
            layout="vertical",
            contents=[
                FlexButton(
                    style="primary",
                    height="sm",
                    color="#111111",
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
        categories = _load_categories()

        bubbles: List[FlexBubble] = []
        for featured_set in featured_sets:
            bubbles.append(_create_featured_set_bubble(featured_set))

        bubbles.append(_create_single_menu_entry_bubble(categories))

        container = bubbles[0] if len(bubbles) == 1 else FlexCarousel(contents=bubbles[:10])

        message = FlexMessage(
            alt_text="サービス一覧はこちら",
            contents=container,
        )

        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[message],
                )
            )

    except Exception as e:
        logging.error(f"Failed to send service menu: {e}", exc_info=True)
        raise

