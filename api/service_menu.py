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
    """
    config.json の services を読み込んで、
    Flex表示しやすい list に変換して返す。

    想定形式:
    "services": {
      "service_1": {...},
      "service_2": {...}
    }
    """
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
            s.get("order", 999),
            str(s.get("name", "")),
        )
    )

    return services


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


def _create_service_bubble(service: Dict[str, Any], fallback_id: str) -> FlexBubble:
    name = _safe_text(service.get("name"), "サービス")
    description = _safe_text(service.get("description"), "")
    duration = service.get("duration")
    price = service.get("price")
    image_url = _safe_text(service.get("image_url"), DEFAULT_IMAGE) or DEFAULT_IMAGE
    cta_label = _safe_text(service.get("cta_label"), "このメニューで予約する")
    resolved_id = _safe_text(service.get("id"), fallback_id)

    info_rows: List[FlexBox] = []

    if duration:
        info_rows.append(
            FlexBox(
                layout="baseline",
                spacing="sm",
                contents=[
                    FlexText(text="所要時間", size="sm", color="#888888"),
                    FlexText(
                        text=f"{duration}分",
                        size="sm",
                        weight="bold",
                        color="#111111",
                    ),
                ],
            )
        )

    info_rows.append(
        FlexBox(
            layout="baseline",
            spacing="sm",
            contents=[
                FlexText(text="料金", size="sm", color="#888888"),
                FlexText(
                    text=_format_price(price),
                    size="sm",
                    weight="bold",
                    color="#111111",
                ),
            ],
        )
    )

    body_contents: List[Any] = [
        FlexText(text=name, weight="bold", size="xl", wrap=True),
    ]

    if description:
        body_contents.append(
            FlexText(
                text=description,
                size="sm",
                color="#666666",
                wrap=True,
                margin="sm",
            )
        )

    body_contents.extend(
        [
            FlexSeparator(margin="md"),
            FlexBox(
                layout="vertical",
                spacing="sm",
                margin="md",
                contents=info_rows,
            ),
        ]
    )

    return FlexBubble(
        hero=FlexImage(
            url=image_url,
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
                        label=cta_label,
                        displayText=f"{name}で予約したい",
                        data=f"action=select_service&service_id={resolved_id}",
                    ),
                )
            ],
        ),
    )


def send_service_menu(reply_token, configuration) -> None:
    """
    config.json の services を使って
    サービス一覧の Flex carousel を返す。
    """
    try:
        services = _load_services()

        bubbles: List[FlexBubble] = []
        for idx, service in enumerate(services, start=1):
            try:
                bubbles.append(_create_service_bubble(service, f"service_{idx}"))
            except Exception as bubble_error:
                logging.error(
                    f"Failed to build service bubble for index {idx}: {bubble_error}",
                    exc_info=True,
                )

        if not bubbles:
            raise ValueError("No valid service cards could be generated")

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
