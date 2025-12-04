"""
Reservation flow system with intent detection, candidate suggestions, and confirmation
"""
import re
import os
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import logging
from api.google_calendar import GoogleCalendarHelper

class ReservationFlow:
    def __init__(self):
        self.user_states = {}  # Store user reservation states
        self.google_calendar = GoogleCalendarHelper()  # Initialize Google Calendar integration
        self.line_configuration = None  # Will be set from main handler
        
        # Load services and staff data from JSON
        self.services_data = self._load_services_data()
        self.services = self.services_data.get("services", {})
        self.staff_members = self.services_data.get("staff", {})
        
        # Load keywords from JSON
        self.keywords_data = self._load_keywords_data()
        self.intent_keywords = self.keywords_data.get("intent_keywords", {})
        self.navigation_keywords = self.keywords_data.get("navigation_keywords", {})
        self.confirmation_keywords = self.keywords_data.get("confirmation_keywords", {})
    
    def _load_services_data(self) -> Dict[str, Any]:
        """Load services and staff data from JSON file"""
        try:
            # Get the directory of this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            services_file = os.path.join(current_dir, "data", "services.json")
            
            with open(services_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load services data: {e}")
            raise RuntimeError(f"Cannot load services.json: {e}")
    
    def _load_keywords_data(self) -> Dict[str, Any]:
        """Load keywords data from JSON file"""
        try:
            # Get the directory of this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            keywords_file = os.path.join(current_dir, "data", "keywords.json")
            
            with open(keywords_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load keywords data: {e}")
            raise RuntimeError(f"Cannot load keywords.json: {e}")
    
    def _calculate_time_duration_minutes(self, start_time: str, end_time: str) -> int:
        """Calculate duration in minutes between two time strings (HH:MM format)"""
        try:
            start_hour, start_minute = map(int, start_time.split(':'))
            end_hour, end_minute = map(int, end_time.split(':'))
            
            start_total_minutes = start_hour * 60 + start_minute
            end_total_minutes = end_hour * 60 + end_minute
            
            return end_total_minutes - start_total_minutes
        except (ValueError, IndexError):
            return 0
    
    def _calculate_optimal_end_time(self, start_time: str, service_duration_minutes: int) -> str:
        """Calculate the optimal end time based on start time and service duration"""
        try:
            start_hour, start_minute = map(int, start_time.split(':'))
            start_total_minutes = start_hour * 60 + start_minute
            
            end_total_minutes = start_total_minutes + service_duration_minutes
            
            end_hour = end_total_minutes // 60
            end_minute = end_total_minutes % 60
            
            return f"{end_hour:02d}:{end_minute:02d}"
        except (ValueError, IndexError):
            return start_time
    
    def _get_service_name_by_id(self, service_id: str) -> str:
        """Get service name by ID"""
        return self.services.get(service_id, {}).get("name", service_id)
    
    def _get_staff_name_by_id(self, staff_id: str) -> str:
        """Get staff name by ID"""
        return self.staff_members.get(staff_id, {}).get("name", staff_id)
    
    def _get_service_id_by_name(self, service_name: str) -> str:
        """Get service ID by name"""
        for service_id, service_data in self.services.items():
            if service_data.get("name") == service_name:
                return service_id
        return service_name

    def _resolve_service_name(self, identifier: str) -> Optional[str]:
        """Resolve a service identifier (id, key, or name) to a canonical service name."""
        if not identifier:
            return None

        normalized = identifier.strip()
        if normalized in self.services:
            return self.services[normalized].get("name", normalized)

        for service in self.services.values():
            service_id = service.get("id")
            service_name = service.get("name")
            if service_id and service_id.lower() == normalized.lower():
                return service_name
            if service_name and service_name.lower() == normalized.lower():
                return service_name
        return None
    
    def _get_staff_id_by_name(self, staff_name: str) -> str:
        """Get staff ID by name"""
        for staff_id, staff_data in self.staff_members.items():
            if staff_data.get("name") == staff_name:
                return staff_id
        return staff_name
    
    def _has_single_staff(self) -> bool:
        """Check if there's only one staff member (excluding '未指定')"""
        active_staff = [staff for staff_id, staff in self.staff_members.items() 
                        if staff.get("name") != "未指定"]
        return len(active_staff) == 1
    
    def _get_single_staff_name(self) -> str:
        """Get the name of the single staff member"""
        active_staff = [staff for staff_id, staff in self.staff_members.items() 
                        if staff.get("name") != "未指定"]
        if len(active_staff) == 1:
            return active_staff[0].get("name")
        return None
    
    def _get_modification_menu(self) -> str:
        """Get the modification menu, conditionally showing staff option"""
        menu_items = [
            "1️⃣ 日時変更したい",
            "2️⃣ サービス変更したい"
        ]
        
        # Only show staff modification if there are multiple staff members
        if not self._has_single_staff():
            menu_items.append("3️⃣ 担当者変更したい")
            menu_items.append("4️⃣ 複数項目変更したい（再予約）")
        else :
            menu_items.append("3️⃣ 複数項目変更したい（再予約）")
        
        return "\n".join(menu_items)
    
    def _get_available_slots(self, selected_date: str = None, staff_name: str = None) -> List[Dict[str, Any]]:
        """Get available time slots from Google Calendar for a specific date and staff member"""
        if selected_date is None:
            # If no date specified, get slots for today
            selected_date = datetime.now().strftime("%Y-%m-%d")
        
        # If staff_name is provided, use staff-specific availability
        if staff_name:
            # Get staff-specific available slots
            staff_slots = self.google_calendar.get_available_slots_for_modification(selected_date, None, staff_name)
            return staff_slots
        
        # Fallback to general availability (for cases where staff is not selected yet)
        # Convert string date to datetime objects for the specific day
        start_date = datetime.strptime(selected_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)  # Next day at 00:00
        
        # Get all slots for the date range and filter for the specific date
        # Note: If staff_name is None, get_available_slots will use default calendar
        all_slots = self.google_calendar.get_available_slots(start_date, end_date, staff_name)
        
        # Filter slots for the specific date
        date_slots = [slot for slot in all_slots if slot["date"] == selected_date]
        
        return date_slots
    
    def _create_calendar_template(self) -> str:
        """Create Google Calendar URL for date selection"""
        # Get the Google Calendar URL from the calendar helper
        calendar_url = self.google_calendar.get_calendar_url()
        
        calendar_message = "📅 **ご希望の日付をお選びください**\n\n"
        calendar_message += "🗓️ **Googleカレンダーで空き状況を確認してください：**\n"
        calendar_message += f"🔗 {calendar_url}\n\n"
        calendar_message += "💡 **手順：**\n"
        calendar_message += "1️⃣ 上記リンクをクリックしてGoogleカレンダーを開く\n"
        calendar_message += "2️⃣ 空いている日付を確認\n"
        calendar_message += "3️⃣ 希望の日付を「YYYY-MM-DD」形式で送信\n"
        calendar_message += "📝 例：`2025-01-15`\n\n"
        calendar_message += "❌ 予約をキャンセルする場合は「キャンセル」と送信"
        
        return calendar_message
    
    
    def detect_intent(self, message: str, user_id: str = None) -> str:
        """Detect user intent from message with context awareness"""
        # Normalize message: strip whitespace
        message_normalized = message.strip()
        
        # Check if user is in reservation flow
        if user_id and user_id in self.user_states:
            state = self.user_states[user_id]
            step = state["step"]
            
            # During other reservation steps, treat as reservation flow
            if step in ["service_selection", 'staff_selection', "date_selection", "time_selection", "confirmation"]:
                return "reservation_flow"
            # If user is in cancel or modify flow, continue the flow regardless of message type
            if step in ["cancel_select_reservation", "cancel_confirm", "modify_select_reservation", "modify_select_field", "modify_time_date_select", "modify_time_input_date", "modify_time_select", "modify_confirm", "modify_staff_select", "modify_service_select", "modify_re_reservation_confirm"]:
                intent = step.split("_")[0]  # Return "cancel" or "modify"
                print(f"Intent detection - User: {user_id}, Step: {step}, Intent: {intent}")
                return intent
        
        # Check if message is a reservation ID format
        if re.match(r"^RES-\d{8}-\d{4}$", message_normalized):
            # If it's a reservation ID but user is not in any flow, we need to determine intent
            # For now, we'll return "general" and let the user specify their intent
            return "general"
        
        # Check if message is a date format (YYYY-MM-DD)
        if re.match(r"^\d{4}-\d{2}-\d{2}$", message_normalized):
            # Validate the date format
            try:
                datetime.strptime(message_normalized, "%Y-%m-%d")
                print(f"Detected date format intent for message: '{message_normalized}'")
                return "reservation_flow"
            except ValueError:
                # Invalid date format (like 2025-02-29 in non-leap year), continue with other checks
                pass
        
        # Get keywords from JSON data
        reservation_keywords = self.intent_keywords.get("reservation", [])
        cancel_keywords = self.intent_keywords.get("cancel", [])
        modify_keywords = self.intent_keywords.get("modify", [])
        
        # Priority order: modify > cancel > reservation (check specific keywords first to avoid substring issues)
        # Use 'in' operator for substring matching (works with Japanese)
        if any(keyword in message_normalized for keyword in modify_keywords):
            print(f"Detected 'modify' intent for message: '{message_normalized}'")
            return "modify"
        elif any(keyword in message_normalized for keyword in cancel_keywords):
            print(f"Detected 'cancel' intent for message: '{message_normalized}'")
            return "cancel"
        elif any(keyword in message_normalized for keyword in reservation_keywords):
            print(f"Detected 'reservation' intent for message: '{message_normalized}'")
            return "reservation"
        else:
            print(f"Detected 'general' intent for message: '{message_normalized}'")
            return "general"
    
    def handle_reservation_flow(self, user_id: str, message: str) -> str:
        """Handle the complete reservation flow"""
        if user_id not in self.user_states:
            self.user_states[user_id] = {"step": "start", "data": {"user_id": user_id}}
        
        # Check for flow cancellation at any step
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"
        
        state = self.user_states[user_id]
        step = state["step"]
        
        if step == "start":
            # Check if the message is a date format - if so, start reservation and handle date
            if re.match(r"^\d{4}-\d{2}-\d{2}$", message_normalized):
                try:
                    datetime.strptime(message_normalized, "%Y-%m-%d")
                    # Start reservation flow and immediately handle date selection
                    self._start_reservation(user_id)
                    return self._handle_date_selection(user_id, message)
                except ValueError:
                    pass
            return self._start_reservation(user_id)
        elif step == "service_selection":
            return self._handle_service_selection(user_id, message)
        elif step == "staff_selection":
            return self._handle_staff_selection(user_id, message)
        elif step == "date_selection":
            return self._handle_date_selection(user_id, message)
        elif step == "time_selection":
            return self._handle_time_selection(user_id, message)
        elif step == "confirmation":
            return self._handle_confirmation(user_id, message)
        else:
            return "予約フローに問題が発生しました。最初からやり直してください。"
    
    def _start_reservation(self, user_id: str) -> str:
        """Start reservation process"""
        self.user_states[user_id]["step"] = "service_selection"
        
        # Generate service list from JSON data
        service_list = []
        for service_id, service_data in self.services.items():
            service_name = service_data.get("name", service_id)
            duration = service_data.get("duration", 60)
            price = service_data.get("price", 3000)
            service_list.append(f"・{service_name}（{duration}分・{price:,}円）")
        
        services_text = "\n".join(service_list)
        
        return f"""ご予約ありがとうございます！
どのサービスをご希望ですか？

{services_text}

サービス名をお送りください。

※予約をキャンセルされる場合は「キャンセル」とお送りください。"""

    def start_reservation_with_service(self, user_id: str, service_identifier: str) -> str:
        """Start a reservation flow with a preselected service (e.g., from a Flex postback)."""
        service_name = self._resolve_service_name(service_identifier)
        if not service_name:
            return "申し訳ございませんが、選択されたメニューは現在ご用意がありません。"

        self.user_states[user_id] = {
            "step": "service_selection",
            "data": {
                "user_id": user_id
            }
        }

        # Reuse the standard handler by sending the resolved service name as input
        return self._handle_service_selection(user_id, service_name)
    
    def _handle_service_selection(self, user_id: str, message: str) -> str:
        """Handle service selection"""
        # Check for flow cancellation first
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"
        
        selected_service = None
        
        # Service matching using JSON data
        for service_id, service_data in self.services.items():
            service_name = service_data.get("name", service_id)
            if service_name.lower() in message_normalized.lower():
                selected_service = service_name
                break
        
        # Also check for English keywords
        if not selected_service:
            service_mapping = {
                "cut": "カット",
                "color": "カラー",
                "perm": "パーマ",
                "treatment": "トリートメント"
            }
            
            for keyword, service_name in service_mapping.items():
                if keyword.lower() in message_normalized.lower():
                    selected_service = service_name
                    break
        
        if not selected_service:
            return "申し訳ございませんが、そのサービスは提供しておりません。上記のサービスからお選びください。"
        
        self.user_states[user_id]["data"]["service"] = selected_service
        
        # Check if there's only one staff member
        if self._has_single_staff():
            # Automatically assign the single staff member
            single_staff_name = self._get_single_staff_name()
            self.user_states[user_id]["data"]["staff"] = single_staff_name
            self.user_states[user_id]["step"] = "date_selection"
            
            return f"""{selected_service}ですね！
担当は{single_staff_name}さんで承ります。

ご希望の日付をお選びください。
📅 **Googleカレンダーで空き状況を確認してください：**
🔗 {self.google_calendar.get_calendar_url()}

💡 **手順：**
1️⃣ 上記リンクをクリックしてGoogleカレンダーを開く
2️⃣ 空いている日付を確認
3️⃣ 希望の日付を「YYYY-MM-DD」形式で送信
📝 例：`2025-01-15`

❌ 予約をキャンセルする場合は「キャンセル」とお送りください。"""
        else:
            # Multiple staff members - show selection
            self.user_states[user_id]["step"] = "staff_selection"
            
            # Generate staff list from JSON data
            staff_list = []
            for staff_id, staff_data in self.staff_members.items():
                staff_name = staff_data.get("name", staff_id)
                specialty = staff_data.get("specialty", "")
                experience = staff_data.get("experience", "")
                staff_list.append(f"・{staff_name}（{specialty}・{experience}）")
            
            staff_text = "\n".join(staff_list)
            
            return f"""{selected_service}ですね！
担当の美容師をお選びください。

{staff_text}

美容師名をお送りください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください。"""
    
    def _handle_staff_selection(self, user_id: str, message: str) -> str:
        """Handle staff selection"""
        # Check for flow cancellation first
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"
        
        # Check for navigation to service selection
        service_change_keywords = self.navigation_keywords.get("service_change", [])
        if any(keyword in message_normalized for keyword in service_change_keywords):
            self.user_states[user_id]["step"] = "service_selection"
            return self._start_reservation(user_id)
        
        selected_staff = None
        message_lower = message.strip().lower()
        
        # Staff matching using direct name matching
        for staff_id, staff_data in self.staff_members.items():
            staff_name = staff_data.get("name", staff_id)
            if staff_name.lower() in message_lower or message_lower in staff_name.lower():
                selected_staff = staff_name
                break
        
        if not selected_staff:
            return "申し訳ございませんが、その美容師は選択できません。上記の美容師からお選びください。"
        
        self.user_states[user_id]["data"]["staff"] = selected_staff
        self.user_states[user_id]["step"] = "date_selection"
        
        # Add "さん" only for specific staff members, not for "未指定"
        staff_display = f"{selected_staff}さん" if selected_staff != "未指定" else selected_staff
        
        # Return calendar template for date selection
        return self._create_calendar_template()
    
    def _handle_date_selection(self, user_id: str, message: str) -> str:
        """Handle date selection from calendar template"""
        # Check for flow cancellation first
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"
        
        # Check for navigation to service selection
        service_change_keywords = self.navigation_keywords.get("service_change", [])
        if any(keyword in message_normalized for keyword in service_change_keywords):
            self.user_states[user_id]["step"] = "service_selection"
            return self._start_reservation(user_id)
        
        # Parse date from user input - only accept YYYY-MM-DD format
        selected_date = None
        
        # Try to parse YYYY-MM-DD format
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', message)
        if date_match:
            selected_date = date_match.group(1)
            # Validate the date format
            try:
                datetime.strptime(selected_date, "%Y-%m-%d")
            except ValueError:
                selected_date = None
        
        if not selected_date:
            return "申し訳ございませんが、日付の形式が正しくありません。\n「YYYY-MM-DD」の形式で入力してください。\n例）2025-01-15"
        
        self.user_states[user_id]["data"]["date"] = selected_date
        self.user_states[user_id]["step"] = "time_selection"
        
        # Get available time periods for selected date from Google Calendar
        staff_name = self.user_states[user_id]["data"].get("staff")
        available_slots = self._get_available_slots(selected_date, staff_name)
        available_periods = [slot for slot in available_slots if slot["available"]]

        # Get service duration
        service_name = self.user_states[user_id]["data"].get("service")
        service_info = {}
        for service_id, service_data in self.services.items():
            if service_data.get("name") == service_name:
                service_info = service_data
                break
        service_duration = service_info.get("duration", 60)  # Default to 60 minutes

        # Filter only periods where service fits
        filtered_periods = []
        for period in available_periods:
            slot_duration = self._calculate_time_duration_minutes(period["time"], period["end_time"])
            if slot_duration >= service_duration:
                filtered_periods.append(period)
        
        if not filtered_periods:
            # No available slots for selected date - return to date selection
            self.user_states[user_id]["step"] = "date_selection"
            # ...existing "no available slot" message...
            return f"""申し訳ございませんが、{selected_date}は{service_name}（{service_duration}分）の予約可能な時間がありません。

他の日付をお選びください。

📅 **Googleカレンダーで空き状況を確認してください：**
🔗 {self.google_calendar.get_calendar_url()}

💡 **手順：**
1️⃣ 上記リンクをクリックしてGoogleカレンダーを開く
2️⃣ 空いている日付を確認
3️⃣ 希望の日付を「YYYY-MM-DD」形式で送信
📝 例：`2025-01-15`

❌ 予約をキャンセルする場合は「キャンセル」と送信"""
        
        # Check if service duration can fit in any available slot
        service_name = self.user_states[user_id]["data"].get("service")
        if service_name:
            # Get service duration
            service_info = {}
            for service_id, service_data in self.services.items():
                if service_data.get("name") == service_name:
                    service_info = service_data
                    break
            
            service_duration = service_info.get("duration", 60)  # Default to 60 minutes
            
            # Check if any slot can accommodate the service duration
            can_accommodate = False
            max_slot_duration = 0
            
            for period in available_periods:
                slot_duration = self._calculate_time_duration_minutes(
                    period["time"], 
                    period["end_time"]
                )
                max_slot_duration = max(max_slot_duration, slot_duration)
                
                if slot_duration >= service_duration:
                    can_accommodate = True
                    break
            
            if not can_accommodate:
                # Service duration is greater than all available slots
                self.user_states[user_id]["step"] = "date_selection"
                
                # Convert duration to readable format
                service_hours = service_duration // 60
                service_minutes = service_duration % 60
                if service_hours > 0 and service_minutes > 0:
                    duration_str = f"{service_hours}時間{service_minutes}分"
                elif service_hours > 0:
                    duration_str = f"{service_hours}時間"
                else:
                    duration_str = f"{service_minutes}分"
                
                max_hours = max_slot_duration // 60
                max_minutes = max_slot_duration % 60
                if max_hours > 0 and max_minutes > 0:
                    max_duration_str = f"{max_hours}時間{max_minutes}分"
                elif max_hours > 0:
                    max_duration_str = f"{max_hours}時間"
                else:
                    max_duration_str = f"{max_minutes}分"
                
                return f"""申し訳ございませんが、{selected_date}の予約可能な時間帯では、{service_name}（{duration_str}）の予約ができません。

📅 選択した日付：{selected_date}
💇 選択したサービス：{service_name}（{duration_str}）
⏱️ この日の最大空き時間：{max_duration_str}

この日付では{service_name}の予約時間が確保できません。

他の日付をお選びください。

📅 **Googleカレンダーで空き状況を確認してください：**
🔗 {self.google_calendar.get_calendar_url()}

💡 **手順：**
1️⃣ 上記リンクをクリックしてGoogleカレンダーを開く
2️⃣ 空いている日付を確認
3️⃣ 希望の日付を「YYYY-MM-DD」形式で送信
📝 例：`2025-01-15`

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""
        
        # Format available periods for display
        period_strings = []
        for period in filtered_periods:
            start_time = period["time"]
            end_time = period["end_time"]
            period_strings.append(f"・{start_time}~{end_time}")
        
        return f"""{selected_date}ですね！
{service_name}（{service_duration}分）の予約可能な時間帯は以下の通りです：

{chr(10).join(period_strings)}

ご希望の開始時間をお送りください。
例）10:00 または 10:30

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""
    
    def _check_advance_booking_time(self, date_str: str, start_time: str) -> tuple:
        """
        Check if the requested booking time is at least 2 hours in advance.
        Returns (is_valid, error_message)
        """
        try:
            from datetime import datetime, timedelta
            import pytz
            
            # Set Tokyo timezone
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            
            # Parse the requested date and time and set to Tokyo timezone
            requested_datetime_naive = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            requested_datetime = tokyo_tz.localize(requested_datetime_naive)
            
            # Get current time in Tokyo timezone
            current_datetime = datetime.now(tokyo_tz)
            
            # Calculate time difference
            time_difference = requested_datetime - current_datetime
            hours_until_booking = time_difference.total_seconds() / 3600
            
            # Check if it's at least 2 hours in advance (with small tolerance for precision)
            if hours_until_booking < 1.99:
                # Calculate how much time is needed
                needed_hours = 2 - hours_until_booking
                needed_minutes = int(needed_hours * 60)
                
                # Handle edge case where needed_minutes is 0
                if needed_minutes <= 0:
                    time_message = "数分"
                elif needed_minutes < 60:
                    time_message = f"{needed_minutes}分"
                else:
                    hours = needed_minutes // 60
                    minutes = needed_minutes % 60
                    if minutes == 0:
                        time_message = f"{hours}時間"
                    else:
                        time_message = f"{hours}時間{minutes}分"
                
                error_message = f"""申し訳ございませんが、ご予約は来店の2時間前までにお取りいただけます。

📅 ご希望の日時：{date_str} {start_time} (東京時間)
⏰ 現在時刻：{current_datetime.strftime('%Y-%m-%d %H:%M')} (東京時間)
⏱️ 必要時間：あと{time_message}お待ちください

2時間以上先の時間帯をご選択ください。"""
                
                return False, error_message
            
            return True, None
            
        except Exception as e:
            logging.error(f"Error checking advance booking time: {e}")
            return False, "時間の確認中にエラーが発生しました。"
    
    def _handle_time_selection(self, user_id: str, message: str) -> str:
        """Handle time selection"""
        # Check for flow cancellation first
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            del self.user_states[user_id]
            return "予約をキャンセルいたします。またのご利用をお待ちしております。"
        
        # Check for navigation to date selection
        date_change_keywords = self.navigation_keywords.get("date_change", [])
        if any(keyword in message_normalized for keyword in date_change_keywords):
            self.user_states[user_id]["step"] = "date_selection"
            return self._create_calendar_template()
        
        selected_date = self.user_states[user_id]["data"]["date"]
        staff_name = self.user_states[user_id]["data"].get("staff")
        print("[Time Selection] :", staff_name, selected_date)
        # Get available slots with better error handling
        try:
            available_slots = self._get_available_slots(selected_date, staff_name)
            available_periods = [slot for slot in available_slots if slot["available"]]

            # Get service duration
            service_name = self.user_states[user_id]["data"].get("service")
            service_info = {}
            for service_id, service_data in self.services.items():
                if service_data.get("name") == service_name:
                    service_info = service_data
                    break
            service_duration = service_info.get("duration", 60)  # Default to 60 minutes

            # Filter only periods where service fits
            filtered_periods = []
            for period in available_periods:
                slot_duration = self._calculate_time_duration_minutes(period["time"], period["end_time"])
                if slot_duration >= service_duration:
                    filtered_periods.append(period)
                
        except Exception as e:
            logging.error(f"Error getting available slots: {e}")
            return f"申し訳ございません。空き時間の取得中にエラーが発生しました。\nスタッフまでお問い合わせください。"

        # Parse start time from user input (only start time needed now)
        start_time = self._parse_single_time(message.strip())
        
        if not start_time:
            # Show available periods in error message
            period_strings = []
            for period in filtered_periods:
                period_start = period["time"]
                period_end = period["end_time"]
                period_strings.append(f"・{period_start}~{period_end}")
            
            return f"""時間の入力形式が正しくありません。

正しい入力例：
・10:00
・10:30
・10時
・10時30分

{selected_date}の予約可能な時間帯：
{chr(10).join(period_strings)}

上記の空き時間から開始時間をお選びください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""

        # Check if the booking time is at least 2 hours in advance
        is_valid_time, time_error_message = self._check_advance_booking_time(selected_date, start_time)
        if not is_valid_time:
            return time_error_message

        # Calculate end time based on service duration
        service_name = self.user_states[user_id]["data"]["service"]
        # Find service by name in the services data
        service_info = {}
        for service_id, service_data in self.services.items():
            if service_data.get("name") == service_name:
                service_info = service_data
                break
        
        required_duration = service_info.get("duration", 60)  # Default to 60 minutes
        
        end_time = self._calculate_optimal_end_time(start_time, required_duration)

        # Validate that the time range falls within available periods
        is_valid_range = False
        matching_period = None
        
        for period in available_periods:
            period_start = period["time"]
            period_end = period["end_time"]
            
            # Debug logging
            print(f"[Time Validation] Checking period: {period_start} - {period_end}")
            print(f"  start_time: {start_time}, end_time: {end_time}")
            print(f"  period_start <= start_time: {period_start} <= {start_time} = {period_start <= start_time}")
            print(f"  end_time <= period_end: {end_time} <= {period_end} = {end_time <= period_end}")
            
            # Check if the entire time range is within this period
            if period_start <= start_time and end_time <= period_end:
                is_valid_range = True
                matching_period = period
                print(f"  ✅ VALID: Time range fits in this period")
                break
            else:
                print(f"  ❌ INVALID: Time range doesn't fit in this period")
        
        if not is_valid_range:
            # Show available periods in error message
            period_strings = []
            for period in available_periods:
                period_start = period["time"]
                period_end = period["end_time"]
                period_strings.append(f"・{period_start}~{period_end}")
            
            return f"""申し訳ございませんが、{start_time}から{required_duration}分の予約は空いていません。

{selected_date}の予約可能な時間帯：
{chr(10).join(period_strings)}

上記の空き時間からお選びください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""
        
        # Check for user time conflict (user can't have multiple reservations at the same time)
        user_time_conflict = self.google_calendar.check_user_time_conflict(
            selected_date, start_time, end_time, user_id
        )
        print("[Time Validation] User ID:", user_id)
        print("[Time Validation] User time conflict:", user_time_conflict)
        if user_time_conflict:
            # Return to time selection with error message
            self.user_states[user_id]["step"] = "time_selection"
            
            # Get available periods again for display
            available_slots = self._get_available_slots(selected_date, staff_name)
            available_periods = [slot for slot in available_slots if slot["available"]]
            
            period_strings = []
            for period in available_periods:
                period_start = period["time"]
                period_end = period["end_time"]
                period_strings.append(f"・{period_start}~{period_end}")
            
            return f"""申し訳ございませんが、{selected_date} {start_time}~{end_time}の時間帯に既に他のご予約が入っています。

お客様は同じ時間帯に複数のご予約をお取りいただけません。

{selected_date}の予約可能な時間帯は以下の通りです：

{chr(10).join(period_strings)}

別の時間を選択してください。

❌ 予約をキャンセルする場合は「キャンセル」とお送りください"""
        
        # Store both start and end times
        print("[Time Validation] Start time:", start_time)
        print("[Time Validation] End time:", end_time)
        print("[Time Validation] User ID:", user_id)
        print("[Time Validation] User states:", self.user_states[user_id])
        self.user_states[user_id]["data"]["start_time"] = start_time
        self.user_states[user_id]["data"]["end_time"] = end_time
        self.user_states[user_id]["data"]["time"] = start_time  # Keep for backward compatibility
        self.user_states[user_id]["step"] = "confirmation"

        print("[Time Validation] User states after storing:", self.user_states[user_id])

        service = self.user_states[user_id]["data"]["service"]
        staff = self.user_states[user_id]["data"]["staff"]

        # Get service info by finding the service ID first
        service_info = {}
        for service_id, service_data in self.services.items():
            if service_data.get("name") == service:
                service_info = service_data
                break
        # Check if end time was automatically adjusted
        original_end_time = self.user_states[user_id]["data"].get("original_end_time")

        print("[Time Validation] Service:", service)
        print("[Time Validation] Staff:", staff)
        print("[Time Validation] Service info:", service_info)
        print("[Time Validation] Original end time:", original_end_time)
        print("[Time Validation] End time:", end_time)
        adjustment_message = ""
        if original_end_time and original_end_time != end_time:
            adjustment_message = f"\n💡 **終了時間を{service}の所要時間に合わせて{end_time}に調整しました**\n"
        
        return f"""予約内容の確認です：{adjustment_message}
📅 日時：{selected_date} {start_time}~{end_time}
💇 サービス：{service}
👨‍💼 担当者：{staff}
⏱️ 所要時間：{service_info['duration']}分
💰 料金：{service_info['price']:,}円

この内容で予約を確定しますか？
「はい」または「確定」とお送りください。

※予約をキャンセルされる場合は「キャンセル」とお送りください。"""

    def _handle_confirmation(self, user_id: str, message: str) -> str:
        """Handle final confirmation"""
        yes_keywords = self.confirmation_keywords.get("yes", [])
        if any(keyword in message for keyword in yes_keywords):
            # Complete the reservation
            reservation_data = self.user_states[user_id]["data"].copy()
            print("reservation_data", reservation_data)
            
            # CRITICAL: Check availability again before confirming to prevent race conditions
            availability_check = self._check_final_availability(reservation_data)
            if not availability_check["available"]:
                # Slot is no longer available - inform user and clear state
                del self.user_states[user_id]
                return f"""❌ 申し訳ございませんが、選択された時間帯は既に他のお客様にご予約いただいておりました。

{availability_check["message"]}

別の時間帯でご予約いただけますでしょうか？
「予約したい」とお送りください。"""
            
            # Generate reservation ID
            reservation_id = self.google_calendar.generate_reservation_id(reservation_data['date'])
            reservation_data['reservation_id'] = reservation_id
            
            # Get client display name
            client_name = self._get_line_display_name(user_id)
            
            # Create calendar event immediately
            calendar_success = self.google_calendar.create_reservation_event(
                reservation_data, 
                client_name
            )
            
            if not calendar_success:
                logging.warning(f"Failed to create calendar event for user {user_id}")
           
            # Save reservation to Google Sheets Reservations sheet
            sheets_success = False
            try:
                from api.google_sheets_logger import GoogleSheetsLogger
                sheets_logger = GoogleSheetsLogger()
                
                # Prepare reservation data for Google Sheets
                service_name = reservation_data['service']
                service_info = {}
                for service_id, service_data in self.services.items():
                    if service_data.get("name") == service_name:
                        service_info = service_data
                        break
                sheet_reservation_data = {
                    "reservation_id": reservation_id,
                    "user_id": user_id,  # Add user ID for reminder system
                    "client_name": client_name,
                    "date": reservation_data['date'],
                    "start_time": reservation_data.get('start_time', reservation_data.get('time', '')),
                    "end_time": reservation_data.get('end_time', ''),
                    "service": reservation_data['service'],
                    "staff": reservation_data['staff'],
                    "duration": service_info.get('duration', 60),
                    "price": service_info.get('price', 0)
                }
                
                sheets_success = sheets_logger.save_reservation(sheet_reservation_data)
                if sheets_success:
                    print(f"Successfully saved reservation {reservation_id} to Reservations sheet")
                else:
                    logging.error(f"Failed to save reservation {reservation_id} to Reservations sheet")
                    
            except Exception as e:
                logging.error(f"Error saving reservation to Google Sheets: {e}")
                import traceback
                traceback.print_exc()
            
            # Send notification for reservation confirmation
            try:
                from api.notification_manager import send_reservation_confirmation_notification
                send_reservation_confirmation_notification(reservation_data, client_name)
            except Exception as e:
                logging.error(f"Failed to send reservation confirmation notification: {e}")
            
            # Keep reservation data in user state for logging in index.py
            # The user state will be cleared after logging in index.py
            self.user_states[user_id]["data"] = reservation_data
           
            # Get time range for display
            time_display = reservation_data.get('start_time', reservation_data['time'])
            if 'end_time' in reservation_data:
                time_display = f"{reservation_data['start_time']}~{reservation_data['end_time']}"
           
            return f"""✅ 予約が確定いたしました！

🆔 予約ID：{reservation_id}
📅 日時：{reservation_data['date']} {time_display}
💇 サービス：{reservation_data['service']}
👨‍💼 担当者：{reservation_data['staff']}
💰 料金：{service_info.get('price', 0):,}円

当日はお時間までにお越しください。
ご予約ありがとうございました！"""
    
    def _check_final_availability(self, reservation_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if the reservation slot is still available before final confirmation.
        This prevents race conditions when multiple users try to book the same slot.
        """
        try:
            date_str = reservation_data['date']
            start_time = reservation_data.get('start_time', reservation_data.get('time', ''))
            end_time = reservation_data.get('end_time', '')
            staff_name = reservation_data['staff']
            user_id = reservation_data.get('user_id', '')
            
            # If no end_time, calculate it from service duration
            if not end_time:
                service_name = reservation_data['service']
                service_info = {}
                for service_id, service_data in self.services.items():
                    if service_data.get("name") == service_name:
                        service_info = service_data
                        break
                duration = service_info.get('duration', 60)  # Default 60 minutes
                start_dt = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
                end_dt = start_dt + timedelta(minutes=duration)
                end_time = end_dt.strftime("%H:%M")
            
            # Check staff availability for the time slot
            staff_available = self.google_calendar.check_staff_availability_for_time(
                date_str, start_time, end_time, staff_name
            )
            
            if not staff_available:
                return {
                    "available": False,
                    "message": f"👨‍💼 {staff_name}さんの{start_time}~{end_time}の時間帯は既に予約が入っております。"
                }
            
            # Check if user has another reservation at the same time
            user_conflict = self.google_calendar.check_user_time_conflict(
                date_str, start_time, end_time, user_id
            )
            
            if user_conflict:
                return {
                    "available": False,
                    "message": f"⚠️ 同じ時間帯に他のご予約がございます。"
                }
            
            return {"available": True, "message": ""}
            
        except Exception as e:
            logging.error(f"Error checking final availability: {e}")
            # If there's an error, assume it's available to avoid blocking legitimate reservations
            return {"available": True, "message": ""}
    
    def _check_modification_availability(self, reservation: Dict[str, Any], pending_modification: Dict[str, Any], modification_type: str) -> Dict[str, Any]:
        """
        Check if the modification target slot is still available before final confirmation.
        This prevents race conditions when multiple users try to modify to the same slot.
        """
        try:
            if modification_type == "time":
                # Check time modification availability
                new_date = pending_modification.get("new_date", reservation["date"])
                new_time = pending_modification.get("new_time", "")
                
                if not new_time:
                    return {"available": True, "message": ""}
                
                # Parse time range
                start_time, end_time = self._parse_time_range(new_time)
                if not start_time or not end_time:
                    return {"available": True, "message": ""}
                
                # Check staff availability for the new time slot
                staff_available = self.google_calendar.check_staff_availability_for_time(
                    new_date, start_time, end_time, reservation["staff"], reservation["reservation_id"]
                )
                
                if not staff_available:
                    return {
                        "available": False,
                        "message": f"👨‍💼 {reservation['staff']}さんの{start_time}~{end_time}の時間帯は既に予約が入っております。"
                    }
                
                # Check if user has another reservation at the same time
                user_conflict = self.google_calendar.check_user_time_conflict(
                    new_date, start_time, end_time, reservation.get("user_id", ""), reservation["reservation_id"]
                )
                
                if user_conflict:
                    return {
                        "available": False,
                        "message": f"⚠️ 同じ時間帯に他のご予約がございます。"
                    }
                
            elif modification_type == "service":
                # Check service modification availability
                new_service = pending_modification.get("new_service", "")
                if not new_service:
                    return {"available": True, "message": ""}
                
                # Check if service change causes overlap
                is_available, new_end_time, conflict_details = self.google_calendar.check_service_change_overlap(
                    reservation["date"], reservation["start_time"], new_service, reservation["staff"], reservation["reservation_id"]
                )
                
                if not is_available:  # is_available=False means there IS an overlap
                    conflict_message = ""
                    if conflict_details:
                        conflict_message = conflict_details.get('message', '')
                    return {
                        "available": False,
                        "message": f"⚠️ {new_service}に変更すると時間が重複します。{conflict_message}"
                    }
                
            elif modification_type == "staff":
                # Check staff modification availability
                new_staff = pending_modification.get("new_staff", "")
                if not new_staff:
                    return {"available": True, "message": ""}
                
                # Check if new staff is available at the same time
                staff_available = self.google_calendar.check_staff_availability_for_time(
                    reservation["date"], reservation["start_time"], reservation["end_time"], new_staff, reservation["reservation_id"]
                )
                
                if not staff_available:
                    return {
                        "available": False,
                        "message": f"👨‍💼 {new_staff}さんの{reservation['start_time']}~{reservation['end_time']}の時間帯は既に予約が入っております。"
                    }
            
            return {"available": True, "message": ""}
            
        except Exception as e:
            logging.error(f"Error checking modification availability: {e}")
            # If there's an error, assume it's available to avoid blocking legitimate modifications
            return {"available": True, "message": ""}
    
    def get_response(self, user_id: str, message: str) -> str:
        """Main entry point for reservation flow"""
        intent = self.detect_intent(message, user_id)
        
        if intent == "reservation":
            return self.handle_reservation_flow(user_id, message)
        elif intent == "reservation_flow":
            return self.handle_reservation_flow(user_id, message)
        elif intent == "modify":
            return self._handle_modify_request(user_id, message)
        elif intent == "cancel":
            return self._handle_cancel_request(user_id, message)
        else:
            return None 

    def set_line_configuration(self, configuration):
        """Set LINE configuration for getting display names"""
        self.line_configuration = configuration
    
    def _get_line_display_name(self, user_id: str) -> str:
        """Get LINE display name for the user"""
        if not self.line_configuration:
            return "お客様"  # Fallback name
        
        try:
            from linebot.v3.messaging import ApiClient, MessagingApi
            with ApiClient(self.line_configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                profile = line_bot_api.get_profile(user_id)
                return profile.display_name
        except Exception as e:
            logging.error(f"Failed to get LINE display name: {e}")
            return "お客様"  # Fallback name

    def _handle_cancel_request(self, user_id: str, message: str = None) -> str:
        """Handle reservation cancellation with reservation selection"""
        state = self.user_states.get(user_id)
        
        # Check for cancellation of the cancel flow
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        if message:
            message_normalized = message.strip()
            if any(keyword in message_normalized for keyword in flow_cancel_keywords):
                if user_id in self.user_states:
                    del self.user_states[user_id]
                return "キャンセルをキャンセルいたします。またのご利用をお待ちしております。"
        
        # Step 1: Start cancellation flow - show user's reservations
        if not state or state.get("step") not in ["cancel_select_reservation", "cancel_confirm"]:
            self.user_states[user_id] = {"step": "cancel_select_reservation"}
            return self._show_user_reservations_for_cancellation(user_id)
        
        # Step 2: Handle reservation selection
        elif state.get("step") == "cancel_select_reservation":
            return self._handle_cancel_reservation_selection(user_id, message)
        
        # Step 3: Handle confirmation
        elif state.get("step") == "cancel_confirm":
            return self._handle_cancel_confirmation(user_id, message)
        
        return "キャンセルフローに問題が発生しました。最初からやり直してください。"
    
    def _show_user_reservations_for_cancellation(self, user_id: str) -> str:
        """Show user's reservations for cancellation selection"""
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            import pytz
            
            sheets_logger = GoogleSheetsLogger()
            client_name = self._get_line_display_name(user_id)
            
            # Get user's reservations
            reservations = sheets_logger.get_user_reservations(client_name)
            print(f"Found {len(reservations) if reservations else 0} reservations for client: {client_name}")
            
            if not reservations:
                return "申し訳ございませんが、あなたの予約が見つかりませんでした。\nスタッフまでお問い合わせください。"
            
            # Filter out past reservations by comparing with current time
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_time = datetime.now(tokyo_tz)
            future_reservations = []
            
            for res in reservations:
                try:
                    # Parse reservation date and start time
                    reservation_date = res.get('date', '')
                    reservation_start_time = res.get('start_time', '')
                    
                    if not reservation_date or not reservation_start_time:
                        # Skip reservations without date or time
                        continue
                    
                    # Parse datetime in Tokyo timezone
                    reservation_datetime_naive = datetime.strptime(
                        f"{reservation_date} {reservation_start_time}", 
                        "%Y-%m-%d %H:%M"
                    )
                    reservation_datetime = tokyo_tz.localize(reservation_datetime_naive)
                    
                    # Only include future reservations
                    if reservation_datetime > current_time:
                        future_reservations.append(res)
                        
                except (ValueError, TypeError) as e:
                    # Skip reservations with invalid date/time format
                    logging.warning(f"Skipping reservation with invalid date/time: {res.get('reservation_id', 'Unknown')} - {e}")
                    continue
            
            if not future_reservations:
                return "申し訳ございませんが、今後予定されている予約が見つかりませんでした。\n過去の予約はキャンセルできません。"
            
            # Store only future reservations for selection
            self.user_states[user_id]["user_reservations"] = future_reservations
            
            # Create reservation list (show max 5 future reservations)
            reservation_list = []
            for i, res in enumerate(future_reservations[:5], 1):
                reservation_list.append(f"{i}️⃣ {res['date']} {res['start_time']}~{res['end_time']} - {res['service']} ({res['reservation_id']})")
            
            return f"""ご予約のキャンセルですね。

あなたの予約一覧：

{chr(10).join(reservation_list)}

キャンセルしたい予約の番号を入力してください。

または、予約IDを直接入力することもできます。
例）RES-20250115-0001

❌ 取り消しをやめる場合は「キャンセル」とお送りください"""
            
        except Exception as e:
            logging.error(f"Failed to show user reservations for cancellation: {e}")
            return "申し訳ございません。予約検索中にエラーが発生しました。スタッフまでお問い合わせください。"
    
    def _handle_cancel_reservation_selection(self, user_id: str, message: str) -> str:
        """Handle reservation selection for cancellation"""
        state = self.user_states[user_id]
        reservations = state["user_reservations"]
        
        try:
            # Check if message is a reservation ID
            if re.match(r"^RES-\d{8}-\d{4}$", message):
                reservation_id = message
                # Find the reservation
                selected_reservation = None
                print(f"Looking for reservation ID: {reservation_id}")
                print(f"Available reservations: {[res['reservation_id'] for res in reservations]}")
                for res in reservations:
                    if res["reservation_id"] == reservation_id:
                        selected_reservation = res
                        break
                
                if selected_reservation:
                    # Store selected reservation and move to confirmation
                    self.user_states[user_id]["selected_reservation"] = selected_reservation
                    self.user_states[user_id]["step"] = "cancel_confirm"
                    
                    # Get Google Calendar URL
                    calendar_url = self.google_calendar.get_calendar_url()
                    
                    return f"""キャンセルする予約を確認してください：

📋 予約内容：
🆔 予約ID：{selected_reservation['reservation_id']}
📅 日時：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
💇 サービス：{selected_reservation['service']}
👨‍💼 担当者：{selected_reservation['staff']}

🗓️ **Googleカレンダーで予約状況を確認：**
🔗 {calendar_url}

この予約をキャンセルしますか？
「はい」または「確定」とお送りください。

❌ 取り消しをやめる場合は「キャンセル」とお送りください。"""
                else:
                    return "申し訳ございませんが、その予約IDが見つからないか、あなたの予約ではありません。\n正しい予約IDまたは番号を入力してください。"
            
            # Check if message is a number (reservation selection)
            elif message.isdigit():
                reservation_index = int(message) - 1
                if 0 <= reservation_index < len(reservations):
                    selected_reservation = reservations[reservation_index]
                    
                    # Store selected reservation and move to confirmation
                    self.user_states[user_id]["selected_reservation"] = selected_reservation
                    self.user_states[user_id]["step"] = "cancel_confirm"
                    
                    # Get Google Calendar URL
                    calendar_url = self.google_calendar.get_calendar_url()
                    
                    return f"""キャンセルする予約を確認してください：

📋 予約内容：
🆔 予約ID：{selected_reservation['reservation_id']}
📅 日時：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
💇 サービス：{selected_reservation['service']}
👨‍💼 担当者：{selected_reservation['staff']}

🗓️ **Googleカレンダーで予約状況を確認：**
🔗 {calendar_url}

この予約をキャンセルしますか？
「はい」または「確定」とお送りください。

❌ 取り消しをやめる場合は「キャンセル」とお送りください。"""
                else:
                    return f"申し訳ございませんが、その番号は選択できません。\n1から{len(reservations)}の番号を入力してください。"
            else:
                return f"申し訳ございませんが、正しい形式で入力してください。\n番号（1-{len(reservations)}）または予約ID（RES-YYYYMMDD-XXXX）を入力してください。"
                
        except Exception as e:
            logging.error(f"Reservation selection for cancellation failed: {e}")
            return "申し訳ございません。予約選択中にエラーが発生しました。スタッフまでお問い合わせください。"
    
    def _handle_cancel_confirmation(self, user_id: str, message: str) -> str:
        """Handle cancellation confirmation"""
        state = self.user_states[user_id]
        reservation = state["selected_reservation"]
        
        # Check for confirmation keywords
        yes_keywords = self.confirmation_keywords.get("yes", [])
        no_keywords = self.confirmation_keywords.get("no", [])
        
        if any(keyword in message for keyword in yes_keywords):
            # Execute cancellation
            return self._execute_reservation_cancellation(user_id, reservation)
        elif any(keyword in message for keyword in no_keywords):
            # Cancel the cancellation
            del self.user_states[user_id]
            return "キャンセルをキャンセルいたします。予約はそのまま残ります。\nまたのご利用をお待ちしております。"
        else:
            return "「はい」または「確定」でキャンセルを確定するか、「キャンセル」で中止してください。"
    
    def _execute_reservation_cancellation(self, user_id: str, reservation: Dict) -> str:
        """Execute the actual reservation cancellation"""
        # Check if cancellation is within 2 hours of reservation start time
        try:
            from datetime import datetime, timedelta
            import pytz
            
            # Get current time in Tokyo timezone
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_time = datetime.now(tokyo_tz)
            
            # Parse reservation date and start time
            reservation_date = reservation["date"]
            reservation_start_time = reservation["start_time"]
            
            # Create reservation datetime in Tokyo timezone
            reservation_datetime = datetime.strptime(f"{reservation_date} {reservation_start_time}", "%Y-%m-%d %H:%M")
            reservation_datetime = tokyo_tz.localize(reservation_datetime)
            
            # Calculate time difference
            time_diff = reservation_datetime - current_time
            
            # Check if within 2 hours (120 minutes)
            if time_diff.total_seconds() <= 7200:  # 2 hours = 7200 seconds
                return f"""申し訳ございませんが、予約開始時刻の2時間以内のキャンセルはお受けできません。

📅 予約日時：{reservation_date} {reservation_start_time}
⏰ 現在時刻：{current_time.strftime('%Y-%m-%d %H:%M')}
⏱️ 残り時間：{int(time_diff.total_seconds() / 3600)}時間{int((time_diff.total_seconds() % 3600) / 60)}分

緊急の場合は直接サロンまでお電話ください。"""
            
        except Exception as e:
            logging.error(f"Error checking cancellation time limit: {e}")
            # Continue with cancellation if time check fails
        
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            sheets_logger = GoogleSheetsLogger()
            
            reservation_id = reservation["reservation_id"]
            
            # Update status in Google Sheets to "Cancelled"
            sheets_success = sheets_logger.update_reservation_status(reservation_id, "Cancelled")
            
            if not sheets_success:
                return "申し訳ございません。キャンセル処理中にエラーが発生しました。\nスタッフまでお問い合わせください。"
            
            # Remove from Google Calendar (use staff name from reservation data)
            staff_name = reservation.get("staff")
            calendar_success = self.google_calendar.cancel_reservation_by_id(reservation_id, staff_name)
            
            if not calendar_success:
                logging.warning(f"Failed to remove reservation {reservation_id} from Google Calendar")
            
            # Send notification for reservation cancellation
            try:
                from api.notification_manager import send_reservation_cancellation_notification
                client_name = self._get_line_display_name(user_id)
                send_reservation_cancellation_notification(reservation, client_name)
            except Exception as e:
                logging.error(f"Failed to send reservation cancellation notification: {e}")
            
            # Clear user state
            del self.user_states[user_id]
            
            return f"""✅ 予約のキャンセルが完了しました！

📋 キャンセル内容：
🆔 予約ID：{reservation_id}
📅 日時：{reservation['date']} {reservation['start_time']}~{reservation['end_time']}
💇 サービス：{reservation['service']}
👨‍💼 担当者：{reservation['staff']}

またのご利用をお待ちしております。"""
                
        except Exception as e:
            logging.error(f"Reservation cancellation execution failed: {e}")
            return "申し訳ございません。キャンセル処理中にエラーが発生しました。\nスタッフまでお問い合わせください。"

    def _handle_reservation_id_cancellation(self, user_id: str, reservation_id: str) -> str:
        """Handle direct reservation cancellation by ID"""
        try:
            # Update status in Google Sheets to "Cancelled"
            from api.google_sheets_logger import GoogleSheetsLogger
            sheets_logger = GoogleSheetsLogger()
            sheets_success = sheets_logger.update_reservation_status(reservation_id, "Cancelled")
            
            if not sheets_success:
                return "申し訳ございません。キャンセル処理中にエラーが発生しました。\nスタッフまでお問い合わせください。"
            
            # Remove from Google Calendar
            # Note: For direct reservation ID cancellation, we need to search all calendars
            # The get_reservation_by_id method will handle this
            calendar_success = self.google_calendar.cancel_reservation_by_id(reservation_id)
            
            if not calendar_success:
                logging.warning(f"Failed to remove reservation {reservation_id} from Google Calendar")
            
            return f"""✅ 予約のキャンセルが完了しました！

📋 キャンセル内容：
• 予約ID：{reservation_id}

またのご利用をお待ちしております。"""
                
        except Exception as e:
            logging.error(f"Reservation ID cancellation failed: {e}")
            return "申し訳ございません。キャンセル処理中にエラーが発生しました。\nスタッフまでお問い合わせください。"


    def _normalize_time_format(self, time_str: str) -> str:
        """Normalize time string to HH:MM format (zero-padded)"""
        try:
            # Check if already normalized (starts with 0 or is 10+)
            parts = time_str.split(':')
            if len(parts) == 2:
                hour_part = parts[0]
                minute_part = parts[1]
                
                # Validate the format
                if len(minute_part) != 2 or not minute_part.isdigit():
                    return None
                
                # Normalize hour part
                if len(hour_part) == 1:
                    # Single digit hour, pad with zero
                    normalized_hour = f"0{hour_part}"
                elif len(hour_part) == 2 and hour_part.isdigit():
                    # Already two digits
                    normalized_hour = hour_part
                else:
                    return None
                
                # Validate the normalized time
                normalized_time = f"{normalized_hour}:{minute_part}"
                datetime.strptime(normalized_time, "%H:%M")
                return normalized_time
            else:
                return None
        except (ValueError, IndexError):
            return None

    def _parse_time_range(self, text: str) -> tuple:
        """Parse start and end times from user input.
        Returns tuple of (start_time, end_time) in HH:MM format, or (None, None) if invalid.
        Supports various time formats including single/double digit hours and Japanese format.
        """
        text = text.strip()
        
        # Pattern 1: "10:00~11:00" or "10:00～11:00" or "9:00~10:00"
        match = re.search(r'^(\d{1,2}:\d{2})[~～](\d{1,2}:\d{2})$', text)
        if match:
            start_time = self._normalize_time_format(match.group(1))
            end_time = self._normalize_time_format(match.group(2))
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 2: "10:00 11:00" (space separated) or "9:00 10:00"
        match = re.search(r'^(\d{1,2}:\d{2})\s+(\d{1,2}:\d{2})$', text)
        if match:
            start_time = self._normalize_time_format(match.group(1))
            end_time = self._normalize_time_format(match.group(2))
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 3: "9~12" or "9～12" (hour only, assumes :00 minutes)
        match = re.search(r'^(\d{1,2})[~～](\d{1,2})$', text)
        if match:
            start_hour = match.group(1)
            end_hour = match.group(2)
            start_time = self._normalize_time_format(f"{start_hour}:00")
            end_time = self._normalize_time_format(f"{end_hour}:00")
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 4: "9 12" (space separated, hour only)
        match = re.search(r'^(\d{1,2})\s+(\d{1,2})$', text)
        if match:
            start_hour = match.group(1)
            end_hour = match.group(2)
            start_time = self._normalize_time_format(f"{start_hour}:00")
            end_time = self._normalize_time_format(f"{end_hour}:00")
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 5: "9時~12時" or "9時～12時" (Japanese format)
        match = re.search(r'^(\d{1,2})時[~～](\d{1,2})時$', text)
        if match:
            start_hour = match.group(1)
            end_hour = match.group(2)
            start_time = self._normalize_time_format(f"{start_hour}:00")
            end_time = self._normalize_time_format(f"{end_hour}:00")
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 6: "9時 12時" (Japanese format, space separated)
        match = re.search(r'^(\d{1,2})時\s+(\d{1,2})時$', text)
        if match:
            start_hour = match.group(1)
            end_hour = match.group(2)
            start_time = self._normalize_time_format(f"{start_hour}:00")
            end_time = self._normalize_time_format(f"{end_hour}:00")
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 7: Mixed formats like "9:00~12" or "9:30~15"
        match = re.search(r'^(\d{1,2}:\d{2})[~～](\d{1,2})$', text)
        if match:
            start_time = self._normalize_time_format(match.group(1))
            end_hour = match.group(2)
            end_time = self._normalize_time_format(f"{end_hour}:00")
            if start_time and end_time:
                return start_time, end_time
        
        # Pattern 8: Mixed formats like "9:00 12" or "9:30 15"
        match = re.search(r'^(\d{1,2}:\d{2})\s+(\d{1,2})$', text)
        if match:
            start_time = self._normalize_time_format(match.group(1))
            end_hour = match.group(2)
            end_time = self._normalize_time_format(f"{end_hour}:00")
            if start_time and end_time:
                return start_time, end_time
        
        return None, None

    def _parse_single_time(self, text: str) -> str:
        """Parse a single time from user input.
        Returns time in HH:MM format, or None if invalid.
        Supports various time formats including single/double digit hours and Japanese format.
        """
        text = text.strip()
        
        # Pattern 1: "10:00" or "9:30" (standard format)
        match = re.search(r'^(\d{1,2}:\d{2})$', text)
        if match:
            return self._normalize_time_format(match.group(1))
        
        # Pattern 2: "10" or "9" (hour only, assumes :00 minutes)
        match = re.search(r'^(\d{1,2})$', text)
        if match:
            hour = match.group(1)
            return self._normalize_time_format(f"{hour}:00")
        
        # Pattern 3: "10時" or "9時" (Japanese format, hour only)
        match = re.search(r'^(\d{1,2})時$', text)
        if match:
            hour = match.group(1)
            return self._normalize_time_format(f"{hour}:00")
        
        # Pattern 4: "10時30分" or "9時15分" (Japanese format with minutes)
        match = re.search(r'^(\d{1,2})時(\d{1,2})分$', text)
        if match:
            hour = match.group(1)
            minute = match.group(2)
            return self._normalize_time_format(f"{hour}:{minute}")
        
        return None

    def _handle_modify_request(self, user_id: str, message: str) -> str:
        """Handle comprehensive reservation modification with enhanced features"""
        state = self.user_states.get(user_id)
        
        # Check for cancellation
        flow_cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        message_normalized = message.strip()
        if any(keyword in message_normalized for keyword in flow_cancel_keywords):
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "予約変更をキャンセルいたします。またのご利用をお待ちしております。"
        
        # Step 1: Start modification flow - show user's reservations
        if not state or state.get("step") not in ["modify_select_reservation", "modify_select_field", "modify_time_date_select", "modify_time_input_date", "modify_time_select", "modify_confirm", "modify_staff_select", "modify_service_select", "modify_re_reservation_confirm"]:
            self.user_states[user_id] = {"step": "modify_select_reservation"}
            return self._show_user_reservations_for_modification(user_id)
        
        # Step 2: Handle reservation selection
        if state.get("step") == "modify_select_reservation":
            return self._handle_modify_reservation_selection(user_id, message)
        
        # Step 3: Handle field selection
        elif state.get("step") == "modify_select_field":
            print(f"Routing to field selection - User: {user_id}, Message: '{message}'")
            return self._handle_field_selection(user_id, message)
        
        # Step 4: Handle time modification date selection
        elif state.get("step") == "modify_time_date_select":
            return self._handle_time_date_selection(user_id, message)
        
        # Step 5: Handle time modification new date input
        elif state.get("step") == "modify_time_input_date":
            return self._handle_time_input_date(user_id, message)
        
        # Step 6: Handle time selection for modification
        elif state.get("step") == "modify_time_select":
            return self._handle_time_selection_for_modification(user_id, message)
        
        # Step 7: Handle staff selection for modification
        elif state.get("step") == "modify_staff_select":
            return self._handle_staff_selection_for_modification(user_id, message)
        
        # Step 8: Handle service selection for modification
        elif state.get("step") == "modify_service_select":
            return self._handle_service_selection_for_modification(user_id, message)
        
        # Step 9: Handle confirmation
        elif state.get("step") == "modify_confirm":
            return self._handle_modification_confirmation(user_id, message)
        
        # Step 10: Handle re-reservation confirmation
        elif state.get("step") == "modify_re_reservation_confirm":
            return self._handle_re_reservation_confirmation(user_id, message)
        
        return "予約変更フローに問題が発生しました。最初からやり直してください。"
    
    def _show_user_reservations_for_modification(self, user_id: str) -> str:
        """Show user's reservations for modification selection"""
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            import pytz
            
            sheets_logger = GoogleSheetsLogger()
            client_name = self._get_line_display_name(user_id)
            
            # Get user's reservations
            reservations = sheets_logger.get_user_reservations(client_name)
            
            if not reservations:
                return "申し訳ございませんが、あなたの予約が見つかりませんでした。\nスタッフまでお問い合わせください。"
            
            # Filter out past reservations by comparing with current time
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_time = datetime.now(tokyo_tz)
            future_reservations = []
            
            for res in reservations:
                try:
                    # Parse reservation date and start time
                    reservation_date = res.get('date', '')
                    reservation_start_time = res.get('start_time', '')
                    
                    if not reservation_date or not reservation_start_time:
                        # Skip reservations without date or time
                        continue
                    
                    # Parse datetime in Tokyo timezone
                    reservation_datetime_naive = datetime.strptime(
                        f"{reservation_date} {reservation_start_time}", 
                        "%Y-%m-%d %H:%M"
                    )
                    reservation_datetime = tokyo_tz.localize(reservation_datetime_naive)
                    
                    # Only include future reservations
                    if reservation_datetime > current_time:
                        future_reservations.append(res)
                        
                except (ValueError, TypeError) as e:
                    # Skip reservations with invalid date/time format
                    logging.warning(f"Skipping reservation with invalid date/time: {res.get('reservation_id', 'Unknown')} - {e}")
                    continue
            
            if not future_reservations:
                return "申し訳ございませんが、今後予定されている予約が見つかりませんでした。\n過去の予約は変更できません。"
            
            # Store only future reservations for selection
            self.user_states[user_id]["user_reservations"] = future_reservations
            
            # Create reservation list (show max 5 future reservations)
            reservation_list = []
            for i, res in enumerate(future_reservations[:5], 1):
                reservation_list.append(f"{i}️⃣ {res['date']} {res['start_time']}~{res['end_time']} - {res['service']} ({res['reservation_id']})")
            
            return f"""ご予約の変更ですね。

あなたの予約一覧：

{chr(10).join(reservation_list)}

変更したい予約の番号を入力してください。

または、予約IDを直接入力することもできます。
例）RES-20250115-0001

変更をやめる場合は「キャンセル」とお送りください。"""
            
        except Exception as e:
            logging.error(f"Failed to show user reservations for modification: {e}")
            return "申し訳ございません。予約検索中にエラーが発生しました。スタッフまでお問い合わせください。"
    
    def _handle_modify_reservation_selection(self, user_id: str, message: str) -> str:
        """Handle reservation selection for modification"""
        state = self.user_states[user_id]
        reservations = state["user_reservations"]
        
        try:
            # Check if message is a reservation ID
            if re.match(r"^RES-\d{8}-\d{4}$", message):
                reservation_id = message
                # Find the reservation
                selected_reservation = None
                print(f"Looking for reservation ID: {reservation_id}")
                print(f"Available reservations: {[res['reservation_id'] for res in reservations]}")
                for res in reservations:
                    if res["reservation_id"] == reservation_id:
                        selected_reservation = res
                        break
                
                if selected_reservation:
                    # Store selected reservation and move to field selection
                    self.user_states[user_id]["reservation_data"] = selected_reservation
                    self.user_states[user_id]["step"] = "modify_select_field"
                    
                    # Get Google Calendar URL
                    calendar_url = self.google_calendar.get_calendar_url()
                    
                    return f"""予約が見つかりました！

📋 現在の予約内容：
🆔 予約ID：{selected_reservation['reservation_id']}
📅 日時：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
💇 サービス：{selected_reservation['service']}
👨‍💼 担当者：{selected_reservation['staff']}

🗓️ **Googleカレンダーで予約状況を確認：**
🔗 {calendar_url}

何を変更しますか？
{self._get_modification_menu()}

変更をやめる場合は「キャンセル」とお送りください。"""
                else:
                    return "申し訳ございませんが、その予約IDが見つからないか、あなたの予約ではありません。\n正しい予約IDまたは番号を入力してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
            
            # Check if message is a number (reservation selection)
            elif message.isdigit():
                reservation_index = int(message) - 1
                if 0 <= reservation_index < len(reservations):
                    selected_reservation = reservations[reservation_index]
                    
                    # Store selected reservation and move to field selection
                    self.user_states[user_id]["reservation_data"] = selected_reservation
                    self.user_states[user_id]["step"] = "modify_select_field"
                    
                    # Get Google Calendar URL
                    calendar_url = self.google_calendar.get_calendar_url()
                    
                    return f"""予約が見つかりました！

📋 現在の予約内容：
🆔 予約ID：{selected_reservation['reservation_id']}
📅 日時：{selected_reservation['date']} {selected_reservation['start_time']}~{selected_reservation['end_time']}
💇 サービス：{selected_reservation['service']}
👨‍💼 担当者：{selected_reservation['staff']}

🗓️ **Googleカレンダーで予約状況を確認：**
🔗 {calendar_url}

何を変更しますか？
{self._get_modification_menu()}

変更をやめる場合は「キャンセル」とお送りください。"""
                else:
                    return f"申し訳ございませんが、その番号は選択できません。\n1から{len(reservations)}の番号を入力してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
            else:
                return f"申し訳ございませんが、正しい形式で入力してください。\n番号（1-{len(reservations)}）または予約ID（RES-YYYYMMDD-XXXX）を入力してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
                
        except Exception as e:
            logging.error(f"Reservation selection for modification failed: {e}")
            return "申し訳ございません。予約選択中にエラーが発生しました。スタッフまでお問い合わせください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
    
    def _handle_field_selection(self, user_id: str, message: str) -> str:
        """Handle field selection for modification"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        print(f"Field selection - User: {user_id}, Message: '{message}', State: {state}")
        
        # Check for numeric selection first
        if message.strip() == "1":
            print("Selected time modification (1)")
            return self._handle_time_modification(user_id, message)
        elif message.strip() == "2":
            print("Selected service modification (2)")
            return self._handle_service_modification(user_id, message)
        elif message.strip() == "3":
            # Check if staff modification is available
            if self._has_single_staff():
                print("Selected re-reservation (3 - staff not available)")
                return self._handle_re_reservation(user_id, message)
            else:
                print("Selected staff modification (3)")
                return self._handle_staff_modification(user_id, message)
        elif message.strip() == "4":
            # Only available when staff modification is available
            if not self._has_single_staff():
                print("Selected re-reservation (4)")
                return self._handle_re_reservation(user_id, message)
        
        # Only numeric selection is supported
        return f"申し訳ございませんが、正しい番号を入力してください。\n\n{self._get_modification_menu()}\n\n変更をやめる場合は「キャンセル」とお送りください。"
    
    def _handle_re_reservation(self, user_id: str, message: str) -> str:
        """Handle re-reservation option - cancel current reservation and start new reservation"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        print(f"Re-reservation selected - User: {user_id}, Reservation: {reservation['reservation_id']}")
        
        # Set step to re-reservation confirmation
        self.user_states[user_id]["step"] = "modify_re_reservation_confirm"
        
        # Show explanation and ask for confirmation
        return f"""複数項目の変更をご希望ですね。

現在の予約をキャンセルして、新しい予約を作成していただく方法をご案内いたします。

📋 **現在の予約内容：**
🆔 予約ID：{reservation['reservation_id']}
📅 日時：{reservation['date']} {reservation['start_time']}~{reservation['end_time']}
💇 サービス：{reservation['service']}
👨‍💼 担当者：{reservation['staff']}

⚠️ **注意事項：**
• 現在の予約を自動的にキャンセルいたします
• キャンセル後、新しい予約を作成していただきます
• 複数の項目（日時・サービス・担当者）を自由に変更できます

この方法で進めてもよろしいですか？

「はい」または「確定」と入力してください。
キャンセルする場合は「キャンセル」と入力してください。

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_re_reservation_confirmation(self, user_id: str, message: str) -> str:
        """Handle re-reservation confirmation - cancel current reservation and start new reservation flow"""
        state = self.user_states.get(user_id)
        if not state:
            return "申し訳ございません。セッションが切れました。最初からやり直してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        reservation = state.get("reservation_data")
        if not reservation:
            return "申し訳ございません。予約データが見つかりません。最初からやり直してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        print(f"Re-reservation confirmation - User: {user_id}, Message: '{message}'")
        
        # Check for confirmation
        message_normalized = message.strip().lower()
        if message_normalized in ["はい", "確定", "yes", "ok"]:
            # Cancel the current reservation
            try:
                from api.google_sheets_logger import GoogleSheetsLogger
                from api.notification_manager import notification_manager
                
                sheets_logger = GoogleSheetsLogger()
                
                reservation_id = reservation["reservation_id"]
                client_name = self._get_line_display_name(user_id)
                
                # Ensure user_id is in reservation data for proper tracking
                if "user_id" not in reservation:
                    reservation["user_id"] = user_id
                
                # Update status in Google Sheets to "Cancelled"
                sheets_success = sheets_logger.update_reservation_status(reservation_id, "Cancelled")
                
                if not sheets_success:
                    return "申し訳ございません。キャンセル処理中にエラーが発生しました。\nスタッフまでお問い合わせください。"
                
                # Cancel the Google Calendar event (use staff name from reservation data)
                staff_name = reservation.get("staff")
                calendar_success = self.google_calendar.cancel_reservation_by_id(reservation_id, staff_name)
                
                if not calendar_success:
                    print(f"Warning: Failed to cancel calendar event for reservation {reservation_id}")
                
                # Log the cancellation action
                try:
                    sheets_logger.log_message(
                        user_id=user_id,
                        user_name=client_name,
                        message_type="user_message",
                        user_message="再予約による自動キャンセル",
                        bot_response="予約をキャンセルして新しい予約フローを開始",
                        action_type="cancellation",
                        reservation_data=reservation
                    )
                except Exception as log_error:
                    print(f"Warning: Failed to log cancellation action: {log_error}")
                
                # Send notification to manager about cancellation
                try:
                    cancellation_reservation_data = {
                        "reservation_id": reservation_id,
                        "date": reservation['date'],
                        "start_time": reservation['start_time'],
                        "end_time": reservation['end_time'],
                        "service": reservation['service'],
                        "staff": reservation['staff']
                    }
                    notification_manager.notify_reservation_cancellation(
                        cancellation_reservation_data,
                        client_name
                    )
                except Exception as notify_error:
                    logging.error(f"Failed to send cancellation notification: {notify_error}")
                    print(f"Warning: Failed to send cancellation notification: {notify_error}")
                
                # Send cancellation confirmation message
                cancellation_message = f"""✅ **予約キャンセル完了**

📋 **キャンセルされた予約内容：**
🆔 予約ID：{reservation_id}
📅 日時：{reservation['date']} {reservation['start_time']}~{reservation['end_time']}
💇 サービス：{reservation['service']}
👨‍💼 担当者：{reservation['staff']}

現在の予約をキャンセルいたしました。
新しい予約を作成するには「予約したい」とお送りください。"""
                
                # Clear the modification state and set up for new reservation
                if user_id in self.user_states:
                    del self.user_states[user_id]
                
                return cancellation_message
                
            except Exception as e:
                print(f"Error in re-reservation confirmation: {e}")
                return "申し訳ございません。処理中にエラーが発生しました。\nスタッフまでお問い合わせください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        # Check for cancellation
        elif message_normalized in ["キャンセル", "cancel", "いいえ", "no"]:
            # Clear the modification state
            if user_id in self.user_states:
                del self.user_states[user_id]
            return "再予約をキャンセルいたします。\n\n何かご不明な点がございましたら、スタッフまでお問い合わせください。"
        
        # Invalid response
        else:
            return "申し訳ございませんが、「はい」「確定」または「キャンセル」でお答えください。"
    
    def _handle_time_modification(self, user_id: str, message: str) -> str:
        """Handle time modification - ask if user wants to change date"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # Store modification type and move to date selection
        self.user_states[user_id]["modification_type"] = "time"
        self.user_states[user_id]["step"] = "modify_time_date_select"
        
        # Get Google Calendar URL
        calendar_url = self.google_calendar.get_calendar_url()
        
        return f"""時間変更ですね！

📋 現在の予約：
📅 日時：{reservation['date']} {reservation['start_time']}~{reservation['end_time']}

🗓️ **Googleカレンダーで予約状況を確認：**
🔗 {calendar_url}

日付を変更しますか？

1️⃣ 同じ日付で時間だけ変更
2️⃣ 日付も変更したい

番号を選択してください。

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_time_date_selection(self, user_id: str, message: str) -> str:
        """Handle date selection for time modification"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # Check user's choice
        if message.strip() == "1":
            # Same date, just change time
            return self._show_available_times_for_date(user_id, reservation["date"])
        elif message.strip() == "2":
            # User wants to change date
            self.user_states[user_id]["step"] = "modify_time_input_date"
            
            # Get Google Calendar URL
            calendar_url = self.google_calendar.get_calendar_url()
            
            return f"""新しい日付を入力してください。

🗓️ **Googleカレンダーで予約状況を確認：**
🔗 {calendar_url}

📅 日付の形式：YYYY-MM-DD
例）2025-10-20

※ 土曜日と日曜日は定休日です。

変更をやめる場合は「キャンセル」とお送りください。"""
        else:
            return """番号を選択してください：

1️⃣ 同じ日付で時間だけ変更
2️⃣ 日付も変更したい

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_time_input_date(self, user_id: str, message: str) -> str:
        """Handle new date input for time modification"""
        # Parse and validate date
        import re
        from datetime import datetime
        
        date_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', message.strip())
        if not date_match:
            return "日付の形式が正しくありません。\nYYYY-MM-DD の形式で入力してください。\n例）2025-10-20\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        try:
            new_date = message.strip()
            date_obj = datetime.strptime(new_date, "%Y-%m-%d")
            
            # Check if it's not Sunday (weekday 6)
            if date_obj.weekday() == 6:
                return "申し訳ございませんが、日曜日は定休日です。\n別の日付を選択してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
            
            # Check if date is in the future
            if date_obj.date() < datetime.now().date():
                return "過去の日付は選択できません。\n本日以降の日付を入力してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
            
            # Date is valid, show available times
            return self._show_available_times_for_date(user_id, new_date)
            
        except ValueError:
            return "日付の形式が正しくありません。\nYYYY-MM-DD の形式で入力してください。\n例）2025-10-20\n\n変更をやめる場合は「キャンセル」とお送りください。"
    
    def _show_available_times_for_date(self, user_id: str, date: str) -> str:
        """Show available times for a specific date - includes current reservation's time"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        print(f"[Show Times] User modifying reservation:")
        print(f"  ID: {reservation.get('reservation_id', 'Unknown')}")
        print(f"  Date: {reservation.get('date', 'Unknown')}")
        print(f"  Time: {reservation.get('start_time', '?')}~{reservation.get('end_time', '?')}")
        print(f"  Service: {reservation.get('service', 'Unknown')}")
        
        # Get available slots for the date (excluding current reservation to free up that time)
        # Only consider events for the current staff member
        available_slots = self.google_calendar.get_available_slots_for_modification(
            date, 
            reservation["reservation_id"],
            reservation["staff"]  # Pass current staff to filter events
        )
        
        if not available_slots:
            return f"申し訳ございませんが、{date}は空いている時間がありません。\n別の日付を選択してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        # Store the selected date and available slots
        self.user_states[user_id]["selected_date"] = date
        self.user_states[user_id]["available_slots"] = available_slots
        self.user_states[user_id]["step"] = "modify_time_select"
        
        # Create time options message with current reservation marker
        time_options = []
        current_start = reservation.get("start_time", "")
        current_end = reservation.get("end_time", "")
        
        for slot in available_slots:
            slot_start = slot["time"]
            slot_end = slot["end_time"]
            
            # Check if this slot contains or overlaps with the current reservation time
            is_current = False
            if date == reservation.get("date"):
                # Check if current reservation falls within this available slot
                if slot_start <= current_start < slot_end or slot_start < current_end <= slot_end:
                    is_current = True
                # Or exact match
                elif slot_start == current_start and slot_end == current_end:
                    is_current = True
            
            current_marker = " (現在の予約時間を含む)" if is_current else ""
            time_options.append(f"✅ {slot_start}~{slot_end}{current_marker}")
        
        return f"""📅 {date} の利用可能な時間：
{chr(10).join(time_options)}

新しい開始時間を入力してください。
例）13:00 または 13:30

💡 現在の予約時間も選択可能です（変更なしの確認）

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_time_selection_for_modification(self, user_id: str, message: str) -> str:
        """Handle time selection for modification"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        selected_date = state["selected_date"]
        
        # Parse start time from user input
        start_time = self._parse_single_time(message.strip())
        
        if not start_time:
            return """時間の入力形式が正しくありません。

正しい入力例：
・13:00
・13:30
・13時
・13時30分

上記の空き時間から開始時間をお選びください。

変更をやめる場合は「キャンセル」とお送りください。"""
        
        # Calculate end time based on service duration
        service_name = reservation.get("service", "")
        service_info = {}
        for service_id, service_data in self.services.items():
            if service_data.get("name") == service_name:
                service_info = service_data
                break
        required_duration = service_info.get("duration", 60)  # Default to 60 minutes
        
        end_time = self._calculate_optimal_end_time(start_time, required_duration)
        
        # Store modification type and pending modification
        self.user_states[user_id]["modification_type"] = "time"
        self.user_states[user_id]["step"] = "modify_confirm"
        self.user_states[user_id]["pending_modification"] = {
            "type": "time",
            "new_date": selected_date,
            "new_time": f"{start_time}~{end_time}"
        }
        
        # Show confirmation message
        return f"""時間変更の確認

📅 変更内容：
• 日付：{selected_date}
• 時間：{start_time}~{end_time}（{required_duration}分）

この内容で変更を確定しますか？

「はい」または「確定」で変更を確定
「キャンセル」で変更を中止

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_service_modification(self, user_id: str, message: str) -> str:
        """Handle service modification with duration validation"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # Show available services as candidates (excluding current service)
        current_service = reservation['service']
        available_service_names = []
        for service_id, service_data in self.services.items():
            service_name = service_data.get("name", service_id)
            if service_name != current_service:
                available_service_names.append(service_name)
        service_list = "\n".join([f"• {service_name}" for service_name in available_service_names])
        
        # Update user state to wait for service selection
        self.user_states[user_id]["step"] = "modify_service_select"
        
        return f"""サービスを選択してください

📋 利用可能なサービス：
{service_list}

上記からサービス名を入力してください。

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_staff_modification(self, user_id: str, message: str) -> str:
        """Handle staff modification"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # This method should only be called when multiple staff are available
        # (since the menu option is hidden when there's only one staff)
        
        # Multiple staff members - show selection
        current_staff = reservation['staff']
        available_staff = []
        for staff_id, staff_data in self.staff_members.items():
            staff_name = staff_data.get("name", staff_id)
            if staff_name != current_staff:
                available_staff.append(staff_name)
        
        staff_list = "\n".join([f"• {staff}" for staff in available_staff])
        
        # Update user state to wait for staff selection
        self.user_states[user_id]["step"] = "modify_staff_select"
        
        return f"""担当者を選択してください

📋 利用可能な担当者：
{staff_list}

上記から担当者名を入力してください。

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _confirm_staff_change(self, user_id: str, new_staff: str) -> str:
        """Confirm staff change and update reservation"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # Update the reservation with new staff
        reservation['staff'] = new_staff
        
        # Update user state to confirmation
        self.user_states[user_id]["step"] = "modify_staff_confirm"
        
        return f"""担当者変更の確認

📋 変更内容：
🆔 予約ID：{reservation['reservation_id']}
📅 日時：{reservation['date']} {reservation['start_time']}~{reservation['end_time']}
💇 サービス：{reservation['service']}
👨‍💼 担当者：{reservation['staff']} → {new_staff}

この内容で変更しますか？
「はい」または「確定」と入力してください。"""
    
    def _handle_staff_selection_for_modification(self, user_id: str, message: str) -> str:
        """Handle staff selection for modification"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # Normalize and validate staff
        message_normalized = message.strip()
        new_staff = None
        
        # Try exact match first with staff names
        for staff_id, staff_data in self.staff_members.items():
            staff_name = staff_data.get("name", staff_id)
            if staff_name == message_normalized:
                new_staff = staff_name
                break
        
        if not new_staff:
            # Try case-insensitive match
            for staff_id, staff_data in self.staff_members.items():
                staff_name = staff_data.get("name", staff_id)
                if staff_name.lower() == message_normalized.lower():
                    new_staff = staff_name
                    break
            
            # Try partial match (if user types part of the staff name)
            if not new_staff:
                for staff_id, staff_data in self.staff_members.items():
                    staff_name = staff_data.get("name", staff_id)
                    if message_normalized in staff_name or staff_name in message_normalized:
                        new_staff = staff_name
                        break
        
        if not new_staff:
            # Show available staff excluding current staff
            current_staff = reservation['staff']
            available_staff = []
            for staff_id, staff_data in self.staff_members.items():
                staff_name = staff_data.get("name", staff_id)
                if staff_name != current_staff:
                    available_staff.append(staff_name)
            available_staff_str = "、".join(available_staff)
            return f"申し訳ございませんが、その担当者は選択できません。\n\n利用可能な担当者：\n{available_staff_str}\n\n上記から選択してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        # Store modification type and pending modification
        self.user_states[user_id]["modification_type"] = "staff"
        self.user_states[user_id]["step"] = "modify_confirm"
        self.user_states[user_id]["pending_modification"] = {
            "type": "staff",
            "new_staff": new_staff
        }
        
        # Show confirmation message
        return f"""担当変更の確認

📅 変更内容：
• 現在の担当：{reservation['staff']}
• 新しい担当：{new_staff}

この内容で変更を確定しますか？

「はい」または「確定」で変更を確定
「キャンセル」で変更を中止

変更をやめる場合は「キャンセル」とお送りください。"""
    
    def _handle_service_selection_for_modification(self, user_id: str, message: str) -> str:
        """Handle service selection for modification"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        
        # Normalize and validate service
        message_normalized = message.strip()
        new_service = None
        
        # Try exact match first against service names
        for service_id, service_data in self.services.items():
            service_name = service_data.get("name", service_id)
            if service_name == message_normalized:
                new_service = service_name
                break
        
        if not new_service:
            # Try case-insensitive match against service names
            for service_id, service_data in self.services.items():
                service_name = service_data.get("name", service_id)
                if service_name.lower() == message_normalized.lower():
                    new_service = service_name
                    break
            
            # Try partial match (if user types part of the service name)
            if not new_service:
                for service_id, service_data in self.services.items():
                    service_name = service_data.get("name", service_id)
                    if message_normalized in service_name or service_name in message_normalized:
                        new_service = service_name
                        break
        
        if not new_service:
            # Show available services excluding current service
            current_service = reservation['service']
            available_service_names = []
            for service_id, service_data in self.services.items():
                service_name = service_data.get("name", service_id)
                if service_name != current_service:
                    available_service_names.append(service_name)
            available_services = available_service_names
            available_services_str = "、".join(available_services)
            return f"申し訳ございませんが、そのサービスは選択できません。\n\n利用可能なサービス：\n{available_services_str}\n\n上記から選択してください。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        # Get new service info to calculate new end time
        new_service_info = {}
        for service_id, service_data in self.services.items():
            if service_data.get("name") == new_service:
                new_service_info = service_data
                break
        
        if not new_service_info:
            return "申し訳ございませんが、サービスの情報を取得できませんでした。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        new_duration = new_service_info.get("duration", 60)
        
        # Calculate new end time based on new service duration
        reservation_date = reservation['date']
        reservation_start_time = reservation['start_time']
        
        try:
            start_dt = datetime.strptime(reservation_start_time, "%H:%M")
            new_end_dt = start_dt + timedelta(minutes=new_duration)
            new_end_time = new_end_dt.strftime("%H:%M")
        except Exception as e:
            logging.error(f"Error calculating new end time: {e}")
            return "申し訳ございませんが、時間の計算中にエラーが発生しました。\n\n変更をやめる場合は「キャンセル」とお送りください。"
        
        # Check if new end time overlaps with next reservation for the same staff
        try:
            import pytz
            
            # Get all events for the date
            all_events = self.google_calendar.get_events_for_date(reservation_date)
            
            # Filter events by staff
            staff_events = self.google_calendar._filter_events_by_staff(all_events, reservation['staff'])
            
            # Parse current reservation datetime
            current_reservation_start = datetime.strptime(
                f"{reservation_date} {reservation_start_time}", 
                "%Y-%m-%d %H:%M"
            )
            new_reservation_end = datetime.strptime(
                f"{reservation_date} {new_end_time}", 
                "%Y-%m-%d %H:%M"
            )
            
            # Find the next reservation (after current reservation start time)
            next_reservation = None
            next_reservation_start = None
            
            tz = pytz.timezone('Asia/Tokyo')
            
            for event in staff_events:
                # Skip the current reservation being modified
                description = event.get('description', '')
                if reservation.get('reservation_id') and f"予約ID: {reservation['reservation_id']}" in description:
                    continue
                
                event_start_str = event.get('start', {}).get('dateTime', '')
                if event_start_str:
                    # Parse event time
                    event_start = datetime.fromisoformat(event_start_str.replace('Z', '+00:00'))
                    event_start = event_start.astimezone(tz).replace(tzinfo=None)
                    
                    # Check if this event starts after current reservation
                    if event_start > current_reservation_start:
                        if next_reservation_start is None or event_start < next_reservation_start:
                            next_reservation_start = event_start
                            next_reservation = event
            
            # Check if new end time overlaps with next reservation
            if next_reservation and next_reservation_start:
                if new_reservation_end > next_reservation_start:
                    # Get next reservation details for error message
                    next_event_summary = next_reservation.get('summary', '予約')
                    next_event_start_str = next_reservation.get('start', {}).get('dateTime', '')
                    
                    if next_event_start_str:
                        next_event_start = datetime.fromisoformat(next_event_start_str.replace('Z', '+00:00'))
                        next_event_start = next_event_start.astimezone(tz)
                        next_start_time_str = next_event_start.strftime("%H:%M")
                        
                        return f"""申し訳ございませんが、{new_service}（{new_duration}分）に変更すると、次の予約と時間が重複してしまいます。

📅 予約日時：{reservation_date} {reservation_start_time}~{new_end_time}
👨‍💼 担当者：{reservation['staff']}
⏱️ 新しい所要時間：{new_duration}分
🚫 次の予約：{next_start_time_str}開始

別のサービスを選択するか、時間を変更してください。

変更をやめる場合は「キャンセル」とお送りください。"""
        except Exception as e:
            logging.error(f"Error checking next reservation overlap: {e}")
            # Continue with modification if check fails (don't block user)
        
        # Store modification type and pending modification
        self.user_states[user_id]["modification_type"] = "service"
        self.user_states[user_id]["step"] = "modify_confirm"
        self.user_states[user_id]["pending_modification"] = {
            "type": "service",
            "new_service": new_service
        }
        
        # Show confirmation message
        return f"""サービス変更の確認

📅 変更内容：
• 現在のサービス：{reservation['service']} ({reservation.get('duration', 'N/A')}分)
• 新しいサービス：{new_service} ({new_duration}分)
• 新しい終了時間：{new_end_time}

この内容で変更を確定しますか？

「はい」または「確定」で変更を確定
「キャンセル」で変更を中止"""
    
    def _handle_modification_confirmation(self, user_id: str, message: str) -> str:
        """Handle modification confirmation and execution"""
        state = self.user_states[user_id]
        reservation = state["reservation_data"]
        modification_type = state["modification_type"]
        pending_modification = state.get("pending_modification", {})
        
        # Check for confirmation keywords
        yes_keywords = self.confirmation_keywords.get("yes", [])
        no_keywords = self.confirmation_keywords.get("no", [])
        cancel_keywords = self.navigation_keywords.get("flow_cancel", [])
        
        if any(keyword in message for keyword in yes_keywords):
            # User confirmed - proceed with modification
            pass  # Continue to execution below
        elif any(keyword in message for keyword in no_keywords) or any(keyword in message for keyword in cancel_keywords):
            # User cancelled - abort modification
            del self.user_states[user_id]
            return "変更をキャンセルいたします。予約はそのまま残ります。\nまたのご利用をお待ちしております。"
        else:
            # Invalid response - ask again
            return "「はい」または「確定」で変更を確定するか、「キャンセル」で中止してください。"
        
        # Check if modification is within 2 hours of reservation start time
        try:
            from datetime import datetime, timedelta
            import pytz
            
            # Get current time in Tokyo timezone
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_time = datetime.now(tokyo_tz)
            
            # Parse reservation date and start time
            reservation_date = reservation["date"]
            reservation_start_time = reservation["start_time"]
            
            # Create reservation datetime in Tokyo timezone
            reservation_datetime = datetime.strptime(f"{reservation_date} {reservation_start_time}", "%Y-%m-%d %H:%M")
            reservation_datetime = tokyo_tz.localize(reservation_datetime)
            
            # Calculate time difference
            time_diff = reservation_datetime - current_time
            
            # Check if within 2 hours (120 minutes)
            if time_diff.total_seconds() <= 7200:  # 2 hours = 7200 seconds
                return f"""申し訳ございませんが、予約開始時刻の2時間以内の変更はお受けできません。

📅 予約日時：{reservation_date} {reservation_start_time}
⏰ 現在時刻：{current_time.strftime('%Y-%m-%d %H:%M')}
⏱️ 残り時間：{int(time_diff.total_seconds() / 3600)}時間{int((time_diff.total_seconds() % 3600) / 60)}分
💰 料金：{reservation['price']:,}円

緊急の場合は直接サロンまでお電話ください。"""
            
        except Exception as e:
            logging.error(f"Error checking modification time limit: {e}")
            # Continue with modification if time check fails
        
        # CRITICAL: Check availability again before confirming modification to prevent race conditions
        availability_check = self._check_modification_availability(reservation, pending_modification, modification_type)
        if not availability_check["available"]:
            # Slot is no longer available - inform user and clear state
            del self.user_states[user_id]
            return f"""❌ 申し訳ございませんが、変更先の時間帯は既に他のお客様にご予約いただいておりました。

{availability_check["message"]}

別の時間帯で変更いただけますでしょうか？
「予約変更したい」とお送りください。"""
        
        try:
            from api.google_sheets_logger import GoogleSheetsLogger
            sheets_logger = GoogleSheetsLogger()
            
            # Process the modification based on type using pending modification data
            if modification_type == "time":
                # Use the pending modification data
                new_date = pending_modification.get("new_date", reservation["date"])
                new_time = pending_modification.get("new_time", "")
                return self._process_time_modification(user_id, new_time, reservation, sheets_logger, new_date)
            elif modification_type == "service":
                new_service = pending_modification.get("new_service", "")
                return self._process_service_modification(user_id, new_service, reservation, sheets_logger)
            elif modification_type == "staff":
                new_staff = pending_modification.get("new_staff", "")
                return self._process_staff_modification(user_id, new_staff, reservation, sheets_logger)
            else:
                return "申し訳ございません。変更処理中にエラーが発生しました。"
                
        except Exception as e:
            logging.error(f"Modification confirmation failed: {e}")
            return "申し訳ございません。変更処理中にエラーが発生しました。スタッフまでお問い合わせください。"
    
    def _process_time_modification(self, user_id: str, message: str, reservation: Dict, sheets_logger, new_date: str = None) -> str:
        """Process time modification"""
        # Parse time range (ONLY accept time period format)
        start_time, end_time = self._parse_time_range(message)
        
        if not start_time or not end_time:
            return "時間の形式が正しくありません。\n「開始時間~終了時間」の形式で入力してください。\n例）13:00~14:00"
        
        # Get the selected date (might be different from original reservation date)
        selected_date = new_date or self.user_states[user_id].get("selected_date", reservation["date"])
        
        # Check if the new booking time is at least 2 hours in advance
        is_valid_time, time_error_message = self._check_advance_booking_time(selected_date, start_time)
        if not is_valid_time:
            return time_error_message
        
        # Calculate the correct end time based on service duration
        try:
            from datetime import datetime, timedelta
            
            # Get service duration
            service_name = reservation["service"]
            service_info = {}
            for service_id, service_data in self.services.items():
                if service_data.get("name") == service_name:
                    service_info = service_data
                    break
            service_duration = service_info.get("duration", 60)
            
            # Calculate correct end time based on start time + service duration
            start_dt = datetime.strptime(start_time, "%H:%M")
            correct_end_dt = start_dt + timedelta(minutes=service_duration)
            correct_end_time = correct_end_dt.strftime("%H:%M")
            
            # Validate that the user's time period falls within an available slot
            available_slots = self.user_states[user_id]["available_slots"]
            time_is_available = False
            
            for slot in available_slots:
                slot_start_dt = datetime.strptime(slot["time"], "%H:%M")
                slot_end_dt = datetime.strptime(slot["end_time"], "%H:%M")
                
                # Check if user's reservation fits within this slot
                if slot_start_dt <= start_dt and correct_end_dt <= slot_end_dt:
                    time_is_available = True
                    break
            
            if not time_is_available:
                return "申し訳ございませんが、その時間は利用できません。\n利用可能な時間から選択してください。"
            
            # Validate user's input end time
            user_end_dt = datetime.strptime(end_time, "%H:%M")
            user_duration_minutes = int((user_end_dt - start_dt).total_seconds() / 60)
            
            if user_duration_minutes <= 0:
                return "終了時間は開始時間より後である必要があります。\n例）13:00~14:00"
            
            # If user input duration is different from service duration, use the correct one
            if user_duration_minutes != service_duration:
                print(f"User input duration ({user_duration_minutes}分) differs from service duration ({service_duration}分). Using service duration.")
                end_time = correct_end_time
            
        except Exception as e:
            logging.error(f"Error calculating duration: {e}")
            return "時間の形式が正しくありません。\n例）13:00~14:00"
        
        # Check for user time conflict (user can't have multiple reservations at the same time)
        user_time_conflict = self.google_calendar.check_user_time_conflict(
            selected_date, start_time, end_time, user_id, reservation["reservation_id"]
        )
        
        if user_time_conflict:
            return f"""申し訳ございませんが、{selected_date} {start_time}~{end_time}の時間帯に既に他のご予約が入っています。

お客様は同じ時間帯に複数のご予約をお取りいただけません。

別の時間を選択してください。"""
        
        # Update Google Calendar with the selected date (use staff name from reservation data)
        staff_name = reservation.get("staff")
        calendar_success = self.google_calendar.modify_reservation_time(
            reservation["reservation_id"], 
            selected_date,  # Use selected date instead of original date
            start_time,
            new_staff=staff_name  # Pass current staff to ensure correct calendar is used
        )
        
        if not calendar_success:
            return "申し訳ございません。カレンダーの更新に失敗しました。スタッフまでお問い合わせください。"
        
        # Update Google Sheets (include date if changed)
        field_updates = {
            "Start Time": start_time,
            "End Time": end_time
        }
        
        # If date was changed, update it too
        if selected_date != reservation["date"]:
            field_updates["Date"] = selected_date
        
        sheets_success = sheets_logger.update_reservation_data(reservation["reservation_id"], field_updates)
        
        if not sheets_success:
            logging.warning(f"Failed to update sheets for reservation {reservation['reservation_id']}")
        
        # Send notification for reservation modification
        try:
            from api.notification_manager import send_reservation_modification_notification
            # Create old and new reservation data for comparison
            old_reservation = reservation.copy()
            new_reservation = reservation.copy()
            new_reservation.update({
                'date': selected_date,
                'start_time': start_time,
                'end_time': end_time
            })
            send_reservation_modification_notification(old_reservation, new_reservation, self._get_line_display_name(user_id))
        except Exception as e:
            logging.error(f"Failed to send reservation modification notification: {e}")
        
        # Clear user state
        del self.user_states[user_id]
        
        return f"""✅ 時間変更が完了しました！

📋 変更内容：
🆔 予約ID：{reservation['reservation_id']}
📅 日時：{selected_date} {start_time}~{end_time}
💇 サービス：{reservation['service']}
👨‍💼 担当者：{reservation['staff']}

ご予約ありがとうございました！"""
    
    def _process_service_modification(self, user_id: str, message: str, reservation: Dict, sheets_logger) -> str:
        """Process service modification with duration validation"""
        # Normalize and validate service
        message_normalized = message.strip()
        new_service = None
        
        # Try exact match first against service names
        for service_id, service_data in self.services.items():
            service_name = service_data.get("name", service_id)
            if service_name == message_normalized:
                new_service = service_name
                break
        
        if not new_service:
            # Try case-insensitive match against service names
            for service_id, service_data in self.services.items():
                service_name = service_data.get("name", service_id)
                if service_name.lower() == message_normalized.lower():
                    new_service = service_name
                    break
            
            # Try partial match (if user types part of the service name)
            if not new_service:
                for service_id, service_data in self.services.items():
                    service_name = service_data.get("name", service_id)
                    if message_normalized in service_name or service_name in message_normalized:
                        new_service = service_name
                        break
        
        if not new_service:
            # Get available service names for display
            available_service_names = []
            for service_id, service_data in self.services.items():
                available_service_names.append(service_data.get("name", service_id))
            available_services = "、".join(available_service_names)
            return f"申し訳ございませんが、そのサービスは提供しておりません。\n\n利用可能なサービス：\n{available_services}\n\n上記から選択してください。"
        
        # Get service info by finding the service ID first
        new_service_info = {}
        for service_id, service_data in self.services.items():
            if service_data.get("name") == new_service:
                new_service_info = service_data
                break
        new_duration = new_service_info["duration"]
        new_price = new_service_info["price"]
        
        # Check if changing to the new service would cause time overlaps for the current staff
        is_available, new_end_time, conflict_info = self.google_calendar.check_service_change_overlap(
            reservation["date"],
            reservation["start_time"],
            new_service,
            reservation["staff"],
            reservation["reservation_id"]
        )
        
        if not is_available:
            # Build detailed conflict message
            conflict_message = f"""申し訳ございませんが、{new_service}（{new_duration}分）に変更すると、{reservation['staff']}の他の予約と時間が重複してしまいます。

📅 予約日時：{reservation['date']} {reservation['start_time']}~{new_end_time}
👨‍💼 担当者：{reservation['staff']}
⏱️ 新しい所要時間：{new_duration}分

🚫 時間が重複する予約："""
            
            if conflict_info and conflict_info.get('conflicts'):
                for conflict in conflict_info['conflicts']:
                    conflict_message += f"\n• {conflict['client']}様: {conflict['start_time']}~{conflict['end_time']}"
            
            conflict_message += f"""

💡 **解決方法：**
1️⃣ 時間を変更してからサービスを変更
2️⃣ 別のサービスを選択
3️⃣ 別の日付に変更

どの方法をご希望ですか？"""
            
            return conflict_message
        
        # Compute new end time based on new service duration for Sheets/confirmation
        try:
            from datetime import datetime, timedelta
            start_dt_for_service = datetime.strptime(reservation["start_time"], "%H:%M")
            new_end_time = (start_dt_for_service + timedelta(minutes=new_duration)).strftime("%H:%M")
            
            # Check if new end time exceeds business hours (18:00)
            new_end_dt = datetime.strptime(new_end_time, "%H:%M")
            business_end_dt = datetime.strptime("18:00", "%H:%M")
            
            if new_end_dt > business_end_dt:
                return f"""申し訳ございませんが、{new_service}（{new_duration}分）は営業時間外になってしまいます。

📅 予約日時：{reservation['date']} {reservation['start_time']}
⏰ 新しい終了時刻：{new_end_time}
🕕 営業終了時刻：18:00

より短い時間のサービスをご選択いただくか、別の時間帯をご検討ください。"""
                
        except Exception:
            new_end_time = reservation.get("end_time", "")

        # Check for user time conflict (user can't have multiple reservations at the same time)
        user_time_conflict = self.google_calendar.check_user_time_conflict(
            reservation["date"], reservation["start_time"], new_end_time, user_id, reservation["reservation_id"]
        )
        
        if user_time_conflict:
            return f"""申し訳ございませんが、{reservation['date']} {reservation['start_time']}~{new_end_time}の時間帯に既に他のご予約が入っています。

お客様は同じ時間帯に複数のご予約をお取りいただけません。

別の時間を選択してください。"""
        
        # Update Google Calendar: change service and adjust duration on the exact event by ID
        # Use current staff name to ensure correct calendar is used
        staff_name = reservation.get("staff")
        calendar_success = self.google_calendar.modify_reservation_time(
            reservation["reservation_id"],
            reservation["date"],
            reservation["start_time"],
            new_service=new_service,
            new_staff=staff_name  # Pass current staff to ensure correct calendar is used
        )
        
        if not calendar_success:
            return "申し訳ございません。カレンダーの更新に失敗しました。スタッフまでお問い合わせください。"
        
        # Update Google Sheets (ensure End Time reflects new service duration)
        field_updates = {
            "Service": new_service,
            "Duration (min)": new_duration,
            "Price": new_price,
            "End Time": new_end_time
        }
        sheets_success = sheets_logger.update_reservation_data(reservation["reservation_id"], field_updates)
        
        if not sheets_success:
            logging.warning(f"Failed to update sheets for reservation {reservation['reservation_id']}")
        
        # Send notification for reservation modification
        try:
            from api.notification_manager import send_reservation_modification_notification
            # Create old and new reservation data for comparison
            old_reservation = reservation.copy()
            new_reservation = reservation.copy()
            new_reservation.update({
                'service': new_service,
                'duration': new_duration,
                'end_time': new_end_time
            })
            send_reservation_modification_notification(old_reservation, new_reservation, self._get_line_display_name(user_id))
        except Exception as e:
            logging.error(f"Failed to send reservation modification notification: {e}")

        # Update local reservation snapshot for confirmation message
        reservation["service"] = new_service
        reservation["duration"] = new_duration
        reservation["end_time"] = new_end_time

        # Clear user state
        del self.user_states[user_id]
        
        return f"""✅ サービス変更が完了しました！

📋 変更内容：
🆔 予約ID：{reservation['reservation_id']}
📅 日時：{reservation['date']} {reservation['start_time']}~{new_end_time}
💇 サービス：{new_service} ({new_duration}分・{new_price:,}円)
👨‍💼 担当者：{reservation['staff']}

ご予約ありがとうございました！"""
    
    def _process_staff_modification(self, user_id: str, message: str, reservation: Dict, sheets_logger) -> str:
        """Process staff modification"""
        # Normalize and validate staff
        message_normalized = message.strip()
        new_staff = None
        
        # Try exact match first against staff names
        for staff_id, staff_data in self.staff_members.items():
            staff_name = staff_data.get("name", staff_id)
            if staff_name == message_normalized:
                new_staff = staff_name
                break
        
        if not new_staff:
            # Try case-insensitive match against staff names
            for staff_id, staff_data in self.staff_members.items():
                staff_name = staff_data.get("name", staff_id)
                if staff_name.lower() == message_normalized.lower():
                    new_staff = staff_name
                    break
            
            # Try partial match (if user types part of the staff name)
            if not new_staff:
                for staff_id, staff_data in self.staff_members.items():
                    staff_name = staff_data.get("name", staff_id)
                    if message_normalized in staff_name or staff_name in message_normalized:
                        new_staff = staff_name
                        break
        
        if not new_staff:
            # Get available staff names for display
            available_staff_names = []
            for staff_id, staff_data in self.staff_members.items():
                available_staff_names.append(staff_data.get("name", staff_id))
            available_staff = "、".join(available_staff_names)
            return f"申し訳ございませんが、その担当者は選択できません。\n\n利用可能な担当者：\n{available_staff}\n\n上記から選択してください。"
        
        # Check if the new staff is available for the current reservation time
        is_available = self.google_calendar.check_staff_availability_for_time(
            reservation["date"],
            reservation["start_time"],
            reservation["end_time"],
            new_staff,
            reservation["reservation_id"]
        )
        
        if not is_available:
            return f"""申し訳ございませんが、{new_staff}は{reservation['date']} {reservation['start_time']}~{reservation['end_time']}の時間帯に他の予約が入っています。

別の担当者を選択するか、時間を変更してから担当者を変更してください。

💡 **時間変更も可能です** - 「日時変更したい」を選択してください。"""
        
        # Check for user time conflict (user can't have multiple reservations at the same time)
        user_time_conflict = self.google_calendar.check_user_time_conflict(
            reservation["date"], reservation["start_time"], reservation["end_time"], user_id, reservation["reservation_id"]
        )
        
        if user_time_conflict:
            return f"""申し訳ございませんが、{reservation['date']} {reservation['start_time']}~{reservation['end_time']}の時間帯に既に他のご予約が入っています。

お客様は同じ時間帯に複数のご予約をお取りいただけません。

別の時間を選択してください。"""
        
        # Update Google Calendar summary to reflect new staff
        calendar_success = self.google_calendar.modify_reservation_time(
            reservation["reservation_id"],
            reservation["date"],
            reservation["start_time"],
            new_staff=new_staff
        )
        if not calendar_success:
            return "申し訳ございません。カレンダーの更新に失敗しました。スタッフまでお問い合わせください。"
        
        # Update Google Sheets
        field_updates = {
            "Staff": new_staff
        }
        sheets_success = sheets_logger.update_reservation_data(reservation["reservation_id"], field_updates)
        
        if not sheets_success:
            return "申し訳ございません。担当者の更新に失敗しました。スタッフまでお問い合わせください。"
        
        # Send notification for reservation modification
        try:
            from api.notification_manager import send_reservation_modification_notification
            # Create old and new reservation data for comparison
            old_reservation = reservation.copy()
            new_reservation = reservation.copy()
            new_reservation.update({
                'staff': new_staff
            })
            send_reservation_modification_notification(old_reservation, new_reservation, self._get_line_display_name(user_id))
        except Exception as e:
            logging.error(f"Failed to send reservation modification notification: {e}")
        
        # Clear user state
        del self.user_states[user_id]
        
        return f"""✅ 担当者変更が完了しました！

📋 変更内容：
🆔 予約ID：{reservation['reservation_id']}
📅 日時：{reservation['date']} {reservation['start_time']}~{reservation['end_time']}
💇 サービス：{reservation['service']}
👨‍💼 担当者：{new_staff}

ご予約ありがとうございました！"""


def main():
    """Interactive test function for reservation flow"""
    print("=== Interactive Reservation Flow Tester ===")
    print("Type your messages to test the reservation system interactively!")
    print("Type 'quit' or 'exit' to stop testing.")
    print("Type 'help' to see available commands.")
    print("="*60)
    
    try:
        # Initialize ReservationFlow
        rf = ReservationFlow()
        print("✅ ReservationFlow initialized successfully")
        
        # Test user ID
        test_user_id = "interactive_test_user"
        
        print(f"\n🎯 Ready to test! User ID: {test_user_id}")
        print("💡 Try starting with: 予約したい")
        print("-" * 60)
        
        while True:
            try:
                # Get user input
                user_input = input("\n👤 You: ").strip()
                
                # Handle special commands
                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("👋 Goodbye! Thanks for testing!")
                    break
                elif user_input.lower() == 'help':
                    print_help()
                    continue
                elif user_input.lower() == 'status':
                    print_user_status(rf, test_user_id)
                    continue
                elif user_input.lower() == 'clear':
                    clear_user_state(rf, test_user_id)
                    continue
                elif user_input.lower() == 'reset':
                    test_user_id = f"interactive_test_user_{int(time.time())}"
                    print(f"🔄 Reset with new user ID: {test_user_id}")
                    continue
                elif not user_input:
                    print("⚠️ Please enter a message or command.")
                    continue
                
                # Get response from reservation flow
                response = rf.get_response(test_user_id, user_input)
                
                # Display response
                print(f"\n🤖 Bot: {response}")
                
                # Show current user state
                if test_user_id in rf.user_states:
                    current_step = rf.user_states[test_user_id].get('step', 'unknown')
                    print(f"📊 Current step: {current_step}")
                else:
                    print("📊 Current step: No active session")
                
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye! Thanks for testing!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                import traceback
                traceback.print_exc()
        
    except Exception as e:
        print(f"❌ Error during initialization: {e}")
        import traceback
        traceback.print_exc()


def print_help():
    """Print help information for the interactive tester"""
    print("\n" + "="*60)
    print("📖 INTERACTIVE TESTER HELP")
    print("="*60)
    print("🎯 RESERVATION FLOW COMMANDS:")
    print("  • 予約したい, 予約お願い, 予約できますか - Start reservation")
    print("  • カット, カラー, パーマ, トリートメント - Select service")
    print("  • 田中, 佐藤, 山田, 未指定 - Select staff")
    print("  • 2025-01-15 (or any date) - Select date")
    print("  • 10:00~11:00 (or any time range) - Select time")
    print("  • はい, 確定, お願い - Confirm reservation")
    print("  • いいえ, キャンセル, やめる - Cancel reservation")
    print()
    print("🔄 NAVIGATION COMMANDS:")
    print("  • 日付変更, 日付を変更, 別の日 - Go back to date selection")
    print("  • サービス変更, サービスを変更 - Go back to service selection")
    print("  • キャンセル, 取り消し, やめる - Cancel current flow")
    print()
    print("📋 RESERVATION MANAGEMENT:")
    print("  • 予約キャンセル, 予約取り消し - Cancel existing reservation")
    print("  • 予約変更, 予約修正 - Modify existing reservation")
    print()
    print("🛠️ TESTER COMMANDS:")
    print("  • help - Show this help message")
    print("  • status - Show current user state")
    print("  • clear - Clear current user state")
    print("  • reset - Reset with new user ID")
    print("  • quit, exit, q - Exit the tester")
    print("="*60)


def print_user_status(rf, user_id):
    """Print current user state information"""
    print(f"\n📊 USER STATUS: {user_id}")
    print("-" * 40)
    
    if user_id in rf.user_states:
        state = rf.user_states[user_id]
        step = state.get('step', 'unknown')
        data = state.get('data', {})
        
        print(f"Current Step: {step}")
        print("Reservation Data:")
        for key, value in data.items():
            print(f"  • {key}: {value}")
    else:
        print("No active session")
    
    print("-" * 40)


def clear_user_state(rf, user_id):
    """Clear the current user state"""
    if user_id in rf.user_states:
        del rf.user_states[user_id]
        print(f"✅ Cleared user state for {user_id}")
    else:
        print(f"ℹ️ No user state found for {user_id}")


# Import time for reset functionality
import time


if __name__ == "__main__":
    main()
