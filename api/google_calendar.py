"""
Google Calendar integration for salon reservations
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pytz
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

class GoogleCalendarHelper:
    def __init__(self):
        load_dotenv()
        # Remove this line:
        # self.calendar_id = os.getenv("GOOGLE_CALENDAR_ID")
        self.timezone = os.getenv("GOOGLE_CALENDAR_TIMEZONE", "Asia/Tokyo")
        self.service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        
        # Load services and staff data from JSON
        self.services_data = self._load_services_data()
        self.staff_data = self.services_data.get("staff", {})
        self.services = self.services_data.get("services", {})
        
        # Optionally, set a fallback calendar_id if needed
        self.calendar_id = None

        # Initialize Google Calendar service
        self.service = None
        try:
            self._authenticate()
        except Exception as e:
            print(f"Failed to initialize Google Calendar: {e}")
            self.service = None
    
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
    
    def _load_services_data(self) -> Dict[str, Any]:
        """Load services and staff data from JSON file"""
        try:
            # Get the directory of this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            services_file = os.path.join(current_dir, "data", "services.json")
            
            with open(services_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load services data: {e}")
            # Return default data if file loading fails
            return {}
    
    def _authenticate(self):
        """Authenticate with Google Calendar API using service account"""
        try:
            if not self.service_account_json:
                print("GOOGLE_SERVICE_ACCOUNT_JSON not set, calendar integration disabled")
                return
            
            # Parse service account JSON from environment variable
            try:
                service_account_info = json.loads(self.service_account_json)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
                return
            
            # Load service account credentials from JSON
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/calendar']
            )
            
            # Build the service
            self.service = build('calendar', 'v3', credentials=credentials)
            print("Google Calendar API authenticated successfully")
            
        except Exception as e:
            print(f"Failed to authenticate with Google Calendar: {e}")
            self.service = None
    
    def generate_reservation_id(self, date_str: str) -> str:
        """Generate a unique reservation ID in format RES-YYYYMMDD-XXXX"""
        # Extract date components
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_part = date_obj.strftime("%Y%m%d")
        
        # For simplicity, use timestamp-based counter (in real app, use database counter)
        import time
        counter = int(time.time() * 1000) % 10000  # Last 4 digits of timestamp
        
        return f"RES-{date_part}-{counter:04d}"
    
    def create_reservation_event(self, reservation_data: Dict[str, Any], client_name: str) -> bool:
        """
        Create a calendar event for a completed reservation
        
        Args:
            reservation_data: Dict containing service, staff, date, time, reservation_id
            client_name: Client's display name from LINE
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.service:
            print("Google Calendar not configured, skipping event creation")
            return False
        
        try:
            # Parse date and time
            date_str = reservation_data['date']
            service = reservation_data['service']
            staff = reservation_data['staff']
            
            # Get staff-specific calendar ID
            staff_calendar_id = self._get_staff_calendar_id(staff)
            if not staff_calendar_id:
                print(f"Staff calendar ID not found for {staff}, skipping event creation")
                return False
            
            # Handle both single time and time range
            if 'start_time' in reservation_data and 'end_time' in reservation_data:
                start_time_str = reservation_data['start_time']
                end_time_str = reservation_data['end_time']
                
                # Calculate start and end datetime
                start_datetime = datetime.strptime(f"{date_str} {start_time_str}", "%Y-%m-%d %H:%M")
                end_datetime = datetime.strptime(f"{date_str} {end_time_str}", "%Y-%m-%d %H:%M")
            else:
                # Fallback to single time (backward compatibility)
                time_str = reservation_data['time']
                start_datetime = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                
                # Get service duration and calculate end time
                duration_minutes = self._get_service_duration_minutes(service)
                end_datetime = start_datetime + timedelta(minutes=duration_minutes)
            
            # Calculate duration for display purposes
            duration_minutes = int((end_datetime - start_datetime).total_seconds() / 60)
            
            # Format for Google Calendar API
            start_iso = start_datetime.isoformat()
            end_iso = end_datetime.isoformat()
            
            # Get reservation ID
            reservation_id = reservation_data.get('reservation_id', self.generate_reservation_id(date_str))
            
            # Build event details
            event_title = f"[予約] {service} - {client_name} ({staff})"
            
            # Build description
            description = f"""
予約ID: {reservation_id}
サービス: {service}
担当者: {staff}
お客様: {client_name}
所要時間: {duration_minutes}分
予約元: LINE Bot
            """.strip()
            
            # Get staff color ID
            staff_color_id = self._get_staff_color_id(staff)
            
            # Create event
            event = {
                'summary': event_title,
                'description': description,
                'start': {
                    'dateTime': start_iso,
                    'timeZone': self.timezone,
                },
                'end': {
                    'dateTime': end_iso,
                    'timeZone': self.timezone,
                },
            }
            
            # Add color if staff color is available
            if staff_color_id:
                event['colorId'] = staff_color_id
            
            # Add staff as attendee if not "未指定"
            if staff != "未指定":
                staff_email = self._get_staff_email(staff)
                if staff_email:
                    event['attendees'] = [{'email': staff_email}]
            
            # Create the event in staff-specific calendar
            created_event = self.service.events().insert(
                calendarId=staff_calendar_id,
                body=event
            ).execute()
            
            print(f"Calendar event created: {created_event.get('htmlLink')}")
            return True
            
        except HttpError as e:
            print(f"Google Calendar API error: {e}")
            return False
        except Exception as e:
            print(f"Failed to create calendar event: {e}")
            return False

    def _get_service_duration_minutes(self, service_name: str) -> int:
        """Return duration in minutes for a given service name."""
        if not service_name:
            return 60

        # Try direct lookup by service ID key
        service_data = self.services.get(service_name)
        if service_data and isinstance(service_data, dict):
            return service_data.get("duration", 60)

        # Fallback: search by service name field
        for service_id, data in self.services.items():
            if isinstance(data, dict) and data.get("name") == service_name:
                return data.get("duration", 60)

        return 60

    def _find_upcoming_event_by_client(self, client_name: str, days_ahead: int = 90) -> Optional[Dict[str, Any]]:
        """Find the next upcoming event for the given client name.

        Tries to match by summary or description containing the client name.
        """
        if not self.service or not self.calendar_id:
            print("Google Calendar not configured, cannot search events")
            return None

        try:
            now = datetime.utcnow()
            end = now + timedelta(days=days_ahead)
            events_result = self.service.events().list(
                calendarId=self.calendar_id,
                timeMin=now.isoformat() + 'Z',
                timeMax=end.isoformat() + 'Z',
                singleEvents=True,
                orderBy='startTime'
            ).execute()

            events = events_result.get('items', [])
            for event in events:
                summary = event.get('summary', '') or ''
                description = event.get('description', '') or ''
                if client_name and (client_name in summary or client_name in description):
                    return event

            return None
        except Exception as e:
            print(f"Failed to search events: {e}")
            return None

    def cancel_reservation(self, client_name: str) -> bool:
        """Delete the client's upcoming reservation event if found."""
        event = self._find_upcoming_event_by_client(client_name)
        if not event:
            return False
        try:
            self.service.events().delete(
                calendarId=self.calendar_id,
                eventId=event['id']
            ).execute()
            print(f"Cancelled reservation for {client_name}")
            return True
        except Exception as e:
            print(f"Failed to cancel reservation: {e}")
            return False

    def cancel_reservation_by_id(self, reservation_id: str) -> bool:
        """Delete a reservation event by reservation ID."""
        try:
            # Search for events with the reservation ID in the description
            events_result = self.service.events().list(
                calendarId=self.calendar_id,
                timeMin=datetime.now().isoformat() + 'Z',
                maxResults=100,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            
            for event in events:
                description = event.get('description', '')
                if reservation_id in description:
                    # Found the event with this reservation ID
                    self.service.events().delete(
                        calendarId=self.calendar_id,
                        eventId=event['id']
                    ).execute()
                    print(f"Cancelled reservation {reservation_id} from Google Calendar")
                    return True
            
            print(f"Reservation {reservation_id} not found in Google Calendar")
            return False
            
        except Exception as e:
            print(f"Failed to cancel reservation by ID {reservation_id}: {e}")
            return False

    def modify_reservation_time(self, reservation_id: str, new_date: str, new_time: str, new_service: Optional[str] = None, new_staff: Optional[str] = None) -> bool:
        """Update the start/end time for a reservation by its ID.

        If new_service is provided, adjust duration by that service and update summary.
        If new_staff is provided, update summary staff name and move to new staff's calendar.
        Otherwise preserve the original duration.
        """
        try:
            # Find the event by reservation ID (search in all calendars if staff not specified)
            event = self.get_reservation_by_id(reservation_id, new_staff)
            if not event:
                print(f"Reservation {reservation_id} not found")
                return False
            
            # Extract staff name from event summary if not provided
            if not new_staff:
                summary = event.get('summary', '')
                try:
                    import re
                    m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                    if m:
                        current_staff = m.group(3)
                        new_staff = current_staff
                except Exception:
                    pass
            
            # Get staff-specific calendar ID
            staff_calendar_id = self._get_staff_calendar_id(new_staff) if new_staff else self.calendar_id
            if not staff_calendar_id:
                print(f"Staff calendar ID not found for {new_staff}")
                return False

            # Extract current event details
            current_start = event.get('start', {}).get('dateTime', '')
            current_end = event.get('end', {}).get('dateTime', '')
            
            if not current_start:
                print(f"No start time found for reservation {reservation_id}")
                return False

            # Parse current datetime
            start_dt = datetime.fromisoformat(current_start)
            
            # Apply new date and time
            new_date_obj = datetime.strptime(new_date, "%Y-%m-%d")
            normalized_time = self._normalize_time_format(new_time)
            if not normalized_time:
                return False
            new_time_obj = datetime.strptime(normalized_time, "%H:%M")
            
            # Update the datetime
            start_dt = start_dt.replace(
                year=new_date_obj.year,
                month=new_date_obj.month,
                day=new_date_obj.day,
                hour=new_time_obj.hour,
                minute=new_time_obj.minute
            )
            
            # Calculate end time
            if new_service:
                duration_minutes = self._get_service_duration_minutes(new_service)
                end_dt = start_dt + timedelta(minutes=duration_minutes)
            else:
                # Preserve original duration
                if current_end:
                    current_end_dt = datetime.fromisoformat(current_end)
                    duration = current_end_dt - datetime.fromisoformat(current_start)
                    end_dt = start_dt + duration
                else:
                    # Default 60 minutes if no end time
                    end_dt = start_dt + timedelta(minutes=60)

            # Update the event
            event['start'] = {
                'dateTime': start_dt.isoformat(),
                'timeZone': self.timezone,
            }
            event['end'] = {
                'dateTime': end_dt.isoformat(),
                'timeZone': self.timezone,
            }
            
            # If changing service or staff, update summary while preserving other parts
            if new_service or new_staff:
                summary = event.get('summary', '') or ''
                # Expected format: "[予約] SERVICE - CLIENT (STAFF)"
                try:
                    import re
                    m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                    if m:
                        current_service = m.group(1)
                        client = m.group(2)
                        current_staff = m.group(3)
                        updated_service = new_service if new_service else current_service
                        updated_staff = new_staff if new_staff else current_staff
                        event['summary'] = f"[予約] {updated_service} - {client} ({updated_staff})"
                except Exception:
                    pass
            
            # Update color if staff is being changed
            if new_staff:
                new_staff_color_id = self._get_staff_color_id(new_staff)
                if new_staff_color_id:
                    event['colorId'] = new_staff_color_id

            # If staff changed, we need to delete from old calendar and create in new calendar
            if new_staff:
                # Extract current staff from event summary
                summary = event.get('summary', '')
                current_staff = None
                try:
                    import re
                    m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                    if m:
                        current_staff = m.group(3)
                except Exception:
                    pass
                
                # If staff changed, move event to new calendar
                if current_staff and current_staff != new_staff:
                    current_calendar_id = self._get_staff_calendar_id(current_staff)
                    if current_calendar_id and current_calendar_id != staff_calendar_id:
                        # Delete from old calendar
                        try:
                            self.service.events().delete(
                                calendarId=current_calendar_id,
                                eventId=event['id']
                            ).execute()
                            print(f"Moved reservation from {current_staff}'s calendar to {new_staff}'s calendar")
                        except Exception as e:
                            print(f"Warning: Failed to delete from old calendar: {e}")
                        
                        # Create in new calendar (event will be recreated with new ID)
                        created_event = self.service.events().insert(
                            calendarId=staff_calendar_id,
                            body=event
                        ).execute()
                        print(f"Successfully moved reservation {reservation_id} to {new_staff}'s calendar")
                        return True
            
            # Update in staff-specific calendar
            updated = self.service.events().update(
                calendarId=staff_calendar_id,
                eventId=event['id'],
                body=event
            ).execute()

            print(f"Successfully modified reservation {reservation_id}")
            print(f"  New time: {start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%H:%M')}")
            return True
            
        except Exception as e:
            print(f"Failed to modify reservation time for {reservation_id}: {e}")
            return False
    
    def get_available_slots(self, start_date: datetime, end_date: datetime, staff_name: str = None) -> list:
        """
        Get available time slots from Google Calendar
        
        Args:
            start_date: Start date to check availability
            end_date: End date to check availability
            staff_name: Optional staff name to get slots from staff-specific calendar
            
        Returns:
            list: List of available time slots
        """
        # Get staff-specific calendar ID if staff_name is provided
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        
        if not self.service or not calendar_id:
            print("Google Calendar not configured, using fallback slots")
            return self._generate_fallback_slots(start_date, end_date)
        
        try:
            # Get events from staff-specific calendar
            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=start_date.isoformat() + 'Z',
                timeMax=end_date.isoformat() + 'Z',
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            
            # Generate only available (unselected) slots
            available_slots = self._generate_all_slots(start_date, end_date, events)
            
            return available_slots
            
        except Exception as e:
            print(f"Failed to get available slots from Google Calendar: {e}")
            return self._generate_fallback_slots(start_date, end_date)
    
    def _generate_all_slots(self, start_date: datetime, end_date: datetime, events: list = None) -> list:
        """Generate available time periods based on gaps between existing reservations"""
        slots = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        # Business hours: 9:00~12:00, 13:00~18:00 (skip 12:00~13:00 lunch break)
        business_periods = [
            {"start": 9, "end": 12},   # 9:00 ~ 12:00
            {"start": 13, "end": 18}   # 13:00 ~ 18:00
        ]
        
        while current_date <= end_date_only:
            print(f"[Generate All Slots] Current date: {current_date}")
            # Skip Sundays (weekday 6)
            if current_date.weekday() != 6:
                # Get events for this specific date
                date_events = []
                if events:
                    for event in events:
                        event_start = datetime.fromisoformat(event['start'].get('dateTime', event['start'].get('date', '')))
                        if event_start.date() == current_date:
                            date_events.append(event)
                
                # Sort events by start time
                date_events.sort(key=lambda e: datetime.fromisoformat(e['start'].get('dateTime', e['start'].get('date', ''))))
                
                # Find available periods for each business period
                for business_period in business_periods:
                    print("calling _find_available_periods")
                    available_periods = self._find_available_periods(
                        current_date, business_period, date_events
                    )
                    # Add available periods as slots
                    for period in available_periods:
                        slots.append({
                            "date": current_date.strftime("%Y-%m-%d"),
                            "time": period["start"],
                            "end_time": period["end"],
                            "available": True
                        })
            
            current_date += timedelta(days=1)
        
        return slots
    
    def _find_available_periods(self, date, business_period, events):
        """Find available time periods within business hours, excluding existing events"""
        available_periods = []
        
        # Convert business period to datetime objects
        tz = pytz.timezone(self.timezone)
        business_start = tz.localize(datetime.combine(date, datetime.min.time().replace(hour=business_period["start"])))
        business_end = tz.localize(datetime.combine(date, datetime.min.time().replace(hour=business_period["end"])))
        
        print(f"[Find Periods] Business: {business_start.strftime('%H:%M')} ~ {business_end.strftime('%H:%M')}, Events: {len(events)}")
        
        # Convert events to datetime ranges and merge overlapping ones
        available_periods = []
        for event in events:
            event_start = datetime.fromisoformat(event['start'].get('dateTime', event['start'].get('date', '')))
            event_end = datetime.fromisoformat(event['end'].get('dateTime', event['end'].get('date', '')))
            
            print(f"[Find Periods] Processing event: {event_start.strftime('%H:%M')} ~ {event_end.strftime('%H:%M')}")
            print(f"  Current business_start: {business_start.strftime('%H:%M')}")
            
            if event_start <= business_end and event_end >= business_start:
                print(f"  Event overlaps with business hours")
                if event_start > business_start:
                    print(f"  Gap found: {business_start.strftime('%H:%M')} ~ {event_start.strftime('%H:%M')}")
                    available_periods.append({
                        'start': business_start.strftime("%H:%M"),
                        'end': event_start.strftime("%H:%M")
                    })
                    business_start = event_end
                    print(f"  Updated business_start to: {business_start.strftime('%H:%M')}")
                elif event_start == business_start:
                    print(f"  Event starts at business_start, moving to: {event_end.strftime('%H:%M')}")
                    business_start = event_end
                    print(f"  Updated business_start to: {business_start.strftime('%H:%M')}")
            else:
                print(f"  Event outside business hours, skipping")

        print(f"[Find Periods] After all events, business_start: {business_start.strftime('%H:%M')}, business_end: {business_end.strftime('%H:%M')}")
        if business_start < business_end:
            print(f"[Find Periods] Final gap: {business_start.strftime('%H:%M')} ~ {business_end.strftime('%H:%M')}")
            available_periods.append({
                'start': business_start.strftime("%H:%M"),
                'end': business_end.strftime("%H:%M")
            })
        
        print(f"[Find Periods] Total available periods: {len(available_periods)}")
        return available_periods
    
    def _generate_fallback_slots(self, start_date: datetime, end_date: datetime) -> list:
        """Generate fallback slots when Google Calendar is not available"""
        print("[Fallback] Generating fallback slots - Google Calendar not configured")
        try:
            slots = self._generate_all_slots(start_date, end_date, None)
            print(f"[Fallback] Generated {len(slots)} fallback slots")
            return slots
        except Exception as e:
            print(f"[Fallback] Error generating fallback slots: {e}")
            # Return minimal fallback slots
            return [
                {
                    "date": start_date.strftime("%Y-%m-%d"),
                    "time": "09:00",
                    "end_time": "12:00",
                    "available": True
                },
                {
                    "date": start_date.strftime("%Y-%m-%d"),
                    "time": "13:00",
                    "end_time": "18:00",
                    "available": True
                }
            ]
    
    def get_calendar_url(self, staff_name: str = None) -> str:
        """Get the public Google Calendar URL for viewing availability (staff-specific)"""
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        if not calendar_id:
            return "https://calendar.google.com/calendar"
        
        # Enhanced calendar URL with better visibility and Tokyo timezone
        base_url = f"https://calendar.google.com/calendar/embed?src={calendar_id}"
        params = [
            "ctz=Asia%2FTokyo",  # Tokyo timezone
            "mode=WEEK",         # Week view
            "showTitle=1",       # Hide title
            "showNav=1",         # Show navigation
            "showDate=1",        # Show date
            "showTabs=1",        # Show tabs
            "showCalendars=1",   # Show calendar list
            "showTz=1",          # Show timezone
            "height=600",        # Calendar height
            "wkst=1",            # Week starts Sunday
            "bgcolor=%23ffffff", # White background
            "color=%23B1365F"    # Calendar color
        ]
        
        return f"{base_url}&{'&'.join(params)}"
    
    def get_public_calendar_url(self, staff_name: str = None) -> str:
        """Get the public calendar URL (if calendar is made public) - staff-specific"""
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        if not calendar_id:
            return "https://calendar.google.com/calendar"
        
        # Public calendar URL format with Tokyo timezone
        return f"https://calendar.google.com/calendar/embed?src={calendar_id}&ctz=Asia%2FTokyo"
    
    def get_simple_calendar_url(self, staff_name: str = None) -> str:
        """Get a simple calendar URL for basic sharing - staff-specific"""
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        if not calendar_id:
            return "https://calendar.google.com/calendar"
        
        # Simple calendar URL
        return f"https://calendar.google.com/calendar/embed?src={calendar_id}"
    
    def get_short_calendar_url(self, staff_name: str = None) -> str:
        """Get a short calendar URL for notifications - staff-specific"""
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        if not calendar_id:
            return "https://calendar.google.com/calendar"
        
        # Short calendar URL with minimal parameters
        return f"https://calendar.google.com/calendar/embed?src={calendar_id}&ctz=Asia%2FTokyo"
    
    def get_events_for_date(self, date_str: str, staff_name: str = None) -> List[Dict]:
        """Get all events for a specific date (timezone-aware) from staff-specific calendar"""
        # Get staff-specific calendar ID if staff_name is provided
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        
        if not self.service or not calendar_id:
            return []
        
        try:
            # Create timezone-aware datetime objects for Tokyo time
            tz = pytz.timezone(self.timezone)
            
            # Get start of day (00:00:00 Tokyo time)
            start_date = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            start_date_aware = tz.localize(start_date)
            
            # Get end of day (next day 00:00:00 Tokyo time)
            end_date_aware = start_date_aware + timedelta(days=1)
            
            print(f"[Get Events] Fetching events from {start_date_aware.isoformat()} to {end_date_aware.isoformat()} (Calendar: {calendar_id})")
            
            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=start_date_aware.isoformat(),
                timeMax=end_date_aware.isoformat(),
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            print(f"[Get Events] Found {len(events)} event(s) for {date_str}")
            
            return events
        except Exception as e:
            print(f"Failed to get events for date {date_str}: {e}")
            return []
    
    def get_available_slots_for_modification(self, date_str: str, exclude_reservation_id: str = None, staff_name: str = None) -> List[Dict]:
        """
        Get available slots for modification - INCLUDES the user's current reservation time,
        EXCLUDES other reservations for the same staff member
        """
        # Get staff-specific calendar ID
        calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
        
        if not self.service or not calendar_id:
            return self._generate_fallback_slots(
                datetime.strptime(date_str, "%Y-%m-%d"),
                datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
            )
        
        try:
            # Get all events for the date from staff-specific calendar
            all_events = self.get_events_for_date(date_str, staff_name)
            print(f"[Modification] Date: {date_str}, Total events: {len(all_events)}, Current Reservation ID: {exclude_reservation_id}, Staff: {staff_name}")
            
            # Filter events by staff if staff_name is provided
            if staff_name:
                all_events = self._filter_events_by_staff(all_events, staff_name)
                print(f"[Modification] Filtered to {len(all_events)} events for staff: {staff_name}")
            
            current_reservation = None
            other_events = []
            
            if exclude_reservation_id:
                for e in all_events:
                    description = e.get('description', '')
                    event_start = e.get('start', {}).get('dateTime', 'N/A')
                    event_end = e.get('end', {}).get('dateTime', 'N/A')
                    
                    # Extract reservation ID from description for debugging
                    event_res_id = "Unknown"
                    if '予約ID:' in description:
                        event_res_id = description.split('予約ID:')[1].split('\n')[0].strip()
                    
                    # Check if this is the reservation being modified
                    if f"予約ID: {exclude_reservation_id}" in description:
                        current_reservation = e
                        print(f"  📌 Current reservation (INCLUDE in slots): {e.get('summary', 'N/A')}")
                        print(f"     Time: {event_start} ~ {event_end}")
                        print(f"     ID: {event_res_id}")
                    else:
                        other_events.append(e)
                        print(f"  🚫 Other reservation (BLOCK slots): {e.get('summary', 'N/A')}")
                        print(f"     Time: {event_start} ~ {event_end}")
                        print(f"     ID: {event_res_id}")
            else:
                other_events = all_events
            
            print(f"[Modification] Using {len(other_events)} other events for blocking")
            
            # Log the blocking events details
            for e in other_events:
                start_time = e.get('start', {}).get('dateTime', 'N/A')
                end_time = e.get('end', {}).get('dateTime', 'N/A')
                print(f"  🚫 Blocking: {start_time} ~ {end_time}")
            
            # Generate available slots based ONLY on other reservations
            # This means the current reservation's time will be shown as available
            start_date = datetime.strptime(date_str, "%Y-%m-%d")
            end_date = start_date 
            
            print(f"[Modification] Calling _generate_all_slots with:")
            print(f"  start_date: {start_date}")
            print(f"  end_date: {end_date}")
            print(f"  blocking events: {len(other_events)}")
            
            available_slots = self._generate_all_slots(start_date, end_date, other_events)
            print(f"[Modification] Generated {len(available_slots)} available slot(s)")
            for slot in available_slots:
                print(f"  ✅ Available: {slot['time']} ~ {slot['end_time']}")
            
            return available_slots
            
        except Exception as e:
            print(f"Failed to get available slots for modification: {e}")
            return []
    
    def get_available_slots_for_service(self, date_str: str, service_name: str, exclude_reservation_id: str = None, staff_name: str = None) -> List[Dict]:
        """Get available slots considering service duration requirements"""
        service_duration = self._get_service_duration_minutes(service_name)
        
        # Get all available periods
        all_slots = self.get_available_slots_for_modification(date_str, exclude_reservation_id, staff_name)
        
        
        # Filter slots that can accommodate the service duration
        suitable_slots = []
        for slot in all_slots:
            slot_duration = self._calculate_slot_duration(slot)
            if slot_duration >= service_duration:
                suitable_slots.append(slot)
        
        return suitable_slots
    
    def _calculate_slot_duration(self, slot: Dict) -> int:
        """Calculate duration of a time slot in minutes"""
        try:
            normalized_start = self._normalize_time_format(slot["time"])
            normalized_end = self._normalize_time_format(slot["end_time"])
            if not normalized_start or not normalized_end:
                return 0
            start_time = datetime.strptime(normalized_start, "%H:%M")
            end_time = datetime.strptime(normalized_end, "%H:%M")
            duration = (end_time - start_time).total_seconds() / 60
            return int(duration)
        except Exception:
            return 0
    
    def validate_service_time_compatibility(self, date_str: str, time_slot: str, service_name: str) -> bool:
        """Validate if a time slot can accommodate a service duration"""
        service_duration = self._get_service_duration_minutes(service_name)
        
        # Parse time slot (assuming format like "10:00~11:00")
        try:
            if "~" in time_slot:
                start_time, end_time = time_slot.split("~")
                normalized_start = self._normalize_time_format(start_time.strip())
                normalized_end = self._normalize_time_format(end_time.strip())
                if not normalized_start or not normalized_end:
                    return False
                start_dt = datetime.strptime(normalized_start, "%H:%M")
                end_dt = datetime.strptime(normalized_end, "%H:%M")
                slot_duration = (end_dt - start_dt).total_seconds() / 60
                return slot_duration >= service_duration
        except Exception:
            pass
        
        return False
    
    def get_reservation_by_id(self, reservation_id: str, staff_name: str = None) -> Optional[Dict]:
        """Get reservation details by reservation ID from staff-specific calendar"""
        if not self.service:
            return None
        
        try:
            # If staff_name is provided, search only in that staff's calendar
            if staff_name:
                calendar_id = self._get_staff_calendar_id(staff_name)
                if not calendar_id:
                    print(f"Staff calendar ID not found for {staff_name}")
                    return None
                
                events_result = self.service.events().list(
                    calendarId=calendar_id,
                    timeMin=datetime.now().isoformat() + 'Z',
                    maxResults=100,
                    singleEvents=True,
                    orderBy='startTime'
                ).execute()
                
                events = events_result.get('items', [])
                
                for event in events:
                    description = event.get('description', '')
                    if reservation_id in description:
                        return event
                
                return None
            else:
                # Search in all staff calendars (fallback for backward compatibility)
                # First, try default calendar
                if self.calendar_id:
                    events_result = self.service.events().list(
                        calendarId=self.calendar_id,
                        timeMin=datetime.now().isoformat() + 'Z',
                        maxResults=100,
                        singleEvents=True,
                        orderBy='startTime'
                    ).execute()
                    
                    events = events_result.get('items', [])
                    
                    for event in events:
                        description = event.get('description', '')
                        if reservation_id in description:
                            return event
                
                # If not found, search in all staff calendars
                for staff_id, staff_data in self.staff_data.items():
                    staff_calendar_id = self._get_staff_calendar_id(staff_data.get("name"))
                    if not staff_calendar_id:
                        continue
                    
                    try:
                        events_result = self.service.events().list(
                            calendarId=staff_calendar_id,
                            timeMin=datetime.now().isoformat() + 'Z',
                            maxResults=100,
                            singleEvents=True,
                            orderBy='startTime'
                        ).execute()
                        
                        events = events_result.get('items', [])
                        
                        for event in events:
                            description = event.get('description', '')
                            if reservation_id in description:
                                return event
                    except Exception as e:
                        print(f"Error searching calendar {staff_calendar_id}: {e}")
                        continue
                
                return None
            
        except Exception as e:
            print(f"Failed to get reservation by ID {reservation_id}: {e}")
            return None
    
    def cancel_reservation_by_id(self, reservation_id: str, staff_name: str = None) -> bool:
        """Cancel a reservation by reservation ID from staff-specific calendar"""
        try:
            # Find the event with the reservation ID
            event = self.get_reservation_by_id(reservation_id, staff_name)
            
            if not event:
                print(f"Reservation {reservation_id} not found in calendar")
                return False
            
            # Extract staff name from event if not provided
            if not staff_name:
                summary = event.get('summary', '')
                try:
                    import re
                    m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                    if m:
                        staff_name = m.group(3)
                except Exception:
                    pass
            
            # Get staff-specific calendar ID
            staff_calendar_id = self._get_staff_calendar_id(staff_name) if staff_name else self.calendar_id
            if not staff_calendar_id:
                print(f"Staff calendar ID not found for {staff_name}")
                return False
            
            # Delete the event from staff-specific calendar
            self.service.events().delete(
                calendarId=staff_calendar_id,
                eventId=event['id']
            ).execute()
            
            print(f"Successfully cancelled reservation {reservation_id}")
            return True
            
        except Exception as e:
            print(f"Failed to cancel reservation {reservation_id}: {e}")
            return False

    def _get_staff_email(self, staff_name: str) -> Optional[str]:
        """Get staff email from mapping"""
        # Find staff by name in the staff data
        for staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                email_env = staff_data.get("email_env")
                if email_env:
                    return os.getenv(email_env)
                break
        return None
    
    def _get_staff_calendar_id(self, staff_name: str) -> Optional[str]:
        """Get staff calendar ID from services.json. Returns None if not found or staff is '未指定'."""
        if not staff_name or staff_name == "未指定":
            return self.calendar_id  # fallback if needed

        for staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                calendar_id = staff_data.get("calendar_id")
                if calendar_id:
                    return calendar_id  # Use directly from JSON
                break

        print(f"Warning: Calendar ID not found for staff '{staff_name}', using default calendar")
        return self.calendar_id
    
    def _get_staff_color_id(self, staff_name: str) -> Optional[str]:
        """Get staff color ID from mapping"""
        # Find staff by name in the staff data
        for staff_id, staff_data in self.staff_data.items():
            if staff_data.get("name") == staff_name:
                return staff_data.get("color_id")
        return None
    
    def _filter_events_by_staff(self, events: List[Dict], staff_name: str) -> List[Dict]:
        """Filter events to only include those for a specific staff member"""
        if not events:
            return []
        
        filtered_events = []
        for event in events:
            summary = event.get('summary', '') or ''
            # Expected format: "[予約] SERVICE - CLIENT (STAFF)"
            try:
                import re
                m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                if m:
                    event_staff = m.group(3)
                    if event_staff == staff_name:
                        filtered_events.append(event)
            except Exception:
                # If parsing fails, skip this event
                continue
        
        return filtered_events
    
    def check_staff_availability_for_time(self, date_str: str, start_time: str, end_time: str, staff_name: str, exclude_reservation_id: str = None) -> bool:
        """Check if a staff member is available for a specific time period"""
        try:
            # Get all events for the date from staff-specific calendar
            all_events = self.get_events_for_date(date_str, staff_name)
            
            # No need to filter by staff since we're already querying staff-specific calendar
            staff_events = all_events
            
            # Parse the requested time period
            from datetime import datetime
            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
            
            # Check for overlaps with existing appointments
            for event in staff_events:
                # Skip the reservation being modified
                if exclude_reservation_id:
                    description = event.get('description', '')
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue
                
                event_start_str = event.get('start', {}).get('dateTime', '')
                event_end_str = event.get('end', {}).get('dateTime', '')
                
                if event_start_str and event_end_str:
                    # Parse event times
                    event_start = datetime.fromisoformat(event_start_str.replace('Z', '+00:00'))
                    event_end = datetime.fromisoformat(event_end_str.replace('Z', '+00:00'))
                    
                    # Convert to local time for comparison
                    import pytz
                    tz = pytz.timezone(self.timezone)
                    event_start = event_start.astimezone(tz).replace(tzinfo=None)
                    event_end = event_end.astimezone(tz).replace(tzinfo=None)
                    
                    # Check for overlap
                    if (start_datetime < event_end and end_datetime > event_start):
                        return False  # Time conflict found
            
            return True  # No conflicts found
            
        except Exception as e:
            print(f"Error checking staff availability: {e}")
            return False
    
    def check_service_change_overlap(self, date_str: str, start_time: str, new_service: str, staff_name: str, exclude_reservation_id: str = None) -> tuple:
        """
        Check if changing to a new service would cause time overlaps for the staff member.
        Returns (is_available, new_end_time, conflict_info)
        """
        try:
            from datetime import datetime, timedelta
            
            # Get service duration
            service_duration = self._get_service_duration_minutes(new_service)
            
            # Calculate new end time based on start time and service duration
            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            new_end_datetime = start_datetime + timedelta(minutes=service_duration)
            new_end_time = new_end_datetime.strftime("%H:%M")
            
            # Check if the new time period would overlap with other appointments
            is_available = self.check_staff_availability_for_time(
                date_str, start_time, new_end_time, staff_name, exclude_reservation_id
            )
            
            # Get conflict information if there's an overlap
            conflict_info = None
            if not is_available:
                conflict_info = self._get_conflict_details(
                    date_str, start_time, new_end_time, staff_name, exclude_reservation_id
                )
            
            return is_available, new_end_time, conflict_info
            
        except Exception as e:
            print(f"Error checking service change overlap: {e}")
            return False, start_time, None
    
    def check_user_time_conflict(self, date_str: str, start_time: str, end_time: str, user_id: str, exclude_reservation_id: str = None, staff_name: str = None) -> bool:
        """
        Check if a user already has a reservation at the same time.
        Returns True if there's a conflict (user already has a reservation at this time).
        Note: This checks across all staff calendars since a user can't have multiple reservations at the same time regardless of staff.
        """
        try:
            from datetime import datetime
            
            # Get all events for the date from all staff calendars
            # Since a user can't have multiple reservations at the same time regardless of staff,
            # we need to check all calendars
            all_events = []
            
            # Check default calendar if exists
            if self.calendar_id:
                default_events = self.get_events_for_date(date_str, None)
                all_events.extend(default_events)
            
            # Check all staff calendars
            for staff_id, staff_data in self.staff_data.items():
                staff_name_check = staff_data.get("name")
                if staff_name_check:
                    staff_events = self.get_events_for_date(date_str, staff_name_check)
                    all_events.extend(staff_events)
            print("[User Time Conflict] All events:", all_events)
            # Parse the requested time period
            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
            print("[User Time Conflict] Start datetime:", start_datetime)
            print("[User Time Conflict] End datetime:", end_datetime)
            # Check for overlaps with user's existing reservations
            for event in all_events:
                # Skip the reservation being modified
                if exclude_reservation_id:
                    description = event.get('description', '')
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue
                
                # Check if this event belongs to the same user
                if self._is_user_reservation(event, user_id):
                    event_start_str = event.get('start', {}).get('dateTime', '')
                    event_end_str = event.get('end', {}).get('dateTime', '')
                    
                    if event_start_str and event_end_str:
                        # Parse event times
                        event_start = datetime.fromisoformat(event_start_str.replace('Z', '+00:00'))
                        event_end = datetime.fromisoformat(event_end_str.replace('Z', '+00:00'))
                        
                        # Convert to local time for comparison
                        import pytz
                        tz = pytz.timezone(self.timezone)
                        event_start = event_start.astimezone(tz).replace(tzinfo=None)
                        event_end = event_end.astimezone(tz).replace(tzinfo=None)
                        
                        # Check for overlap
                        if (start_datetime < event_end and end_datetime > event_start):
                            return True  # Time conflict found
            
            return False  # No conflicts found
            
        except Exception as e:
            print(f"Error checking user time conflict: {e}")
            return True  # Assume conflict if error occurs (safer approach)
    
    def _is_user_reservation(self, event: Dict, user_id: str) -> bool:
        """Check if an event belongs to a specific user"""
        try:
            # Try to get user ID from event description
            description = event.get('description', '')
            if 'User ID:' in description:
                event_user_id = description.split('User ID:')[1].split('\n')[0].strip()
                return event_user_id == user_id
            
            # If no user ID in description, we can't determine ownership
            # This might happen for older reservations
            return False
            
        except Exception as e:
            print(f"Error checking if event belongs to user: {e}")
            return False
    
    def _get_conflict_details(self, date_str: str, start_time: str, end_time: str, staff_name: str, exclude_reservation_id: str = None) -> dict:
        """Get details about conflicting appointments"""
        try:
            from datetime import datetime
            
            # Get all events for the date from staff-specific calendar
            all_events = self.get_events_for_date(date_str, staff_name)
            
            # No need to filter by staff since we're already querying staff-specific calendar
            staff_events = all_events
            
            # Parse the requested time period
            start_datetime = datetime.strptime(f"{date_str} {start_time}", "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(f"{date_str} {end_time}", "%Y-%m-%d %H:%M")
            
            # Find conflicting appointments
            conflicts = []
            for event in staff_events:
                # Skip the reservation being modified
                if exclude_reservation_id:
                    description = event.get('description', '')
                    if f"予約ID: {exclude_reservation_id}" in description:
                        continue
                
                event_start_str = event.get('start', {}).get('dateTime', '')
                event_end_str = event.get('end', {}).get('dateTime', '')
                
                if event_start_str and event_end_str:
                    # Parse event times
                    event_start = datetime.fromisoformat(event_start_str.replace('Z', '+00:00'))
                    event_end = datetime.fromisoformat(event_end_str.replace('Z', '+00:00'))
                    
                    # Convert to local time for comparison
                    import pytz
                    tz = pytz.timezone(self.timezone)
                    event_start = event_start.astimezone(tz).replace(tzinfo=None)
                    event_end = event_end.astimezone(tz).replace(tzinfo=None)
                    
                    # Check for overlap
                    if (start_datetime < event_end and end_datetime > event_start):
                        # Extract client name from summary
                        summary = event.get('summary', '')
                        client_name = "Unknown"
                        try:
                            import re
                            m = re.search(r"^\[予約\] (.+) - (.+) \((.+)\)$", summary)
                            if m:
                                client_name = m.group(2)
                        except Exception:
                            pass
                        
                        conflicts.append({
                            'client': client_name,
                            'start_time': event_start.strftime("%H:%M"),
                            'end_time': event_end.strftime("%H:%M"),
                            'summary': summary
                        })
            
            return {
                'conflicts': conflicts,
                'staff_name': staff_name,
                'requested_time': f"{start_time}~{end_time}"
            }
            
        except Exception as e:
            print(f"Error getting conflict details: {e}")
            return None


if __name__ == "__main__":
    google_calendar = GoogleCalendarHelper()
    print(google_calendar.get_available_slots_for_modification("2025-10-16", "RES-20251016-1611"))