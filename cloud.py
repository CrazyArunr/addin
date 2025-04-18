from typing import Optional, Dict, Any, List
import requests
from datetime import datetime, timedelta, timezone
from time import sleep
import pytz
from msal import ConfidentialClientApplication
import json
from fuzzywuzzy import fuzz
from dateutil import parser
import pyodbc
import logging
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

# Initialize FastAPI app
app = FastAPI(title="Meeting Management System",
             description="API for managing and matching meeting data")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
CLIENT_ID = "46006a21-36a1-44cd-a75a-1965799fcdd8"
CLIENT_SECRET = "aNW8Q~2HjwxO2RAaLSUdrY7zoB4FvrpEGFJyebx_"
TENANT_ID = "99fa199d-2653-4e16-bd65-17cc244b425e"
SCOPES = ["https://graph.microsoft.com/.default"]
LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')
BATCH_SIZE = 5
REQUEST_DELAY = 2

# Azure SQL Database configuration
AZURE_SQL_CONFIG = {
    'server': 'tcp:decision.database.windows.net,1433',
    'database': 'finalDecision',
    'username': 'priyank',
    'password': '530228@mka',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Pydantic Models
class MeetingData(BaseModel):
    organizer: str
    subject: str
    start: str
    end: str
    organizer_email: Optional[str] = ""
    meeting_type: Optional[str] = "Undefined"
    enable_mom: Optional[str] = "Minutes Required"
    preview: Optional[str] = ""
    location: Optional[str] = ""
    isOnlineMeeting: Optional[bool] = False
    join_url: Optional[str] = ""
    attendees: Optional[List[str]] = []

class MeetingResponse(BaseModel):
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None

# Helper Functions
def get_db_connection():
    """Establish connection to Azure SQL Database"""
    conn_str = f"""
    DRIVER={AZURE_SQL_CONFIG['driver']};
    SERVER={AZURE_SQL_CONFIG['server']};
    DATABASE={AZURE_SQL_CONFIG['database']};
    UID={AZURE_SQL_CONFIG['username']};
    PWD={AZURE_SQL_CONFIG['password']};
    Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
    """
    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Database connection established successfully")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not connect to database"
        )

def create_table_if_not_exists(cursor):
    """Ensure the MatchedMeetings table exists with proper schema"""
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MatchedMeetings')
    BEGIN
        CREATE TABLE MatchedMeetings (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            GraphMeetingID NVARCHAR(255) NOT NULL UNIQUE,
            Organizer NVARCHAR(255) NOT NULL,
            OrganizerEmail NVARCHAR(255),
            Subject NVARCHAR(MAX),
            StartTime DATETIME2,
            EndTime DATETIME2,
            MeetingType NVARCHAR(100) DEFAULT 'Undefined',
            EnableMoM NVARCHAR(20) DEFAULT 'Minutes Required',
            BodyPreview NVARCHAR(MAX),
            Location NVARCHAR(255),
            IsOnlineMeeting BIT,
            JoinUrl NVARCHAR(MAX),
            Attendees NVARCHAR(MAX),
            CreatedAt DATETIME DEFAULT GETDATE()
        )
    END
    """
    try:
        cursor.execute(create_table_query)
        cursor.commit()
        logger.info("Verified/updated database table")
    except pyodbc.Error as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

# Authentication Functions
def get_access_token():
    """Authenticate and get Microsoft Graph access token"""
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPES)
    
    if "access_token" not in result:
        logger.error(f"Authentication failed: {result.get('error_description')}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Microsoft Graph authentication failed"
        )
    
    return result["access_token"]

def get_meeting_options() -> Optional[Dict[str, Any]]:
    """Get available meeting options from the API"""
    url = "https://add-in-gvbvabchhdf6h3ez.centralindia-01.azurewebsites.net/save-meeting/"

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    payload = {
        "meetingType": "swapnil",
        "enableMom": "Minutes required",
        "subject": "Weekly Update",
        "body": "This is a sample meeting for weekly updates.",
        "organizer": "youremail@example.com",
        "startTimeUtc": "2025-04-10 10:00:00",
        "endTimeUtc": "2025-04-10 11:00:00"
    }

    try:
        logger.info("Fetching meeting options")

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=10
        )

        logger.debug(f"Response status: {response.status_code}")
        response.raise_for_status()

        data = response.json()
        if data.get('status') == 'success':
            logger.info(f"Retrieved {len(data.get('data', []))} meeting options")
            return data
        else:
            logger.error(f"API returned unsuccessful status: {data.get('message', 'Unknown error')}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return None

# Meeting Processing Functions
def format_meeting_data(meetings: List[Dict]) -> List[Dict]:
    """Format raw meeting data into standardized structure"""
    formatted = []
    for meeting in meetings or []:
        if not meeting or not isinstance(meeting, dict):
            continue
            
        try:
            meeting_id = meeting.get("id")
            if not meeting_id:
                continue
                
            start = (meeting.get("start", {}) or {}).get("dateTime")
            end = (meeting.get("end", {}) or {}).get("dateTime")
            
            if not start or not end:
                continue
                
            organizer_email = ((meeting.get("organizer", {}) or {}).get("emailAddress", {}) or {}).get("address")
            
            formatted.append({
                "meeting_id": meeting_id,
                "subject": meeting.get("subject", "No Subject"),
                "start": start,
                "end": end,
                "organizer": ((meeting.get("organizer", {}) or {}).get("emailAddress", {}) or {}).get("name"),
                "organizer_email": organizer_email,
                "join_url": (meeting.get("onlineMeeting", {}) or {}).get("joinUrl") or 
                            meeting.get("webLink"),
                "location": (meeting.get("location", {}) or {}).get("displayName"),
                "attendees": [
                    (attendee.get("emailAddress", {}) or {}).get("name")
                    for attendee in (meeting.get("attendees", []) or [])
                    if attendee and isinstance(attendee, dict)
                ],
                "preview": meeting.get("bodyPreview", ""),
                "isOnlineMeeting": meeting.get("isOnlineMeeting", False)
            })
        except Exception as e:
            logger.error(f"Error formatting meeting: {str(e)}")
            continue
            
    return formatted

def get_user_meetings(token: str, user_id: str, hours: int = 1) -> List[Dict]:
    """Get calendar events for a user from last X hours"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": 'outlook.timezone="UTC"'
    }
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(hours=hours)
    
    params = {
        "$select": "subject,start,end,organizer,webLink,location,attendees,bodyPreview,isOnlineMeeting,onlineMeeting",
        "$filter": f"createdDateTime ge {start_date.isoformat()}",
        "$top": 100
    }
    
    meetings = []
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/calendar/events"
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"API error {response.status_code} for {user_id}")
            return []
            
        data = response.json()
        meetings.extend([m for m in data.get("value", []) if m is not None])
        
        # Handle pagination
        next_link = data.get("@odata.nextLink")
        while next_link:
            response = requests.get(next_link, headers=headers, timeout=30)
            if response.status_code == 200:
                page_data = response.json()
                meetings.extend([m for m in page_data.get("value", []) if m is not None])
                next_link = page_data.get("@odata.nextLink")
            else:
                break
                
    except Exception as e:
        logger.error(f"Error getting meetings for {user_id}: {str(e)}")
    
    return meetings

def get_all_users(token):
    """Get all active users with mailboxes"""
    headers = {
        "Authorization": f"Bearer {token}",
        "ConsistencyLevel": "eventual"
    }
    url = "https://graph.microsoft.com/v1.0/users"
    params = {
        "$select": "id,mail,userPrincipalName,userType,mailNickname,accountEnabled",
        "$filter": "accountEnabled eq true",
        "$top": 999
    }
    all_users = []
    
    try:
        while url:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Filter for users likely to have cloud mailboxes
            valid_users = [
                u for u in data.get("value", [])
                if (u.get("mail") and 
                    u.get("accountEnabled", False) and
                    not u.get("mailNickname", "").endswith("#EXT#"))
            ]
            all_users.extend(valid_users)
            url = data.get("@odata.nextLink")
            params = None
        
        return [u["userPrincipalName"] for u in all_users]
    except Exception as e:
        print(f"❌ Error fetching users: {e}")
        return []

def get_organizer_meetings(token: str, organizer_email: str, hours: int = 1) -> List[Dict]:
    """Get all meetings organized by specific user from last X hours"""
    all_meetings = []
    users = get_all_users(token)
    
    for i in range(0, len(users), BATCH_SIZE):
        batch = users[i:i+BATCH_SIZE]
        
        for user_id in batch:
            try:
                meetings = get_user_meetings(token, user_id, hours)
                all_meetings.extend([
                    m for m in meetings
                    if ((m.get("organizer", {}) or {}).get("emailAddress", {}) or {}).get("address", "").lower() == organizer_email.lower()
                ])
            except Exception as e:
                logger.error(f"Error processing user {user_id}: {str(e)}")
        
        if i + BATCH_SIZE < len(users):
            sleep(REQUEST_DELAY)
    
    return all_meetings

@app.get("/")
async def read_root():
    return {"message": "Hello, FastAPI on Docker!"}

# API Endpoints
@app.post("/save-meeting/", response_model=MeetingResponse)
async def save_meeting(meeting_data: MeetingData):
    """Save meeting data to database after matching with Microsoft Graph"""
    try:
        # Validate time fields
        logger.info(f"Processing meeting: {meeting_data.subject}")
        
        start_time = parse_api_time(meeting_data.start)
        end_time = parse_api_time(meeting_data.end)
        
        if not start_time or not end_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid time format"
            )
        
        # Authenticate with Microsoft Graph
        token = get_access_token()
        
        # Find matching meeting from Graph API
        matched_meeting = find_matching_meeting(token, meeting_data)
        
        # Get meeting options from second API
        meeting_options = get_meeting_options()
        
        # Determine meeting type and MoM setting
        if matched_meeting:
            # If we found a match in Graph API, try to find matching options
            matched_options = find_matching_options(matched_meeting, meeting_options)
            
            meeting_type = (
                matched_options.get('meeting_type') 
                if matched_options and matched_options.get('meeting_type')
                else meeting_data.meeting_type
            )
            
            enable_mom = (
                matched_options.get('enable_mom') 
                if matched_options and matched_options.get('enable_mom')
                else meeting_data.enable_mom
            )
        else:
            # No match found, use default values
            meeting_type = meeting_data.meeting_type
            enable_mom = meeting_data.enable_mom
        
        # Prepare data for database
        db_data = {
            'graph_meeting_id': matched_meeting.get('meeting_id') if matched_meeting else None,
            'organizer': matched_meeting.get('organizer') if matched_meeting else meeting_data.organizer,
            'organizer_email': matched_meeting.get('organizer_email') if matched_meeting else meeting_data.organizer_email,
            'subject': matched_meeting.get('subject') if matched_meeting else meeting_data.subject,
            'start': start_time.isoformat(),
            'end': end_time.isoformat(),
            'meeting_type': meeting_type,
            'enable_mom': enable_mom,
            'preview': matched_meeting.get('preview') if matched_meeting else meeting_data.preview,
            'location': matched_meeting.get('location') if matched_meeting else meeting_data.location,
            'isOnlineMeeting': matched_meeting.get('isOnlineMeeting', False) if matched_meeting else meeting_data.isOnlineMeeting,
            'join_url': matched_meeting.get('join_url') if matched_meeting else meeting_data.join_url,
            'attendees': matched_meeting.get('attendees', []) if matched_meeting else meeting_data.attendees
        }
        
        # Save to database
        conn = get_db_connection()
        cursor = conn.cursor()
        create_table_if_not_exists(cursor)
        
        success = store_or_update_meeting(cursor, db_data, is_match=bool(matched_meeting))
        conn.close()
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to save meeting"
            )
            
        return {
            "status": "success",
            "message": "Meeting saved successfully",
            "data": db_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )

def find_matching_options(graph_meeting: Dict, meeting_options: Dict) -> Optional[Dict]:
    """Find matching meeting options from the second API"""
    if not meeting_options or not meeting_options.get('data'):
        return None
    
    best_match = None
    best_score = 0
    
    for option in meeting_options['data']:
        if not isinstance(option, dict):
            continue
            
        # Compare organizer email
        organizer_match = (
            extract_email(str(option.get('organizer', ''))).lower() == 
            extract_email(str(graph_meeting.get('organizer_email', ''))).lower()
        )
        
        if not organizer_match:
            continue
            
        # Compare subject similarity
        subject_similarity = fuzz.ratio(
            normalize_text(option.get('subject', '')),
            normalize_text(graph_meeting.get('subject', ''))
        ) / 100.0
        
        # Compare body similarity
        body_similarity = fuzz.token_set_ratio(
            normalize_text(option.get('body', '')),
            normalize_text(graph_meeting.get('preview', ''))
        ) / 100.0
        
        # Compare time (within 15 minutes)
        option_start = parse_api_time(option.get('startTimeUtc'))
        graph_start = safe_parse_datetime(graph_meeting.get('start'))
        time_match = False
        
        if option_start and graph_start:
            time_diff = abs((option_start - graph_start).total_seconds())
            time_match = time_diff <= 900  # 15 minutes
            
        # Calculate overall score
        score = (
            0.3 * subject_similarity +
            0.2 * body_similarity +
            0.3 * (1 if organizer_match else 0) +
            0.2 * (1 if time_match else 0)
        )
        
        if score > best_score:
            best_score = score
            best_match = {
                'meeting_type': option.get('meetingType'),
                'enable_mom': option.get('enableMom'),
                'score': score
            }
    
    # Only return if we have a good enough match
    return best_match if best_score >= 0.7 else None

def find_matching_meeting(token: str, meeting_data: MeetingData) -> Optional[Dict]:
    """Find exact matching meeting in Microsoft Graph based on subject and time"""
    try:
        # Get the organizer's email
        organizer_email = meeting_data.organizer_email
        if not organizer_email:
            logger.error("No organizer email provided")
            return None
        
        # Get time range window to search
        meeting_start = parse_api_time(meeting_data.start)
        meeting_end = parse_api_time(meeting_data.end)
        
        if not meeting_start or not meeting_end:
            logger.error("Invalid meeting time format")
            return None
        
        # Create time window (1 day before and after)
        search_start = meeting_start - timedelta(days=1)
        search_end = meeting_end + timedelta(days=1)
        
        # Format dates for Graph API
        search_start_str = search_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        search_end_str = search_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Get all users
        users = get_all_users(token)
        
        # Find the meeting across all users' calendars
        best_match = None
        best_score = 0
        
        for i in range(0, len(users), BATCH_SIZE):
            batch = users[i:i+BATCH_SIZE]
            
            for user_id in batch:
                try:
                    # Search for meetings in the time window
                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Prefer": 'outlook.timezone="UTC"'
                    }
                    
                    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/calendar/events"
                    params = {
                        "$select": "id,subject,start,end,organizer,webLink,location,attendees,bodyPreview,isOnlineMeeting,onlineMeeting",
                        "$filter": f"start/dateTime ge '{search_start_str}' and end/dateTime le '{search_end_str}'",
                        "$top": 50
                    }
                    
                    response = requests.get(url, headers=headers, params=params, timeout=30)
                    
                    if response.status_code != 200:
                        logger.warning(f"API error {response.status_code} for {user_id}")
                        continue
                        
                    meetings = response.json().get("value", [])
                    
                    # Look for matching meetings
                    for meeting in meetings:
                        # Skip if not organized by the target organizer
                        meeting_organizer_email = ((meeting.get("organizer", {}) or {}).get("emailAddress", {}) or {}).get("address", "").lower()
                        if meeting_organizer_email != organizer_email.lower():
                            continue
                        
                        # Check subject similarity
                        subject_similarity = fuzz.ratio(
                            normalize_text(meeting.get("subject", "")), 
                            normalize_text(meeting_data.subject)
                        ) / 100.0
                        
                        # Check body similarity if preview is available
                        body_similarity = fuzz.token_set_ratio(
                            normalize_text(meeting.get("bodyPreview", "")), 
                            normalize_text(meeting_data.preview or "")
                        ) / 100.0
                        
                        # Check time proximity
                        meeting_start_time = safe_parse_datetime((meeting.get("start", {}) or {}).get("dateTime"))
                        meeting_end_time = safe_parse_datetime((meeting.get("end", {}) or {}).get("dateTime"))
                        
                        time_match = False
                        if meeting_start_time and meeting_end_time:
                            start_diff = abs((meeting_start_time - meeting_start).total_seconds())
                            end_diff = abs((meeting_end_time - meeting_end).total_seconds())
                            # Allow small time differences (within 15 minutes)
                            time_match = start_diff <= 900 and end_diff <= 900
                        
                        # Calculate overall match score
                        score = (
                            0.5 * subject_similarity +  # Higher weight for subject
                            0.2 * body_similarity +
                            (0.3 if time_match else 0)  # Bonus for time match
                        )
                        
                        # Update best match if this one is better
                        if score > best_score:
                            best_score = score
                            
                            # Format the meeting data
                            best_match = {
                                "meeting_id": meeting.get("id"),
                                "subject": meeting.get("subject", "No Subject"),
                                "start": (meeting.get("start", {}) or {}).get("dateTime"),
                                "end": (meeting.get("end", {}) or {}).get("dateTime"),
                                "organizer": ((meeting.get("organizer", {}) or {}).get("emailAddress", {}) or {}).get("name"),
                                "organizer_email": meeting_organizer_email,
                                "join_url": ((meeting.get("onlineMeeting", {}) or {}).get("joinUrl") or 
                                            meeting.get("webLink")),
                                "location": ((meeting.get("location", {}) or {}).get("displayName")),
                                "attendees": [
                                    (attendee.get("emailAddress", {}) or {}).get("name")
                                    for attendee in (meeting.get("attendees", []) or [])
                                    if attendee and isinstance(attendee, dict)
                                ],
                                "preview": meeting.get("bodyPreview", ""),
                                "isOnlineMeeting": meeting.get("isOnlineMeeting", False)
                            }
                
                except Exception as e:
                    logger.error(f"Error searching meetings for {user_id}: {str(e)}")
                    continue
            
            # Add delay between batches
            if i + BATCH_SIZE < len(users):
                sleep(REQUEST_DELAY)
        
        # Consider a match valid only if the score is high enough
        if best_score >= 0.7:
            logger.info(f"Found matching meeting with score {best_score}: {best_match.get('subject')}")
            return best_match
        else:
            logger.warning(f"No suitable match found. Best score was {best_score}")
            return None
            
    except Exception as e:
        logger.error(f"Error finding matching meeting: {str(e)}")
        return None

def store_or_update_meeting(cursor, meeting_data: Dict, is_match: bool = False) -> bool:
    """Upsert meeting data into database"""
    try:
        start_time = safe_parse_datetime(meeting_data.get('start'))
        end_time = safe_parse_datetime(meeting_data.get('end'))
        
        # Convert attendees list to JSON string for storage
        attendees = meeting_data.get('attendees', [])
        attendees_json = json.dumps(attendees) if attendees else None
        
        # Handle case where graph_meeting_id might be None
        graph_meeting_id = meeting_data.get('graph_meeting_id')
        if not graph_meeting_id:
            # Create a stable temporary ID using a string representation of relevant fields
            temp_id_data = {
                'organizer': meeting_data.get('organizer', ''),
                'subject': meeting_data.get('subject', ''),
                'start': str(start_time),
                'end': str(end_time)
            }
            graph_meeting_id = f"temp_{hash(frozenset(temp_id_data.items()))}"
        
        query = """
        MERGE INTO MatchedMeetings AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            GraphMeetingID, Organizer, OrganizerEmail, Subject, 
            StartTime, EndTime, MeetingType, EnableMoM, BodyPreview,
            Location, IsOnlineMeeting, JoinUrl, Attendees
        )
        ON target.GraphMeetingID = source.GraphMeetingID
        WHEN MATCHED THEN
            UPDATE SET 
                Organizer = source.Organizer,
                OrganizerEmail = source.OrganizerEmail,
                Subject = source.Subject,
                StartTime = source.StartTime,
                EndTime = source.EndTime,
                StartTimeUTC = source.StartTime,
                EndTimeUTC = source.EndTime,
                MeetingType = COALESCE(source.MeetingType, 'Undefined'),
                EnableMoM = COALESCE(source.EnableMoM, 'Minutes Required'),
                BodyPreview = source.BodyPreview,
                Location = source.Location,
                IsOnlineMeeting = source.IsOnlineMeeting,
                JoinUrl = source.JoinUrl,
                Attendees = source.Attendees,
                CreatedAt = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (
                GraphMeetingID, Organizer, OrganizerEmail, Subject,
                StartTime, EndTime, StartTimeUTC, EndTimeUTC,
                MeetingType, EnableMoM, BodyPreview,
                Location, IsOnlineMeeting, JoinUrl, Attendees
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                COALESCE(?, 'Undefined'), 
                COALESCE(?, 'Minutes Required'), 
                ?, ?, ?, ?, ?
            );
        """
        params = [
            graph_meeting_id,
            meeting_data.get('organizer', ''),
            meeting_data.get('organizer_email', ''),
            meeting_data.get('subject', ''),
            start_time,
            end_time,
            meeting_data.get('meeting_type'),
            meeting_data.get('enable_mom'),
            meeting_data.get('preview', ''),
            meeting_data.get('location',''),
            1 if meeting_data.get('isOnlineMeeting', False) else 0,
            meeting_data.get('join_url', ''),
            attendees_json,
            graph_meeting_id,
            meeting_data.get('organizer', ''),
            meeting_data.get('organizer_email', ''),
            meeting_data.get('subject', ''),
            start_time,
            end_time,
            start_time,
            end_time,
            meeting_data.get('meeting_type'),
            meeting_data.get('enable_mom'),
            meeting_data.get('preview', ''),
            meeting_data.get('location', ''),
            1 if meeting_data.get('isOnlineMeeting', False) else 0,
            meeting_data.get('join_url', ''),
            attendees_json
        ]
        
        cursor.execute(query, params)
        cursor.commit()
        return True
        
    except pyodbc.Error as e:
        logger.error(f"Database error: {str(e)}")
        cursor.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error in store_or_update_meeting: {str(e)}")
        cursor.rollback()
        return False

# Time Handling Utilities
def safe_parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Safely parse datetime string"""
    if not dt_str:
        return None
    try:
        dt = parser.parse(dt_str)
        return ensure_utc(dt)
    except Exception:
        return None

def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensure datetime is UTC timezone aware"""
    if not dt:
        return None
    if dt.tzinfo is None:
        return pytz.utc.localize(dt)
    return dt.astimezone(pytz.UTC)

def parse_api_time(time_str: Optional[str]) -> Optional[datetime]:
    """Parse time string from API with timezone handling"""
    if not time_str:
        return None
    try:
        dt = parser.parse(time_str)
        if dt.tzinfo is None:
            dt = LOCAL_TIMEZONE.localize(dt)
        return dt.astimezone(pytz.UTC)
    except Exception:
        return None

# Text Processing Utilities
def normalize_text(text: Optional[str]) -> str:
    """Normalize text for comparison"""
    if not text:
        return ""
    return str(text).lower().strip()

def extract_email(text: str) -> str:
    """Extract email address from text"""
    text = normalize_text(text)
    if "<" in text and ">" in text:
        return text.split("<")[-1].split(">")[0]
    return text

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)