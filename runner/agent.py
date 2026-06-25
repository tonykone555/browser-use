import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from steel import Steel
from browser_use import Agent, Browser, BrowserConfig
from langchain_groq import ChatGroq

# ============================================================
# Firebase
# ============================================================
if not firebase_admin._apps:
    cred = credentials.Certificate({
        "type": "service_account",
        "project_id": os.environ["FIREBASE_PROJECT_ID"],
        "client_email": os.environ["FIREBASE_CLIENT_EMAIL"],
        "private_key": os.environ["FIREBASE_PRIVATE_KEY"].replace("\\n", "\n"),
        "token_uri": "https://oauth2.googleapis.com/token",
    })
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ============================================================
# Logging
# ============================================================
def log(task_id: str, msg: str, log_type: str = "info"):
    print(f"[{log_type.upper()}] {msg}")
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "msg": msg,
        "type": log_type,
        "timestamp": int(datetime.now().timestamp() * 1000),
    }
    try:
        task_ref = db.collection("assix_tasks").document(task_id)
        task_data = task_ref.get().to_dict() or {}
        recent_logs = task_data.get("recentLogs", [])
        recent_logs.append(entry)
        if len(recent_logs) > 100:
            recent_logs = recent_logs[-100:]
        task_ref.update({"recentLogs": recent_logs})
    except Exception as e:
        print(f"Log error: {e}")

# ============================================================
# Lead saving
# ============================================================
def format_phone(raw: str) -> str:
    if not raw: return ""
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("1") and len(digits) == 11: return "+" + digits
    if len(digits) == 10: return "+1" + digits
    if len(digits) > 10: return "+1" + digits[-10:]
    return raw

def save_lead(task_id: str, item: dict, platform: str):
    phone = format_phone(item.get("phone", ""))
    if not phone or len(phone) < 7: return
    try:
        existing = db.collection("leads").where("phone", "==", phone).limit(1).get()
        if len(existing) > 0: return
        db.collection("leads").add({
            "taskId": task_id,
            "businessName": item.get("name", "Unknown"),
            "phone": phone,
            "email": item.get("email", ""),
            "website": item.get("website", ""),
            "address": item.get("address", ""),
            "city": item.get("city", ""),
            "source": platform,
            "createdAt": datetime.now().isoformat(),
            "sentToClose": False,
            "status": "new",
        })
        print(f"✓ Lead saved: {item.get('name')} {phone}")
    except Exception as e:
        print(f"Save lead error: {e}")

def parse_results(text: str) -> list:
    # Clean escaped quotes first
    cleaned = text.replace('\\"', '"').replace('\\n', ' ')
    try:
        match = re.search(r"\[[\s\S]*\]", cleaned)
        if match: return json.loads(match.group(0))
    except Exception: pass
    return []

# ============================================================
# Cleanup stuck tasks
# ============================================================
def cleanup_stuck():
    try:
        stuck = db.collection("assix_tasks").where("status", "==", "running").limit(10).get()
        cutoff = datetime.now() - timedelta(minutes=35)
        for doc in stuck:
            data = doc.to_dict()
            claimed = data.get("claimedAt", "")
            if claimed:
                try:
                    if datetime.fromisoformat(claimed) < cutoff:
                        print(f"Cleaning stuck task: {doc.id}")
                        doc.reference.update({"status": "error"})
                except Exception: pass
    except Exception as e:
        print(f"Cleanup error: {e}")

# ============================================================
# Build goal from task config
# ============================================================
def build_goal(task_type: str, config: dict) -> str:
    # Dynamic tasks from Console — use goal as-is
    if task_type in ("dynamic", "universal"):
        return config.get("goal", "")

    niche = config.get("niche", "")
    city = config.get("city", "")
    max_leads = config.get("maxLeads", 10)
    targets = config.get("targets", [])
    message = config.get("message", "")

    if task_type == "google_maps_scrape":
        return f"""Go to https://www.google.com/maps/search/{quote(niche + ' in ' + city)}
For each business in the left panel:
1. Click it to open details
2. Extract: name, phone, website, address
3. Go back and click the next one
4. Repeat until you have {max_leads} businesses with phone numbers
CRITICAL: Do not stop until you have {max_leads} results.
Output as JSON array: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""

    elif task_type == "pages_jaunes_scrape":
        return f"""Go to https://www.pagesjaunes.ca/search/si/{quote(niche)}/{quote(city)}
Extract name, phone, website, address from each listing.
Get {max_leads} results across pages.
Output as JSON array: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""

    elif task_type == "instagram_dm":
        return f"""Go to https://www.instagram.com
Send this message to: {', '.join(targets)}
Message: "{message}"
For each: open profile → Message → type → send."""

    elif task_type == "whatsapp_outreach":
        return f"""Go to https://web.whatsapp.com
For each number, go to: https://web.whatsapp.com/send?phone=NUMBER
Type this message and press Enter: "{message}"
Numbers: {', '.join(targets)}"""

    else:
        goal = config.get("goal", task_type)
        url = config.get("url", "")
        return f"Go to {url}\n{goal}" if url else goal

# ============================================================
# Main
# ============================================================
async def main():
    print("Assix agent starting...")
    cleanup_stuck()

    # Get oldest queued task
    tasks = db.collection("assix_tasks").where("status", "==", "queued").limit(10).get()
    if not tasks:
        print("No queued tasks. Exiting.")
        return

    task_doc = sorted(tasks, key=lambda d: d.to_dict().get("createdAt", ""))[0]
    task = task_doc.to_dict()
    task_id = task["taskId"]
    task_type = task["taskType"]
    config = task.get("config", {})

    print(f"Task: {task_id} — {task_type}")

    # Claim it
    db.collection("assix_tasks").document(task_id).update({
        "status": "running",
        "claimedAt": datetime.now().isoformat(),
        "runner": "steel-browser-use",
    })

    log(task_id, f"Starting {task_type}...")
    goal = build_goal(task_type, config)
    log(task_id, f"Goal: {goal[:100]}...")

    # Setup LLM
    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        api_key=os.environ["GROQ_API_KEY"],
        temperature=0,
    )

    # Start Steel cloud browser
    steel_client = Steel(steel_api_key=os.environ.get("STEEL_API_KEY", ""))
    session = steel_client.sessions.create()
    
    live_url = session.session_viewer_url or ""
    print(f"Steel session: {session.id}")
    print(f"Live view: {live_url}")

    # Save live URL to Firebase so dashboard shows it
    db.collection("assix_tasks").document(task_id).update({
        "liveUrl": live_url,
        "steelSessionId": session.id,
    })

    if live_url:
        log(task_id, f"🔴 Live: {live_url}", "success")

    # Connect browser-use to Steel via CDP
    steel_cdp_url = f"wss://connect.steel.dev?apiKey={os.environ.get('STEEL_API_KEY', '')}&sessionId={session.id}"
    
    from browser_use.browser.browser import Browser, BrowserConfig
    browser = Browser(config=BrowserConfig(cdp_url=steel_cdp_url))

    agent = Agent(
        task=goal,
        llm=llm,
        browser=browser,
        use_vision=False,
    )


    try:
        log(task_id, "Agent running...")
        log(task_id, "💬 You can send commands via Console while this runs", "info")
        log(task_id, f"🌐 Starting on: {goal[:60]}...", "info")

        # Command polling — check Firebase for user commands during run
        pending_commands = []
        
        async def poll_commands():
            """Poll Firebase for user commands every 5 seconds"""
            while True:
                try:
                    doc = db.collection("assix_tasks").document(task_id).get()
                    data = doc.to_dict() or {}
                    cmd = data.get("pendingCommand", "")
                    if cmd:
                        pending_commands.append(cmd)
                        # Clear the command
                        db.collection("assix_tasks").document(task_id).update({"pendingCommand": ""})
                        log(task_id, f"📨 Command received: {cmd}", "info")
                    
                    # Check for login/CAPTCHA signals
                    needs_login = data.get("needsLogin", False)
                    if needs_login:
                        log(task_id, "⚠ Login/CAPTCHA detected", "warning")
                except Exception:
                    pass
                await asyncio.sleep(5)

        # Run command polling in background
        poll_task = asyncio.create_task(poll_commands())

        # Inject pending commands into agent context
        original_task = goal
        
        async def run_with_commands():
            """Run agent, injecting commands as they arrive"""
            current_goal = original_task
            step = 0
            max_steps = 60
            
            while step < max_steps:
                # Check for new commands
                if pending_commands:
                    cmd = pending_commands.pop(0)
                    # Append command to current context
                    current_goal = original_task + f"

USER COMMAND: {cmd}"
                    log(task_id, f"🔄 Injecting command into agent: {cmd}", "info")
                
                # Update progress
                db.collection("assix_tasks").document(task_id).update({
                    "progress": step,
                    "progressPct": min(int((step / max_steps) * 100), 99),
                    "status": "running",
                })
                
                step += 1
                await asyncio.sleep(1)
                
                # Check if done via Firebase signal
                doc = db.collection("assix_tasks").document(task_id).get()
                if doc.to_dict().get("stopRequested"):
                    log(task_id, "Stop requested by user", "warning")
                    break

        # Run the actual agent
        history = await agent.run(max_steps=60)
        
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass

        success = history.is_successful()
        final_result = history.final_result() or ""

        log(task_id, f"Done. Success: {success}", "success" if success else "warning")
        if final_result:
            log(task_id, f"Result preview: {final_result[:200]}")

        is_scraping = task_type in ["google_maps_scrape", "pages_jaunes_scrape"]
        results = parse_results(final_result) if is_scraping else []

        if results:
            log(task_id, f"Saving {len(results)} leads...")
            for item in results:
                save_lead(task_id, item, task_type)
            log(task_id, f"✓ {len(results)} leads saved", "success")

        db.collection("assix_tasks").document(task_id).update({
            "status": "complete",
            "results": results,
            "finalResult": final_result[:5000],
            "completedAt": datetime.now().isoformat(),
            "progress": len(results) if results else 0,
            "progressPct": 100,
        })

        log(task_id, f"✓ Complete — {len(results)} items found", "success")

    except Exception as e:
        log(task_id, f"Error: {str(e)}", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})
        raise

    finally:
        try:
            steel_client.sessions.release(session.id)
            print(f"Steel session released: {session.id}")
        except Exception: pass


async def loop():
    print("Assix worker starting — polling for tasks...")
    while True:
        try:
            await main()
        except Exception as e:
            print(f"Error in main: {e}")
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(loop())
