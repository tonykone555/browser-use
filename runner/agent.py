import asyncio
import os
import json
import re
import base64
import threading
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from browser_use import Agent, Browser, BrowserConfig
from langchain_groq import ChatGroq
from steel import Steel

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
        print(f"✓ Lead: {item.get('name')} {phone}")
    except Exception as e:
        print(f"Save lead error: {e}")

def parse_results(text: str) -> list:
    cleaned = text.replace('\\"', '"').replace('\\n', ' ')
    try:
        match = re.search(r"\[[\s\S]*\]", cleaned)
        if match: return json.loads(match.group(0))
    except Exception: pass
    return []

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
                        doc.reference.update({"status": "error"})
                except Exception: pass
    except Exception as e:
        print(f"Cleanup error: {e}")

def build_goal(task_type: str, config: dict) -> str:
    if task_type in ("dynamic", "universal"):
        return config.get("goal", "")
    niche = config.get("niche", "")
    city = config.get("city", "")
    max_leads = config.get("maxLeads", 10)

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
Get {max_leads} results. Output as JSON array: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""

    else:
        goal = config.get("goal", task_type)
        url = config.get("url", "")
        return f"Go to {url}\n{goal}" if url else goal

async def main():
    print("Assix agent starting...")
    cleanup_stuck()

    tasks = db.collection("assix_tasks").where("status", "==", "queued").limit(10).get()
    if not tasks:
        print("No queued tasks.")
        return

    task_doc = sorted(tasks, key=lambda d: d.to_dict().get("createdAt", ""))[0]
    task = task_doc.to_dict()
    task_id = task["taskId"]
    task_type = task["taskType"]
    config = task.get("config", {})

    print(f"Task: {task_id} — {task_type}")

    db.collection("assix_tasks").document(task_id).update({
        "status": "running",
        "claimedAt": datetime.now().isoformat(),
        "runner": "steel-browser-use",
    })

    log(task_id, f"Starting {task_type}...")
    goal = build_goal(task_type, config)
    log(task_id, f"Goal: {goal[:80]}...")

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        api_key=os.environ["GROQ_API_KEY"],
        temperature=0,
    )

    # Start Steel session
    steel_client = Steel(steel_api_key=os.environ.get("STEEL_API_KEY", ""))
    session = steel_client.sessions.create()
    live_url = getattr(session, 'session_viewer_url', '') or ''
    
    print(f"Steel session: {session.id}")
    print(f"Live URL: {live_url}")

    db.collection("assix_tasks").document(task_id).update({
        "liveUrl": live_url,
        "steelSessionId": session.id,
    })

    if live_url:
        log(task_id, f"🔴 Live: {live_url}", "success")

    # Screenshot loop in background thread
    stop_screenshots = threading.Event()

    def screenshot_loop():
        import time
        while not stop_screenshots.is_set():
            try:
                img_bytes = steel_client.sessions.screenshot(session.id)
                if img_bytes:
                    img_b64 = base64.b64encode(img_bytes).decode("utf-8")
                    db.collection("assix_tasks").document(task_id).update({
                        "latestScreenshot": img_b64,
                        "screenshotAt": int(datetime.now().timestamp() * 1000),
                    })
            except Exception as e:
                print(f"Screenshot error: {e}")
            time.sleep(3)

    screenshot_thread = threading.Thread(target=screenshot_loop, daemon=True)
    screenshot_thread.start()

    # Connect browser-use to Steel
    cdp_url = f"wss://connect.steel.dev?apiKey={os.environ.get('STEEL_API_KEY', '')}&sessionId={session.id}"
    browser = Browser(config=BrowserConfig(cdp_url=cdp_url))

    agent = Agent(
        task=goal,
        llm=llm,
        browser=browser,
        use_vision=False,
    )

    try:
        log(task_id, "Agent running...")
        history = await agent.run(max_steps=60)

        stop_screenshots.set()

        success = history.is_successful()
        final_result = history.final_result() or ""

        log(task_id, f"Done. Success: {success}", "success" if success else "warning")
        if final_result:
            log(task_id, f"Result: {final_result[:300]}")

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

        log(task_id, f"✓ Complete — {len(results)} items", "success")

    except Exception as e:
        stop_screenshots.set()
        log(task_id, f"Error: {str(e)}", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})
        raise

    finally:
        try:
            steel_client.sessions.release(session.id)
            print(f"Steel session released: {session.id}")
        except Exception: pass


async def loop():
    print("Assix worker — polling for tasks...")
    while True:
        try:
            await main()
        except Exception as e:
            print(f"Error: {e}")
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(loop())
