import asyncio
import os
import json
import re
import base64
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from browser_use import Agent, Browser
from browser_use.browser.browser import BrowserConfig
from langchain_cerebras import ChatCerebras
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
    if not text: return []
    try:
        cleaned = text.replace('\\"', '"').replace('\\n', ' ')
        match = re.search(r'\[[\s\S]*?\]', cleaned)
        if match:
            result = json.loads(match.group(0))
            if isinstance(result, list) and len(result) > 0:
                return result
    except Exception: pass
    try:
        items = []
        business_blocks = re.split(r'(?=Share[A-Z])', text)
        for block in business_blocks:
            if not block.strip(): continue
            phone_match = re.search(r'(\+?1?[\s\-\.]?\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})', block)
            if not phone_match: continue
            phone = phone_match.group(1).strip()
            name_match = re.search(r'Share([^\d\n]+?)(?:\s+[\d\.]+)', block)
            name = name_match.group(1).strip() if name_match else ''
            if not name: continue
            website_match = re.search(r'\[Website\]\(([^\)]+)\)', block)
            website = website_match.group(1) if website_match else ''
            addr_match = re.search(r'·\s+([^·\n]+?)\s*(?:Open|Closed)', block)
            address = addr_match.group(1).strip() if addr_match else ''
            items.append({"name": name, "phone": phone, "website": website, "address": address})
        if items: return items
    except Exception as e:
        print(f"Parse error: {e}")
    return []

def cleanup_stuck():
    try:
        stuck = db.collection("assix_tasks").where("status", "==", "running").limit(10).get()
        cutoff = datetime.now() - timedelta(minutes=35)
        for doc in stuck:
            data = doc.to_dict()
            claimed = data.get("claimedAt", "")
            if not claimed:
                doc.reference.update({"status": "queued"})
                continue
            try:
                if datetime.fromisoformat(claimed) < cutoff:
                    doc.reference.update({"status": "error"})
            except Exception: pass
    except Exception as e:
        print(f"Cleanup error: {e}")

def detect_platform(goal: str) -> str:
    g = goal.lower()
    if "airbnb" in g: return "airbnb"
    if "leboncoin" in g: return "leboncoin"
    if "instagram" in g: return "instagram"
    if "whatsapp" in g: return "whatsapp"
    if "linkedin" in g: return "linkedin"
    if "facebook" in g: return "facebook"
    if "reddit" in g: return "reddit"
    return "default"

def load_session_cookies(platform: str) -> list:
    if platform == "default": return []
    try:
        doc = db.collection("assix_sessions").document(platform).get()
        if doc.exists:
            cookies = doc.to_dict().get("cookies", [])
            if cookies:
                print(f"✓ Session loaded for {platform}")
                return cookies
    except Exception as e:
        print(f"Load session error: {e}")
    return []

def build_goal(task_type: str, config: dict) -> str:
    city = config.get("city", "")
    niche = config.get("niche", "")
    max_leads = config.get("maxLeads", 10)
    message = config.get("message", "")
    max_messages = config.get("maxMessages", 10)
    email = config.get("email", "") or os.environ.get("AIRBNB_EMAIL", "")
    password = config.get("password", "") or os.environ.get("AIRBNB_PASSWORD", "")

    if task_type == "google_maps_scrape":
        return f"""Go to https://www.google.com/maps/search/{quote(niche + ' in ' + city)}

Wait 3 seconds for results to load completely.

You will see a LEFT PANEL with a list of businesses.

For each business in the left panel list:
1. Click on the business name to open its detail panel
2. Wait for details to load
3. Extract: business name, phone number, website URL, full address
4. Note down all details
5. Click the back arrow to return to the results list
6. Click the next business
7. Repeat until you have {max_leads} businesses

After collecting all {max_leads} businesses output ONLY this JSON:
[{{"name":"Business Name","phone":"+1XXXXXXXXXX","website":"https://...","address":"Full address"}}]

Important:
- Click each business individually to get phone numbers
- Phone numbers are only visible in the detail panel
- Do not stop early
- Output JSON only at the very end"""

    elif task_type == "pages_jaunes_scrape":
        return f"""Go to https://www.pagesjaunes.ca/search/si/{quote(niche)}/{quote(city)}

Wait for results to load.

For each business listing on the page:
1. Extract business name
2. Extract phone number
3. Extract website if available
4. Extract address

Collect {max_leads} businesses then output ONLY this JSON:
[{{"name":"...","phone":"...","website":"...","address":"..."}}]"""

    elif task_type == "airbnb_outreach":
        login_part = ""
        if email and password:
            login_part = f"""
If you see a login page:
1. Enter email: {email}
2. Click Next
3. If code screen: click "Try another way" then "Enter your password"
4. Enter password: {password}
5. Click Log in
6. Dismiss any popups after login
"""
        return f"""{login_part}
Go to https://www.airbnb.com/s/{quote(city)}/homes

For each listing:
1. Click the listing photo or title
2. Scroll down to "Meet your Host" section
3. Click "Contact Host" or "Message"
4. If dates required: check-in 2 weeks from today, checkout 3 weeks from today
5. Type exactly: "{message}"
6. Click Send
7. Go back and repeat

Dismiss all popups. Never book or pay. Send to {max_messages} hosts."""

    elif task_type in ("dynamic", "universal"):
        goal = config.get("goal", "")
        url = config.get("url", "")
        return f"Go to {url}\n{goal}" if url else goal

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

    # Mark as claimed immediately
    db.collection("assix_tasks").document(task_id).update({
        "status": "running",
        "claimedAt": datetime.now().isoformat(),
        "runner": "steel-cerebras",
    })

    log(task_id, f"Starting {task_type}...")
    goal = build_goal(task_type, config)
    log(task_id, f"Goal: {goal[:80]}...")

    llm = ChatCerebras(
        model="gpt-oss-120b",
        api_key=os.environ.get("CEREBRAS_API_KEY", ""),
        temperature=0,
    )

    # Start Steel session — 15 min max on hobby plan
    steel_client = Steel(steel_api_key=os.environ.get("STEEL_API_KEY", ""))
    session = steel_client.sessions.create(timeout=900000)

    live_url = f"https://app.steel.dev/sessions/{session.id}"
    print(f"Steel session: {session.id}")
    print(f"Live URL: {live_url}")

    # Save liveUrl IMMEDIATELY so frontend shows it right away
    db.collection("assix_tasks").document(task_id).update({
        "liveUrl": live_url,
        "steelSessionId": session.id,
        "startedAt": datetime.now().isoformat(),
    })

    log(task_id, f"Steel session ready", "success")
    log(task_id, f"Live: {live_url}", "success")
    log(task_id, "Opening browser...", "info")

    platform = detect_platform(goal)
    cookies = load_session_cookies(platform)

    # Screenshot loop — every 5 seconds
    stop_screenshots = threading.Event()

    def screenshot_loop():
        import requests as req
        # Wait 5 seconds before first screenshot
        time.sleep(5)
        while not stop_screenshots.is_set():
            try:
                r = req.get(
                    f"https://api.steel.dev/v1/sessions/{session.id}/screenshot",
                    headers={"Steel-Api-Key": os.environ.get("STEEL_API_KEY", "")},
                    timeout=5
                )
                if r.status_code == 200 and len(r.content) > 1000:
                    img_b64 = base64.b64encode(r.content).decode("utf-8")
                    db.collection("assix_tasks").document(task_id).update({
                        "latestScreenshot": img_b64,
                        "screenshotAt": int(datetime.now().timestamp() * 1000),
                    })
                    print(f"Screenshot saved: {len(r.content)} bytes")
            except Exception as e:
                print(f"Screenshot error: {e}")
            time.sleep(5)

    screenshot_thread = threading.Thread(target=screenshot_loop, daemon=True)
    screenshot_thread.start()

    cdp_url = f"wss://connect.steel.dev?apiKey={os.environ.get('STEEL_API_KEY', '')}&sessionId={session.id}"
    browser = Browser(config=BrowserConfig(cdp_url=cdp_url))

    if cookies:
        try:
            page = await browser.get_current_page()
            await page.context.add_cookies(cookies)
            log(task_id, f"✓ Session cookies loaded for {platform}", "success")
        except Exception as e:
            print(f"Cookie injection error: {e}")

    agent = Agent(
        task=goal,
        llm=llm,
        browser=browser,
        use_vision=False,
        max_input_tokens=40000,
        max_failures=5,
    )

    try:
        log(task_id, "Agent running...")
        history = await agent.run(max_steps=60)

        stop_screenshots.set()

        if platform != "default":
            try:
                page = await browser.get_current_page()
                saved_cookies = await page.context.cookies()
                if saved_cookies:
                    db.collection("assix_sessions").document(platform).set({
                        "cookies": saved_cookies,
                        "savedAt": datetime.now().isoformat(),
                    })
                    log(task_id, f"✓ Session saved for {platform}", "success")
            except Exception as e:
                print(f"Save session error: {e}")

        success = history.is_successful()
        final_result = history.final_result() or ""

        if not final_result or len(final_result) < 50:
            try:
                all_results = history.action_results()
                for r in reversed(all_results):
                    extracted = str(r.extracted_content or "")
                    if len(extracted) > 100:
                        final_result = extracted
                        break
            except Exception: pass

        log(task_id, f"Done. Success: {success}", "success" if success else "warning")
        if final_result:
            log(task_id, f"Result: {final_result[:200]}")

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


if __name__ == "__main__":
    asyncio.run(main())
