import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from browser_use import Agent, Browser, BrowserConfig
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
    cleaned = text.replace('\\"', '"').replace('\\n', ' ')
    try:
        match = re.search(r"\[[\s\S]*\]", cleaned)
        if match: return json.loads(match.group(0))
    except Exception: pass
    return []

def cleanup_stuck():
    try:
        stuck = db.collection("assix_tasks").where("status", "==", "running").limit(10).get()
        cutoff = datetime.now() - timedelta(minutes=10)
        for doc in stuck:
            data = doc.to_dict()
            claimed = data.get("claimedAt", "")
            if not claimed:
                print(f"Resetting unclaimed running task: {doc.id}")
                doc.reference.update({"status": "queued"})
                continue
            try:
                if datetime.fromisoformat(claimed) < cutoff:
                    print(f"Cleaning stuck task: {doc.id}")
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
Wait for the results to load.
Use extract_content to get all business data from the left panel in ONE action.
Then immediately call done with a JSON array of the results.
Do NOT click anything. Do NOT visit any websites.
Extract name, phone, website, address for {max_leads} businesses directly from the list.
Output ONLY this JSON format and nothing else:
[{{"name":"...","phone":"...","website":"...","address":"..."}}]"""

    elif task_type == "pages_jaunes_scrape":
        return f"""Go to https://www.pagesjaunes.ca/search/si/{quote(niche)}/{quote(city)}
Extract name, phone, website, address from each listing.
Get {max_leads} results. Output as JSON array: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""

    elif task_type == "airbnb_outreach":
        message = config.get("message", "")
        max_messages = config.get("maxMessages", 10)
        email = config.get("email", "") or os.environ.get("AIRBNB_EMAIL", "")
        password = config.get("password", "") or os.environ.get("AIRBNB_PASSWORD", "")
        
        login_instructions = ""
        if email and password:
            login_instructions = f"""
LOGIN INSTRUCTIONS (do this first if not already logged in):
1. Go to https://www.airbnb.com/login
2. Type email: {email}
3. Click Next or Continue
4. If you see a code verification screen, click "Try another way"
5. Click "Enter your password" option
6. Type password: {password}
7. Click Log in or Submit
8. If a verification code is sent to email - click "Try another way" then "Enter your password"
9. If any popup appears (promotions, notifications, cookies) - click X, Dismiss or No thanks
9. After successful login, immediately navigate to https://www.airbnb.com/s/{quote(city)}/homes
10. Wait for listings to load then start messaging hosts
"""
        else:
            login_instructions = "If you see a login page, wait 120 seconds for the user to login manually then continue."

        return f"""Go to https://www.airbnb.com/s/{quote(city)}/homes

{login_instructions}

MESSAGING STEPS (repeat for {max_messages} hosts):
1. Click on a listing card (click the photo or title)
2. On the listing page, scroll down to find "Meet your Host" section
3. Click the "Contact Host" or "Message" button
4. If asked for dates, enter check-in 2 weeks from today, checkout 3 weeks from today
5. In the message box type exactly: "{message}"
6. Click Send
7. Press back button to return to search results
8. Click next listing and repeat

POPUP HANDLING:
- If any popup appears at any time (promotions, cookies, notifications, sign up) - close it immediately by clicking X, Dismiss, or No thanks
- Never book or pay anything
- Keep going until {max_messages} messages sent
- NEVER call done early"""

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
        # Debug: show what tasks exist
        all_tasks = db.collection("assix_tasks").limit(5).get()
        for t in all_tasks:
            d = t.to_dict()
            print(f"  Existing task: {d.get('taskId','')} status={d.get('status','')} type={d.get('taskType','')}")
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

    # Use smaller model to save tokens
    llm = ChatCerebras(
        model="gpt-oss-120b",
        api_key=os.environ["CEREBRAS_API_KEY"],
        temperature=0,
    )

    # Start Steel session
    steel_client = Steel(steel_api_key=os.environ.get("STEEL_API_KEY", ""))
    session = steel_client.sessions.create()
    # Use app.steel.dev sessions viewer
    live_url = getattr(session, "session_viewer_url", "") or f"https://app.steel.dev/sessions/{session.id}"

    print(f"Steel session: {session.id}")
    print(f"Live URL: {live_url}")

    db.collection("assix_tasks").document(task_id).update({
        "liveUrl": live_url,
        "steelSessionId": session.id,
    })

    if live_url:
        log(task_id, f"🔴 Live: {live_url}", "success")
        log(task_id, f"👆 Tap WATCH LIVE to see browser", "info")

    # Inject saved cookies into Steel session if available
    if saved_cookies:
        try:
            import requests as req
            for cookie in saved_cookies:
                req.post(
                    f"https://api.steel.dev/v1/sessions/{session.id}/cookies",
                    headers={"Steel-Api-Key": os.environ.get("STEEL_API_KEY", "")},
                    json={"cookies": [cookie]},
                    timeout=5
                )
            log(task_id, "✓ Session cookies injected — already logged in", "success")
        except Exception as e:
            print(f"Cookie injection error: {e}")

    # Connect browser-use to Steel via CDP
    cdp_url = f"wss://connect.steel.dev?apiKey={os.environ.get('STEEL_API_KEY', '')}&sessionId={session.id}"
    browser = Browser(config=BrowserConfig(cdp_url=cdp_url))

    agent = Agent(
        task=goal,
        llm=llm,
        browser=browser,
        use_vision=False,
        max_input_tokens=40000,
    )

    # Load saved session cookies if available
    saved_cookies = []
    if task_type == "airbnb_outreach":
        try:
            session_doc = db.collection("assix_sessions").document("airbnb").get()
            if session_doc.exists:
                saved_cookies = session_doc.to_dict().get("cookies", [])
                if saved_cookies:
                    log(task_id, f"✓ Loaded saved Airbnb session ({len(saved_cookies)} cookies)", "success")
        except Exception as e:
            print(f"Session load error: {e}")

    try:
        log(task_id, "Agent running...")
        history = await agent.run(max_steps=60)

        success = history.is_successful()
        final_result = history.final_result() or ""

        # Also try to get data from action results if final_result is empty
        if not final_result or len(final_result) < 50:
            try:
                all_results = history.action_results()
                for r in reversed(all_results):
                    extracted = str(r.extracted_content or "")
                    if len(extracted) > 100:
                        final_result = extracted
                        break
            except Exception:
                pass

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

        log(task_id, f"✓ Complete — {len(results)} items", "success")

    except Exception as e:
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
