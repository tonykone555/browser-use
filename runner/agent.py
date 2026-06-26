import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore

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

    # Fallback: parse Google Maps text format
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
        if items:
            return items
    except Exception as e:
        print(f"Fallback parse error: {e}")
    return []

def cleanup_stuck():
    try:
        stuck = db.collection("assix_tasks").where("status", "==", "running").limit(10).get()
        cutoff = datetime.now() - timedelta(minutes=10)
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

def build_goal(task_type: str, config: dict) -> tuple:
    """Returns (url, goal) tuple"""
    city = config.get("city", "")
    niche = config.get("niche", "")
    max_leads = config.get("maxLeads", 10)
    message = config.get("message", "")
    max_messages = config.get("maxMessages", 10)
    email = config.get("email", "") or os.environ.get("AIRBNB_EMAIL", "")
    password = config.get("password", "") or os.environ.get("AIRBNB_PASSWORD", "")

    if task_type == "google_maps_scrape":
        url = f"https://www.google.com/maps/search/{quote(niche + ' in ' + city)}"
        goal = f"""Wait for the results to load.
Extract all visible businesses from the left panel in ONE action.
Get name, phone, website, address for {max_leads} businesses.
Output ONLY this JSON format:
[{{"name":"...","phone":"...","website":"...","address":"..."}}]"""
        return url, goal

    elif task_type == "pages_jaunes_scrape":
        url = f"https://www.pagesjaunes.ca/search/si/{quote(niche)}/{quote(city)}"
        goal = f"""Extract name, phone, website, address from each listing.
Get {max_leads} results.
Output as JSON: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""
        return url, goal

    elif task_type == "airbnb_outreach":
        url = f"https://www.airbnb.com/s/{quote(city)}/homes"
        login_part = ""
        if email and password:
            login_part = f"""
If you see a login page:
1. Enter email: {email}
2. Click Next
3. If asked for code, click "Try another way" then "Enter your password"
4. Enter password: {password}
5. Click Log in
6. After login navigate to: https://www.airbnb.com/s/{quote(city)}/homes
"""
        goal = f"""{login_part}
For each listing:
1. Click the listing photo or title
2. Scroll down to "Meet your Host" section
3. Click "Contact Host" or "Message" button
4. If dates required: check-in 2 weeks from today, checkout 3 weeks from today
5. Type message: "{message}"
6. Click Send
7. Go back and repeat for next listing
8. Dismiss any popups (X, No thanks, Dismiss)

Send to {max_messages} hosts total. Never book or pay anything."""
        return url, goal

    elif task_type in ("dynamic", "universal"):
        goal = config.get("goal", "")
        url = config.get("url", "https://www.google.com")
        return url, goal

    else:
        goal = config.get("goal", task_type)
        url = config.get("url", "https://www.google.com")
        return url, goal


async def main():
    print("Assix agent starting...")
    cleanup_stuck()

    tasks = db.collection("assix_tasks").where("status", "==", "queued").limit(10).get()
    if not tasks:
        print("No queued tasks.")
        all_tasks = db.collection("assix_tasks").limit(5).get()
        for t in all_tasks:
            d = t.to_dict()
            print(f"  Task: {d.get('taskId','')} status={d.get('status','')} type={d.get('taskType','')}")
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
        "runner": "skyvern",
    })

    log(task_id, f"Starting {task_type}...")
    url, goal = build_goal(task_type, config)
    log(task_id, f"URL: {url}")
    log(task_id, f"Goal: {goal[:80]}...")

    try:
        from skyvern import Skyvern
        skyvern_client = Skyvern(api_key=os.environ.get("SKYVERN_API_KEY", ""))

        log(task_id, "Starting Skyvern browser task...")

        # Run task via Skyvern
        run = await skyvern_client.agent.run_task(
            url=url,
            goal=goal,
            title=f"Assix: {task_type}",
        )

        task_run_id = run.task_id
        live_url = getattr(run, 'live_url', '') or f"https://app.skyvern.com/tasks/{task_run_id}"

        print(f"Skyvern task ID: {task_run_id}")
        print(f"Live URL: {live_url}")

        db.collection("assix_tasks").document(task_id).update({
            "skyvernTaskId": task_run_id,
            "liveUrl": live_url,
        })

        log(task_id, f"Live: {live_url}", "success")
        log(task_id, "Agent running — check live view for progress", "info")

        # Poll for completion
        import time
        max_wait = 25 * 60
        elapsed = 0

        while elapsed < max_wait:
            time.sleep(10)
            elapsed += 10

            try:
                status_run = await skyvern_client.agent.get_task(task_id=task_run_id)
                status = getattr(status_run, 'status', '') or ''

                if elapsed % 60 == 0:
                    log(task_id, f"Status: {status} ({elapsed}s)")

                db.collection("assix_tasks").document(task_id).update({
                    "progress": elapsed // 10,
                    "progressPct": min(int((elapsed / max_wait) * 100), 99),
                })

                # Take screenshot and save to Firebase
                try:
                    import requests as req
                    screenshot_res = req.get(
                        f"https://api.skyvern.com/api/v1/tasks/{task_run_id}/screenshot",
                        headers={"x-api-key": os.environ.get("SKYVERN_API_KEY", "")},
                        timeout=5
                    )
                    if screenshot_res.status_code == 200:
                        import base64
                        img_b64 = base64.b64encode(screenshot_res.content).decode("utf-8")
                        db.collection("assix_tasks").document(task_id).update({
                            "latestScreenshot": img_b64,
                            "screenshotAt": int(datetime.now().timestamp() * 1000),
                        })
                except Exception as se:
                    pass

                # Detect if agent needs interaction
                if status in ("requires_action", "waiting_for_human", "paused"):
                    log(task_id, "⚠ Agent needs your input — tap TAKE CONTROL", "warning")
                    db.collection("assix_tasks").document(task_id).update({
                        "needsInteraction": True,
                        "interactionUrl": live_url,
                    })
                elif status == "running":
                    db.collection("assix_tasks").document(task_id).update({
                        "needsInteraction": False,
                    })

                if status in ("completed", "failed", "terminated", "canceled"):
                    output = getattr(status_run, 'output', '') or ''
                    extracted = getattr(status_run, 'extracted_information', '') or ''
                    final_result = str(output or extracted or '')

                    log(task_id, f"Finished: {status}", "success" if status == "completed" else "error")
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
                        "status": "complete" if status == "completed" else "error",
                        "results": results,
                        "finalResult": final_result[:5000],
                        "completedAt": datetime.now().isoformat(),
                        "progress": len(results) if results else elapsed // 10,
                        "progressPct": 100,
                    })

                    log(task_id, f"✓ Complete — {len(results)} items", "success")
                    return

            except Exception as e:
                print(f"Poll error: {e}")

        # Timeout
        log(task_id, "Task timed out", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})

    except Exception as e:
        log(task_id, f"Error: {str(e)}", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})
        raise


if __name__ == "__main__":
    asyncio.run(main())
