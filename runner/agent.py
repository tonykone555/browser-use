import os
import json
import re
import time
import requests
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

BROWSER_USE_API_KEY = os.environ.get("BROWSER_USE_API_KEY", "")
BU_BASE = "https://api.browser-use.com/api/v2"
HEADERS = {"X-Browser-Use-API-Key": BROWSER_USE_API_KEY, "Content-Type": "application/json"}

# ============================================================
# Helpers
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
        db.collection("assix_tasks").document(task_id).collection("logs").add(entry)
        task_ref = db.collection("assix_tasks").document(task_id)
        task_data = task_ref.get().to_dict() or {}
        recent_logs = task_data.get("recentLogs", [])
        recent_logs.append(entry)
        if len(recent_logs) > 50:
            recent_logs = recent_logs[-50:]
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
            "market": "english_ca",
            "leadType": "no_website" if not item.get("website") else "has_website",
            "source": platform,
            "createdAt": datetime.now().isoformat(),
            "sentToClose": False,
            "status": "new",
        })
        print(f"Lead saved: {item.get('name')} {phone}")
    except Exception as e:
        print(f"Save lead error: {e}")


def parse_results(text: str) -> list:
    try:
        match = re.search(r"\[[\s\S]*\]", text)
        if match: return json.loads(match.group(0))
    except Exception: pass
    return []


def cleanup_stuck_tasks():
    try:
        stuck = db.collection("assix_tasks").where("status", "==", "running").limit(20).get()
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
# Build goal
# ============================================================
def build_goal(task_type: str, config: dict) -> str:
    if task_type in ("dynamic", "universal"):
        return config.get("goal", "")

    niche = config.get("niche", "")
    city = config.get("city", "")
    max_leads = config.get("maxLeads", 50)
    targets = config.get("targets", [])
    message = config.get("message", "")

    if task_type == "google_maps_scrape":
        return f"""Go to https://www.google.com/maps/search/{quote(niche + ' in ' + city)}
For each business listing in the left panel:
1. Click the listing to open its details
2. Extract: business name, phone number, website URL, full address
3. Go back to results list and click the next listing
4. Repeat until you have {max_leads} businesses with phone numbers
CRITICAL: Never call done until you have {max_leads} results.
Output ALL results as JSON array:
[{{"name": "...", "phone": "...", "website": "...", "address": "..."}}]"""

    elif task_type == "pages_jaunes_scrape":
        return f"""Go to https://www.pagesjaunes.ca/search/si/{quote(niche)}/{quote(city)}
Extract from each listing: name, phone, website, address.
Paginate until you have {max_leads} results.
Output as JSON array: [{{"name": "...", "phone": "...", "website": "...", "address": "..."}}]"""

    elif task_type == "instagram_dm":
        return f"""Go to https://www.instagram.com
Send this message to each user: {', '.join(targets)}
Message: "{message}"
For each user: visit their profile, click Message, type and send."""

    elif task_type == "whatsapp_outreach":
        return f"""Go to https://web.whatsapp.com and wait for it to load.
Send this message to each number: {', '.join(targets)}
Message: "{message}"
For each number go to: https://web.whatsapp.com/send?phone=NUMBER then type and send."""

    else:
        goal = config.get("goal", task_type)
        url = config.get("url", "")
        return f"Go to {url}\n{goal}" if url else goal


# ============================================================
# Browser-use Cloud API v2
# ============================================================
def start_bu_task(goal: str) -> dict:
    res = requests.post(f"{BU_BASE}/tasks", headers=HEADERS, json={"task": goal})
    print(f"Start task response: {res.status_code} {res.text[:200]}")
    res.raise_for_status()
    return res.json()


def get_bu_task(bu_task_id: str) -> dict:
    res = requests.get(f"{BU_BASE}/tasks/{bu_task_id}", headers=HEADERS)
    res.raise_for_status()
    return res.json()


def stop_bu_task(bu_task_id: str):
    try:
        requests.post(f"{BU_BASE}/tasks/{bu_task_id}/stop", headers=HEADERS)
    except Exception: pass


# ============================================================
# Main
# ============================================================
def main():
    print("Assix browser-use CLOUD runner starting...")
    cleanup_stuck_tasks()

    all_tasks = db.collection("assix_tasks").where("status", "==", "queued").limit(10).get()
    if not all_tasks:
        print("No pending tasks. Exiting.")
        return

    task_doc = sorted(all_tasks, key=lambda d: d.to_dict().get("createdAt", ""))[0]
    task = task_doc.to_dict()
    task_id = task["taskId"]
    task_type = task["taskType"]
    config = task.get("config", {})

    print(f"Found task: {task_id} — {task_type}")

    db.collection("assix_tasks").document(task_id).update({
        "status": "running",
        "claimedAt": datetime.now().isoformat(),
        "runner": "browser-use-cloud",
    })

    log(task_id, f"Starting: {task_type}")
    goal = build_goal(task_type, config)
    log(task_id, f"Goal: {goal[:80]}...")

    log(task_id, "Starting browser-use cloud task...")
    bu_data = start_bu_task(goal)
    bu_task_id = bu_data.get("id") or bu_data.get("task_id")
    live_url = bu_data.get("live_url", "")

    print(f"Browser-use task ID: {bu_task_id}")
    print(f"Live URL: {live_url}")

    db.collection("assix_tasks").document(task_id).update({
        "buTaskId": bu_task_id,
        "liveUrl": live_url,
    })

    if live_url:
        log(task_id, f"🔴 Live view: {live_url}", "success")

    # Poll for completion
    max_wait = 25 * 60
    poll_interval = 5
    elapsed = 0
    step = 0

    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval

        try:
            bu_status = get_bu_task(bu_task_id)
            status = bu_status.get("status", "")
            output = bu_status.get("output", "") or bu_status.get("result", "") or ""

            step += 1
            if step % 6 == 0:
                log(task_id, f"Status: {status} ({elapsed}s elapsed)")

            db.collection("assix_tasks").document(task_id).update({
                "progress": step,
                "progressPct": min(int((elapsed / max_wait) * 100), 99),
                "status": "running",
            })

            if status in ("paused", "waiting_for_human"):
                log(task_id, "⚠ Agent paused — needs your input. Open the live view link.", "warning")
                db.collection("assix_tasks").document(task_id).update({
                    "status": "waiting",
                    "waitingMsg": f"Agent needs your input. Open: {live_url}",
                })
                wait_elapsed = 0
                while wait_elapsed < 600:
                    time.sleep(10)
                    wait_elapsed += 10
                    bu_status = get_bu_task(bu_task_id)
                    if bu_status.get("status") not in ("paused", "waiting_for_human"):
                        db.collection("assix_tasks").document(task_id).update({"status": "running"})
                        break
                continue

            if status in ("finished", "completed", "done", "failed", "stopped", "error"):
                log(task_id, f"Task finished: {status}", "success" if status in ("finished", "completed", "done") else "error")

                is_scraping = task_type in ["google_maps_scrape", "pages_jaunes_scrape"]
                results = parse_results(str(output)) if is_scraping else []

                if is_scraping and results:
                    log(task_id, f"Saving {len(results)} leads...")
                    for item in results:
                        save_lead(task_id, item, task_type)
                    log(task_id, f"✓ {len(results)} leads saved", "success")

                db.collection("assix_tasks").document(task_id).update({
                    "status": "complete" if status in ("finished", "completed", "done") else "error",
                    "results": results,
                    "finalResult": str(output)[:5000],
                    "completedAt": datetime.now().isoformat(),
                    "progress": len(results) if results else step,
                    "progressPct": 100,
                })

                log(task_id, f"✓ Complete. {len(results)} items found.", "success")
                return

        except Exception as e:
            print(f"Poll error: {e}")

    stop_bu_task(bu_task_id)
    log(task_id, "Task timed out after 25 minutes", "error")
    db.collection("assix_tasks").document(task_id).update({"status": "error"})


if __name__ == "__main__":
    main()
