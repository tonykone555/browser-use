import asyncio
import os
import json
import re
import time
import requests
import base64
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from skyvern import Skyvern

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
SKYVERN_API_KEY = os.environ.get("SKYVERN_API_KEY", "")

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
    # Fallback: parse Google Maps text
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

def get_credential_id(platform: str) -> str:
    """Get saved Skyvern credential ID for a platform"""
    try:
        doc = db.collection("assix_credentials").document(platform).get()
        if doc.exists:
            return doc.to_dict().get("skyvernCredentialId", "")
    except Exception: pass
    return ""

def build_goal(task_type: str, config: dict) -> tuple:
    """Returns (url, prompt) tuple"""
    city = config.get("city", "")
    niche = config.get("niche", "")
    max_leads = config.get("maxLeads", 10)
    message = config.get("message", "")
    max_messages = config.get("maxMessages", 10)

    if task_type == "google_maps_scrape":
        url = f"https://www.google.com/maps/search/{quote(niche + ' in ' + city)}"
        prompt = f"""Extract all visible businesses from the left panel.
Get name, phone, website, address for {max_leads} businesses.
Output as JSON array: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""
        return url, prompt

    elif task_type == "pages_jaunes_scrape":
        url = f"https://www.pagesjaunes.ca/search/si/{quote(niche)}/{quote(city)}"
        prompt = f"""Extract name, phone, website, address for {max_leads} businesses.
Output as JSON: [{{"name":"...","phone":"...","website":"...","address":"..."}}]"""
        return url, prompt

    elif task_type == "airbnb_outreach":
        url = f"https://www.airbnb.com/s/{quote(city)}/homes"
        prompt = f"""Send this message to {max_messages} Airbnb hosts: "{message}"

For each listing:
1. Click the listing
2. Scroll to "Meet your Host" section
3. Click "Contact Host" or "Message"
4. If dates needed: check-in 2 weeks from today, checkout 3 weeks from today
5. Type the message exactly and send
6. Go back and repeat

Dismiss any popups. Never book or pay anything."""
        return url, prompt

    elif task_type in ("dynamic", "universal"):
        goal = config.get("goal", "")
        url = config.get("url", "https://www.google.com")
        return url, goal

    else:
        goal = config.get("goal", task_type)
        url = config.get("url", "https://www.google.com")
        return url, goal

def fetch_screenshot_b64(url: str) -> str:
    """Fetch a screenshot URL and return base64"""
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return base64.b64encode(r.content).decode("utf-8")
    except Exception as e:
        print(f"Screenshot fetch error: {e}")
    return ""

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
        "runner": "skyvern",
    })

    log(task_id, f"Starting {task_type}...")
    url, prompt = build_goal(task_type, config)
    log(task_id, f"URL: {url}")

    try:
        client = Skyvern(api_key=SKYVERN_API_KEY)

        # Check for saved credentials
        credential_id = get_credential_id(task_type.replace("_outreach", "").replace("_scrape", ""))

        run_kwargs = {
            "url": url,
            "prompt": prompt,
            "wait_for_completion": False,
        }

        # Credential ID is passed via prompt context — Skyvern uses it automatically
        if credential_id:
            run_kwargs["prompt"] = f"Use credential ID: {credential_id}\n\n" + run_kwargs["prompt"]
            log(task_id, f"✓ Using saved credentials", "success")

        log(task_id, "Starting Skyvern task...")
        run = await client.run_task(**run_kwargs)

        run_id = run.run_id
        live_url = getattr(run, 'app_url', '') or f"https://app.skyvern.com/runs/{run_id}"

        print(f"Skyvern run ID: {run_id}")
        print(f"Live URL: {live_url}")

        db.collection("assix_tasks").document(task_id).update({
            "skyvernRunId": run_id,
            "liveUrl": live_url,
        })

        log(task_id, f"Live: {live_url}", "success")
        log(task_id, "Agent running — tap TAKE CONTROL to interact", "info")

        # Poll for completion with screenshot capture
        max_wait = 25 * 60
        elapsed = 0
        screenshot_interval = 10
        last_screenshot = 0

        while elapsed < max_wait:
            time.sleep(5)
            elapsed += 5

            # Take screenshot every 10 seconds
            if elapsed - last_screenshot >= screenshot_interval:
                screenshot = take_screenshot(run_id)
                if screenshot:
                    db.collection("assix_tasks").document(task_id).update({
                        "latestScreenshot": screenshot,
                        "screenshotAt": int(datetime.now().timestamp() * 1000),
                    })
                last_screenshot = elapsed

            try:
                status_run = await client.get_run(run_id=run_id)
                status = str(getattr(status_run, 'status', '') or '')

                # Grab latest screenshot from run response
                try:
                    screenshot_urls = getattr(status_run, 'screenshot_urls', None) or []
                    if screenshot_urls and elapsed - last_screenshot >= screenshot_interval:
                        latest_url = screenshot_urls[0] if isinstance(screenshot_urls[0], str) else screenshot_urls[0].get('url', '')
                        if latest_url:
                            img_b64 = fetch_screenshot_b64(latest_url)
                            if img_b64:
                                db.collection("assix_tasks").document(task_id).update({
                                    "latestScreenshot": img_b64,
                                    "screenshotAt": int(datetime.now().timestamp() * 1000),
                                })
                                last_screenshot = elapsed
                except Exception as se:
                    print(f"Screenshot error: {se}")

                if elapsed % 60 == 0:
                    log(task_id, f"Status: {status} ({elapsed}s)")

                db.collection("assix_tasks").document(task_id).update({
                    "progress": elapsed // 5,
                    "progressPct": min(int((elapsed / max_wait) * 100), 99),
                    "needsInteraction": status in ("requires_action", "waiting_for_human", "paused"),
                })

                if status in ("completed", "failed", "terminated", "canceled"):
                    output = getattr(status_run, 'output', None)
                    extracted = getattr(status_run, 'extracted_information', None)
                    final_result = str(output or extracted or '')

                    log(task_id, f"Finished: {status}", "success" if status == "completed" else "warning")
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
                        "progress": len(results) if results else elapsed // 5,
                        "progressPct": 100,
                        "needsInteraction": False,
                    })

                    log(task_id, f"✓ Complete — {len(results)} items", "success")
                    return

            except Exception as e:
                print(f"Poll error: {e}")

        log(task_id, "Task timed out", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})

    except Exception as e:
        log(task_id, f"Error: {str(e)}", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})
        raise


if __name__ == "__main__":
    asyncio.run(main())
