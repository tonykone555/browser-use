import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from browser_use import Agent, Browser, BrowserConfig
from langchain_groq import ChatGroq

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
# Helpers
# ============================================================
async def log(task_id: str, msg: str, log_type: str = "info"):
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


async def save_lead(task_id: str, item: dict, platform: str):
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


def cleanup_old_queued():
    try:
        all_queued = db.collection("assix_tasks").where("status", "==", "queued").limit(20).get()
        if len(all_queued) > 1:
            sorted_queued = sorted(all_queued, key=lambda d: d.to_dict().get("createdAt", ""), reverse=True)
            for old_task in sorted_queued[1:]:
                old_task.reference.delete()
                print(f"Deleted old queued task: {old_task.id}")
    except Exception as e:
        print(f"Cleanup queued error: {e}")


# ============================================================
# Session management — save/load cookies from Firebase
# ============================================================
def detect_platform(goal: str) -> str:
    goal_lower = goal.lower()
    if "leboncoin" in goal_lower: return "leboncoin"
    if "airbnb" in goal_lower: return "airbnb"
    if "instagram" in goal_lower: return "instagram"
    if "whatsapp" in goal_lower: return "whatsapp"
    if "linkedin" in goal_lower: return "linkedin"
    if "facebook" in goal_lower: return "facebook"
    if "twitter" in goal_lower or "x.com" in goal_lower: return "twitter"
    return "default"


async def load_session(platform: str, context) -> bool:
    if platform == "default": return False
    try:
        doc = db.collection("assix_sessions").document(platform).get()
        if doc.exists:
            cookies = doc.to_dict().get("cookies", [])
            if cookies:
                await context.add_cookies(cookies)
                print(f"Session loaded for {platform}")
                return True
    except Exception as e:
        print(f"Load session error: {e}")
    return False


async def save_session(platform: str, context):
    if platform == "default": return
    try:
        cookies = await context.cookies()
        if cookies:
            db.collection("assix_sessions").document(platform).set({
                "cookies": cookies,
                "savedAt": datetime.now().isoformat()
            })
            print(f"Session saved for {platform}")
    except Exception as e:
        print(f"Save session error: {e}")


# ============================================================
# Build goal — universal, no pre-set messages
# ============================================================
def build_goal(task_type: str, config: dict) -> str:
    # For dynamic/console tasks — use the goal exactly as typed
    if task_type == "dynamic" or task_type == "universal":
        return config.get("goal", "")

    # For structured tasks
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

    elif task_type == "market_research":
        return f"""Go to https://www.google.com/search?q={quote(config.get('topic', '') + ' problems reviews')}
Research "{config.get('topic', '')}". Goal: {config.get('goal', '')}
Search Google and Reddit. Extract pain points, complaints, market size, competitors.
Compile a structured report."""

    elif task_type == "universal_scrape":
        return config.get("extract", "")

    else:
        goal = config.get("goal", task_type)
        url = config.get("url", "")
        return f"Go to {url}\n{goal}" if url else goal


# ============================================================
# Main
# ============================================================
async def main():
    print("Assix browser-use runner starting...")
    cleanup_stuck_tasks()
    cleanup_old_queued()

    all_tasks = db.collection("assix_tasks").where("status", "==", "queued").limit(1).get()
    if not all_tasks:
        print("No pending tasks. Exiting.")
        return

    task = all_tasks[0].to_dict()
    task_id = task["taskId"]
    task_type = task["taskType"]
    config = task.get("config", {})

    print(f"Found task: {task_id} — {task_type}")

    db.collection("assix_tasks").document(task_id).update({
        "status": "running",
        "claimedAt": datetime.now().isoformat(),
        "runner": "github-actions-browser-use",
    })

    await log(task_id, f"Starting: {task_type}")
    goal = build_goal(task_type, config)
    await log(task_id, f"Goal: {goal[:80]}...")

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        api_key=os.environ["GROQ_API_KEY"],
        temperature=0,
    )

    # Detect platform for session management
    platform = detect_platform(goal)
    await log(task_id, f"Platform detected: {platform}")

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser_instance = await p.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1280,800",
            ]
        )
        context = await browser_instance.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        # Load saved session if available
        session_loaded = await load_session(platform, context)
        if session_loaded:
            await log(task_id, f"✓ Session loaded for {platform} — already logged in", "success")
        else:
            await log(task_id, f"No saved session for {platform} — starting fresh")

        page = await context.new_page()

        browser = Browser(
            config=BrowserConfig(
                headless=False,
                disable_security=True,
            )
        )

        agent = Agent(
            task=goal,
            llm=llm,
            browser=browser,
            use_vision=False,
        )

        try:
            await log(task_id, "Browser-use agent running...")
            history = await agent.run(max_steps=60)

            # Save session after successful run
            await save_session(platform, context)

            success = history.is_successful()
            final_result = history.final_result() or ""

            await log(task_id, f"Agent finished. Success: {success}", "success" if success else "warning")
            await log(task_id, f"Result: {final_result[:300]}")

            is_scraping = task_type in ["google_maps_scrape", "pages_jaunes_scrape"]
            results = parse_results(final_result) if is_scraping else []

            if is_scraping and results:
                await log(task_id, f"Saving {len(results)} leads...")
                for item in results:
                    await save_lead(task_id, item, task_type)
                await log(task_id, f"✓ {len(results)} leads saved", "success")

            db.collection("assix_tasks").document(task_id).update({
                "status": "complete",
                "results": results,
                "finalResult": final_result[:5000],
                "completedAt": datetime.now().isoformat(),
                "progress": len(results) if results else 0,
                "progressPct": 100,
            })

            await log(task_id, f"✓ Complete. {len(results)} items found.", "success")

        except Exception as e:
            await log(task_id, f"Error: {str(e)}", "error")
            db.collection("assix_tasks").document(task_id).update({"status": "error"})
            raise
        finally:
            await context.close()
            await browser_instance.close()


if __name__ == "__main__":
    asyncio.run(main())
