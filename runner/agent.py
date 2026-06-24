import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote

import firebase_admin
from firebase_admin import credentials, firestore
from browser_use.beta import Agent, BrowserProfile, ChatBrowserUse
from langchain_groq import ChatGroq

# ============================================================
# Firebase Init
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
        return f"""Go to https://www.instagram.com/accounts/login/
Log in with username "{config.get('igUsername')}" and password "{config.get('igPassword')}".
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
    print("Assix browser-use cloud runner starting...")
    cleanup_stuck_tasks()

    all_tasks = db.collection("assix_tasks").where("status", "==", "queued").limit(5).get()
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
        "runner": "github-actions-browser-use-cloud",
    })

    await log(task_id, f"Starting: {task_type}")
    goal = build_goal(task_type, config)
    await log(task_id, "Browser-use cloud agent starting...")

    max_leads = config.get("maxLeads", 50)

    # Use browser-use cloud with Groq
    llm = ChatBrowserUse(
        model="groq/llama-3.3-70b-versatile",
        api_key=os.environ.get("BROWSER_USE_API_KEY"),
    )

    agent = Agent(
        task=goal,
        llm=llm,
        browser_profile=BrowserProfile(
            headless=True,
        ),
    )

    step_count = [0]

    try:
        await log(task_id, "Agent running...")

        async for step in agent.arun(max_steps=60):
            step_count[0] += 1
            action = str(step.action)[:100] if hasattr(step, 'action') and step.action else f"Step {step_count[0]}"
            await log(task_id, f"→ {action}")

            # Screenshot
            screenshot = getattr(step, 'screenshot', None) or getattr(step, 'state_screenshot', None)
            if screenshot:
                db.collection("assix_tasks").document(task_id).update({
                    "latestScreenshot": screenshot,
                    "screenshotAt": int(datetime.now().timestamp() * 1000),
                })

            db.collection("assix_tasks").document(task_id).update({
                "progress": step_count[0],
                "total": max_leads,
                "progressPct": min(int((step_count[0] / 60) * 100), 99),
                "status": "running",
            })

        history = agent.history
        success = history.is_successful() if hasattr(history, 'is_successful') else True
        final_result = history.final_result() if hasattr(history, 'final_result') else str(history)
        final_result = final_result or ""

        await log(task_id, f"Agent finished. Success: {success}", "success" if success else "warning")

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
            "finalResult": str(final_result)[:5000],
            "completedAt": datetime.now().isoformat(),
            "progress": len(results) if results else step_count[0],
            "progressPct": 100,
        })

        await log(task_id, f"✓ Complete. {len(results)} items found.", "success")

    except Exception as e:
        await log(task_id, f"Error: {str(e)}", "error")
        db.collection("assix_tasks").document(task_id).update({"status": "error"})
        raise


if __name__ == "__main__":
    asyncio.run(main())
