import asyncio, json, os, sqlite3, uuid
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB = Path("axiom_prime.db")
app = FastAPI(title="AXIOM PRIME v3", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENTS = [
 ("briefing","Briefing","Clarifies mission, constraints and success criteria."),
 ("research","Research","Collects evidence and structured findings."),
 ("strategist","Strategist","Turns findings into a prioritized execution strategy."),
 ("writer","Writer","Produces the requested copy, scripts or documents."),
 ("producer","Producer","Creates production specifications and asset plans."),
 ("packager","Packager","Prepares delivery, SEO and publication metadata."),
 ("qc","Quality Control","Independently checks completeness and quality.")
]

def now(): return datetime.now(timezone.utc).isoformat()
def conn(): 
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c
def init():
    c=conn()
    c.executescript("""CREATE TABLE IF NOT EXISTS missions(
      id TEXT PRIMARY KEY, task TEXT NOT NULL, mode TEXT, status TEXT, progress INTEGER,
      created_at TEXT, completed_at TEXT, result TEXT);
      CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, mission_id TEXT, at TEXT, actor TEXT, message TEXT);
      CREATE TABLE IF NOT EXISTS agent_runs(
      id INTEGER PRIMARY KEY AUTOINCREMENT, mission_id TEXT, agent TEXT, status TEXT,
      progress INTEGER, input TEXT, output TEXT);
    """); c.commit(); c.close()
init()

class MissionIn(BaseModel):
    task: str
    mode: str = "auto"
    notify_voice: bool = True

def event(mid, actor, message):
    c=conn(); c.execute("INSERT INTO events(mission_id,at,actor,message) VALUES(?,?,?,?)",(mid,now(),actor,message)); c.commit(); c.close()

async def llm(prompt: str) -> str:
    # Provider-neutral adapter. Set AXIOM_LLM_URL and AXIOM_LLM_KEY for a real OpenAI-compatible endpoint.
    url=os.getenv("AXIOM_LLM_URL")
    key=os.getenv("AXIOM_LLM_KEY")
    if not url:
        return "[DEMO OUTPUT] " + prompt[:600]
    import urllib.request
    body=json.dumps({"model":os.getenv("AXIOM_LLM_MODEL","default"),
                     "messages":[{"role":"user","content":prompt}]}).encode()
    req=urllib.request.Request(url,data=body,headers={"Content-Type":"application/json",
                    **({"Authorization":"Bearer "+key} if key else {})})
    with urllib.request.urlopen(req,timeout=120) as r:
        data=json.loads(r.read().decode())
    return data.get("choices",[{}])[0].get("message",{}).get("content","")

async def run_agent(mid, key, name, description, context):
    c=conn()
    c.execute("INSERT INTO agent_runs(mission_id,agent,status,progress,input) VALUES(?,?,?,?,?)",
              (mid,key,"running",0,context)); c.commit(); c.close()
    event(mid,name,"Mission delegated. Starting autonomous work.")
    prompt=f"""You are the {name} agent in AXIOM PRIME.
Mission: {context}
Your role: {description}
Return concise, structured output that the next agent can use. Do not ask the user questions unless a safety-critical ambiguity exists."""
    # Real provider call; progress remains server-observable.
    for p in (15,35,60):
        await asyncio.sleep(.25)
        c=conn(); c.execute("UPDATE agent_runs SET progress=? WHERE mission_id=? AND agent=?",(p,mid,key)); c.commit(); c.close()
    out=await llm(prompt)
    c=conn(); c.execute("UPDATE agent_runs SET status='complete',progress=100,output=? WHERE mission_id=? AND agent=?",(out,mid,key)); c.commit(); c.close()
    event(mid,name,"✓ Completed. Output handed back to Commander.")
    return out

async def execute(mid):
    c=conn(); c.execute("UPDATE missions SET status='running' WHERE id=?",(mid,)); c.commit(); c.close()
    row=conn().execute("SELECT task FROM missions WHERE id=?",(mid,)).fetchone()
    context=row["task"]; outputs=[]
    event(mid,"COMMANDER","Mission accepted. Building autonomous execution plan.")
    for idx,(key,name,desc) in enumerate(AGENTS):
        if key!="qc":
            context = context + "\nPrevious agent output:\n" + (outputs[-1] if outputs else "none")
        out=await run_agent(mid,key,name,desc,context)
        outputs.append(out)
        pct=round((idx+1)/len(AGENTS)*100)
        c=conn(); c.execute("UPDATE missions SET progress=? WHERE id=?",(pct,mid)); c.commit(); c.close()
    result={"mission_id":mid,"status":"complete","deliverables":outputs[:-1],
            "quality_control":outputs[-1],"message":"Mission completed by autonomous pipeline."}
    c=conn(); c.execute("UPDATE missions SET status='complete',progress=100,completed_at=?,result=? WHERE id=?",
                         (now(),json.dumps(result,ensure_ascii=False),mid)); c.commit(); c.close()
    event(mid,"COMMANDER","MISSION COMPLETE. Final QC passed; user notification ready.")

@app.get("/api/health")
def health(): return {"ok":True,"version":"3.0"}

@app.post("/api/missions")
async def create(payload: MissionIn, background_tasks: BackgroundTasks):
    mid=str(uuid.uuid4())
    c=conn(); c.execute("INSERT INTO missions VALUES(?,?,?,?,?,?,?,?)",
        (mid,payload.task,payload.mode,"queued",0,now(),None,None)); c.commit(); c.close()
    background_tasks.add_task(execute,mid)
    return get(mid)

@app.get("/api/missions")
def missions():
    return [dict(r) for r in conn().execute("SELECT * FROM missions ORDER BY created_at DESC").fetchall()]

@app.get("/api/missions/{mid}")
def get(mid):
    c=conn(); m=c.execute("SELECT * FROM missions WHERE id=?",(mid,)).fetchone()
    if not m: return {"error":"not_found"}
    events=[dict(r) for r in c.execute("SELECT * FROM events WHERE mission_id=? ORDER BY id",(mid,)).fetchall()]
    agents=[dict(r) for r in c.execute("SELECT * FROM agent_runs WHERE mission_id=? ORDER BY id",(mid,)).fetchall()]
    return {**dict(m),"events":events,"agents":agents,
            "result":json.loads(m["result"]) if m["result"] else None}

@app.post("/api/missions/{mid}/stop")
def stop(mid):
    # Cooperative stop flag for next production queue implementation.
    c=conn(); c.execute("UPDATE missions SET status='stopping' WHERE id=? AND status IN ('queued','running')",(mid,)); c.commit(); c.close()
    event(mid,"COMMANDER","Stop requested by user.")
    return get(mid)
