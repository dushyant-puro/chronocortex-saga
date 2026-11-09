// Eligibility is an observation. Admission rechecks the original and claims it.
export function installTaskReplacement({api,session,canPrepare,prepare}) {
  const $=(id)=>document.getElementById(id);
  let owner=null,generation=0,busy=false,selected=null;
  const id=(v)=>typeof v==='string' && v.length>0 && v.length<=256;
  function message(text,warning=false){$('replacement-message').textContent=text;$('replacement-message').setAttribute('role',warning?'alert':'status');$('replacement-message').className=warning?'notice proposal-warning':'notice';}
  function render(){
    $('task-replacement').hidden=!session();
    $('replacement-inspect').disabled=busy||!session();$('replacement-origin').disabled=busy;
    $('replacement-prepare').disabled=busy||!selected||!canPrepare()||!session()?.permissions.includes('ccs:cancel');
  }
  function reset(){owner=null;generation+=1;busy=false;selected=null;$('replacement-origin').value='';message('Inspect one cancelled task. This flow refuses all recorded activity.');$('task-replacement').hidden=true;}
  function sync(){if(owner!==session()){reset();owner=session();}render();}
  async function inspect(event){
    event.preventDefault();const identity=session(),version=generation,task=$('replacement-origin').value;
    if(!identity||busy||!id(task))return;
    selected=null;busy=true;render();message('Inspecting current journal eligibility…');
    try{
      const v=await api('/api/proposals/replacement-status',{task_id:task});
      if(session()!==identity||generation!==version)return;
      if(!v||v.scope!=='JOURNAL_ZERO_ACTIVITY_ONLY'||v.execution_authorized!==false||typeof v.eligible!=='boolean')throw new Error();
      if(v.eligible){
        const o=v.original;
        if(!o||o.task_id!==task||!Number.isSafeInteger(o.version)||o.version<0||typeof o.digest!=='string'||!/^[0-9a-f]{64}$/.test(o.digest))throw new Error();
        selected={task_id:o.task_id,version:o.version,digest:o.digest};
        message(`Eligible checkpoint ${task}, version ${o.version}. Review current replacement arguments next. The original remains cancelled.`);
      }else{
        const reasons={TASK_NOT_CANCELLED:'The original must be explicitly cancelled first.',RECORDED_ACTIVITY:'Recorded activity blocks this replacement flow. Reconciliation is required separately.',CHAIN_NOT_CLOSED:'The original task chain is not durably closed.',ALREADY_REPLACED:'A replacement is already recorded. Inspect that task.',EVIDENCE_CHANGED:'The original evidence changed. Inspect again.'};
        message(reasons[v.reason]||'This task is not eligible for zero-activity replacement.',true);
      }
    }catch{if(session()!==identity||generation!==version)return;selected=null;message('Replacement eligibility is unavailable. No review or admission was sent.',true);}
    finally{if(session()===identity&&generation===version){busy=false;render();}}
  }
  $('replacement-form').addEventListener('submit',inspect);
  $('replacement-origin').addEventListener('input',()=>{selected=null;render();});
  $('replacement-prepare').addEventListener('click',()=>{if(!busy&&selected&&canPrepare()&&session()?.permissions.includes('ccs:cancel'))prepare(selected);});
  return {sync,reset};
}
