(() => {
  'use strict';
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
  const demo = document.querySelector('[data-memory-demo]');
  if (demo) {
    demo.classList.add('enhanced');
    const stages = [
      {client:'One useful discovery', clientLabel:'CLIENT A · TODAY', heading:'READY FOR THE NEXT SESSION', text:'The reason stays with the decision.', caption:'Record what you learned, together with why it matters.'},
      {client:'The conversation ends. The file stays.', clientLabel:'YOUR KNOWLEDGE BASE', heading:'READABLE, LINKED, AND SEARCHABLE', text:'A Markdown file you can open in any editor.', caption:'Keep knowledge outside a conversation, in files you control.'},
      {client:'“Why is the pool limited to 20?”', clientLabel:'CLIENT B · NEXT SESSION', heading:'CONTEXT RETRIEVED', text:'“Higher limits exhaust our worker budget.”', caption:'A new session retrieves the decision and the reason behind it.'}
    ];
    let index=0, paused=reduced.matches, timer=null;
    const controls=[...demo.querySelectorAll('[data-step]')];
    const motion=demo.querySelector('[data-motion]');
    const render=()=>{
      const state=stages[index];
      demo.querySelector('.demo-canvas').dataset.stage=String(index);
      demo.querySelector('[data-client-title]').textContent=state.client;
      demo.querySelector('.client-pill small').textContent=state.clientLabel;
      demo.querySelector('[data-recall-heading]').textContent=state.heading;
      demo.querySelector('[data-recall-text]').textContent=state.text;
      demo.querySelector('[data-caption]').textContent=state.caption;
      demo.querySelector('[data-caption-index]').textContent=`0${index+1} / 03`;
      controls.forEach((button,n)=>{button.classList.toggle('active',n===index);button.setAttribute('aria-pressed',String(n===index));});
      demo.classList.toggle('paused',paused);
      motion.textContent=paused?'Play ▷':'Pause Ⅱ';
      motion.setAttribute('aria-label',paused?'Play animation':'Pause animation');
      const progress=demo.querySelector('.caption-progress i');
      progress.style.animation='none';
      requestAnimationFrame(()=>{progress.style.animation='';});
    };
    const schedule=()=>{clearInterval(timer);timer=null;if(!paused&&!document.hidden)timer=setInterval(()=>{index=(index+1)%3;render();},6000);};
    controls.forEach((button,n)=>button.addEventListener('click',()=>{index=n;paused=true;render();schedule();}));
    motion.addEventListener('click',()=>{paused=!paused;render();schedule();});
    reduced.addEventListener('change',()=>{if(reduced.matches){paused=true;render();schedule();}});
    document.addEventListener('visibilitychange',schedule);
    render();schedule();
  }
  document.querySelectorAll('[data-copy]').forEach(button=>{
    button.addEventListener('click',async()=>{
      const target=document.getElementById(button.dataset.copy);
      if(!target)return;
      const status=button.closest('.install-terminal')?.querySelector('.copy-status');
      try{await navigator.clipboard.writeText(target.textContent);button.textContent='Copied';if(status)status.textContent='Installation commands copied.';}
      catch{if(status)status.textContent='Copy is unavailable. Select the commands above to copy them.';}
    });
  });
})();
