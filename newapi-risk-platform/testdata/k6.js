import http from 'k6/http';
import { check, sleep } from 'k6';
export const options={vus:Number(__ENV.VUS||20),duration:__ENV.DURATION||'30s',thresholds:{http_req_failed:['rate<0.02'],http_req_duration:['p(95)<2500']}};
export default function(){const payload=JSON.stringify({model:'test-model',messages:[{role:'user',content:'Return the word ok'}],stream:false});const r=http.post(__ENV.TARGET_URL,payload,{headers:{Authorization:`Bearer ${__ENV.UPSTREAM_API_KEY}`,'Content-Type':'application/json','X-Risk-User-ID':`load-${__VU}`}});check(r,{'status accepted':x=>x.status===200||x.status===555||x.status===429});sleep(.05)}
