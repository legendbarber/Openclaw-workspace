import urllib.request, re, html, json
u='https://finance.naver.com/research/company_list.naver'
req=urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'})
s=urllib.request.urlopen(req,timeout=20).read().decode('euc-kr','ignore')
rows=re.findall(r'<tr>(.*?)</tr>',s,re.S)
out=[]
for r in rows:
    m=re.search(r'/item/main.naver\?code=(\d+)"[^>]*>(.*?)</a>',r,re.S)
    t=re.search(r'href="company_read.naver\?nid=(\d+)[^"]*"[^>]*>(.*?)</a>',r,re.S)
    tds=re.findall(r'<td[^>]*>(.*?)</td>',r,re.S)
    if m and t:
        clean=lambda x: html.unescape(re.sub('<.*?>','',x)).strip()
        vals=[clean(x) for x in tds]
        out.append({
            'code': m.group(1),
            'name': clean(m.group(2)),
            'title': clean(t.group(2)),
            'broker': vals[2] if len(vals)>2 else '',
            'date': vals[5] if len(vals)>5 else '',
            'nid': t.group(1)
        })
    if len(out)>=20:
        break
with open('C:/Users/mangi/.openclaw/workspace/reports.json','w',encoding='utf-8') as f:
    json.dump(out,f,ensure_ascii=False,indent=2)
print('saved',len(out))
