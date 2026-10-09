// 简易本地开发服务器：同端口托管前端 + 反向代理后端，解决浏览器跨域。
// 用法：node web/dev-server.js   然后浏览器打开 http://localhost:8080
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 8080;
const BACKENDS = [
  { prefix: '/v1', target: 'http://localhost:7777' },
  { prefix: '/usercenter', target: 'http://localhost:7776' },
  { prefix: '/minio', target: 'http://127.0.0.1:39000', strip: '/minio' },
];

const server = http.createServer((req, res) => {
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  };
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); return res.end(); }

  const hit = BACKENDS.find(b => req.url.startsWith(b.prefix));
  if (hit) {
    const targetPath = (hit.strip ? req.url.replace(hit.strip, '') : req.url);
    const u = new URL(targetPath, hit.target);
    const fwdHeaders = { ...req.headers, host: u.host }; // 预签名 URL 按目标 host 签名，必须改回
    const proxyReq = http.request(u, { method: req.method, headers: fwdHeaders }, (proxyRes) => {
      res.writeHead(proxyRes.statusCode, { ...cors, 'Content-Type': proxyRes.headers['content-type'] || 'application/json' });
      proxyRes.pipe(res);
    });
    proxyReq.on('error', (e) => { res.writeHead(502, cors); res.end('proxy error: ' + e.message); });
    req.pipe(proxyReq);
    return;
  }

  // 静态页面
  const file = req.url === '/' ? 'index.html' : req.url.split('?')[0];
  const full = path.join(__dirname, file);
  if (fs.existsSync(full) && fs.statSync(full).isFile()) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', ...cors });
    return fs.createReadStream(full).pipe(res);
  }
  res.writeHead(404, cors); res.end('not found: ' + req.url);
});

server.listen(PORT, () => console.log(`dev server: http://localhost:${PORT}`));
