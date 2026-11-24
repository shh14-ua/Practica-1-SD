#!/usr/bin/env node
/**
 * server.js: Servidor Web Node.js para el Front de Water Management (Release 2).
 * Sirve los archivos estáticos del Dashboard y actúa como proxy hacia API_Central.
 * Desarrollado con módulos estándar de Node.js (sin dependencias externas obligatorias).
 *
 * Seguridad del proxy:
 *  - Solo reenvía consultas (GET /api/...) y las acciones de control (POST /api/action), que Central
 *    protege con el token de administración que introduce el usuario. Las notificaciones climáticas
 *    de WM_W (/api/weather/...) y cualquier otra escritura no se reenvían.
 *  - Añade X-Forwarded-For con la IP real del cliente: Central nunca trata una petición reenviada
 *    como local. Nunca reenvía X-Weather-Token.
 *  - Si API_Central usa HTTPS, verifica su certificado (CA del sistema o CENTRAL_API_CA).
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const url = require('url');

// Configuración por variables de entorno o argumentos
const PORT = parseInt(process.env.PORT || process.argv[2] || '3000', 10);
const CENTRAL_API_URL = process.env.CENTRAL_API_URL || process.argv[3] || 'http://localhost:8080';
// CA adicional para validar un API_Central HTTPS con certificado propio (opcional)
const CENTRAL_API_CA = process.env.CENTRAL_API_CA ? fs.readFileSync(process.env.CENTRAL_API_CA) : undefined;

const PUBLIC_DIR = path.join(__dirname, 'public');

// MIME types soportados
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml'
};

function proxyToCentral(req, res, targetPath) {
  let centralUrl;
  try {
    centralUrl = new URL(CENTRAL_API_URL);
  } catch (e) {
    res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: 'Config Error', message: `CENTRAL_API_URL inválida: ${CENTRAL_API_URL}` }));
    return;
  }

  const isHttps = centralUrl.protocol === 'https:';
  const transport = isHttps ? https : http;
  const defaultPort = isHttps ? 443 : 80;

  // Cabeceras reenviadas: nunca el token de WM_W; el de administración solo para /api/action
  const headers = { ...req.headers, host: centralUrl.host };
  delete headers['x-weather-token'];
  if (targetPath.split('?')[0] !== '/api/action') {
    delete headers['x-admin-token'];
  }
  headers['x-forwarded-for'] = req.socket.remoteAddress || 'desconocida';

  const options = {
    hostname: centralUrl.hostname,
    port: centralUrl.port || defaultPort,
    path: targetPath,
    method: req.method,
    headers
  };
  if (isHttps && CENTRAL_API_CA) {
    options.ca = CENTRAL_API_CA;
  }

  const proxyReq = transport.request(options, (proxyRes) => {
    res.writeHead(proxyRes.statusCode || 200, {
      ...proxyRes.headers,
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Token'
    });
    proxyRes.pipe(res);
  });

  proxyReq.on('error', (err) => {
    res.writeHead(502, {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({
      error: 'Bad Gateway',
      message: `No se pudo contactar con API_Central en ${CENTRAL_API_URL}: ${err.message}`
    }));
  });

  req.pipe(proxyReq);
}

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // Respuesta rápida a preflight CORS
  if (req.method === 'OPTIONS') {
    res.writeHead(200, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Token'
    });
    res.end();
    return;
  }

  // Redirigir peticiones API a Central a través del proxy integrado: consultas públicas y acciones
  // de control (protegidas por Central con el token de administración). Nada más.
  if (pathname.startsWith('/api/')) {
    const allowed = req.method === 'GET' || (req.method === 'POST' && pathname === '/api/action');
    if (!allowed) {
      res.writeHead(403, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ status: 'forbidden', msg: 'Operación no disponible a través del Front' }));
      return;
    }
    proxyToCentral(req, res, req.url);
    return;
  }

  // Configuración para el cliente web: /config.js
  // Usa ruta relativa "" por defecto para enrutar a través del proxy /api/* de este servidor,
  // evitando errores de CORS y fallos de resolución de host internos en Docker/Cloud.
  if (pathname === '/config.js') {
    res.writeHead(200, { 'Content-Type': 'application/javascript; charset=utf-8' });
    res.end(
      `window.API_CENTRAL_URL = "";\n` +
      `window.CENTRAL_BACKEND_TARGET = ${JSON.stringify(CENTRAL_API_URL)};\n`
    );
    return;
  }

  // Servir archivos estáticos de public/
  let filePath = path.join(PUBLIC_DIR, pathname === '/' ? 'index.html' : pathname);

  // Seguridad contra Directory Traversal
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('Acceso denegado');
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      filePath = path.join(PUBLIC_DIR, 'index.html');
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    fs.readFile(filePath, (readErr, content) => {
      if (readErr) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Archivo no encontrado');
      } else {
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
      }
    });
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[FRONT] Cuadro de Mandos Web escuchando en http://0.0.0.0:${PORT}/`);
  console.log(`[FRONT] Conectado con API_Central en ${CENTRAL_API_URL}`);
});
