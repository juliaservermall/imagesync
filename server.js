const express = require('express');
const multer = require('multer');
const axios = require('axios');
const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3224;

const UPLOAD_DIR = path.join(__dirname, 'uploads');
const IMAGES_DIR = path.join(__dirname, 'images');
[UPLOAD_DIR, IMAGES_DIR].forEach(d => fs.mkdirSync(d, { recursive: true }));

let progressState = {
  running: false, total: 0, current: 0, currentName: '',
  downloaded: 0, skipped: 0, errors: 0,
  errorsLog: [], done: false, resultFile: null, resultIsCsv: false,
};
let stopRequested = false;

const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok =
      file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
      file.mimetype === 'text/csv' ||
      file.mimetype === 'application/csv' ||
      file.originalname.endsWith('.xlsx') ||
      file.originalname.endsWith('.csv');
    ok ? cb(null, true) : cb(new Error('Only .xlsx or .csv'));
  },
});

app.use(cors());
app.use(express.json());
app.use('/images', express.static(IMAGES_DIR, {
  setHeaders: res => res.setHeader('X-Robots-Tag', 'noindex, nofollow'),
}));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/robots.txt', (req, res) => {
  res.type('text/plain');
  res.send('User-agent: *\nDisallow: /images/\nDisallow: /uploads/');
});

const IMAGE_COLUMN_NAME = 'Детальная картинка (путь)';
const NAME_COLUMN_NAMES = ['Наименование элемента', 'Наименование'];

function parseCsv(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const clean = content.replace(/^\uFEFF/, '');
  const lines = clean.split(/\r?\n/);

  function parseLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
        else inQuotes = !inQuotes;
      } else if (ch === ';' && !inQuotes) {
        result.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
    result.push(current);
    return result;
  }

  return lines.filter(l => l.trim() !== '').map(l => parseLine(l));
}

function parseFile(filePath, originalName) {
  const isCsv = originalName.toLowerCase().endsWith('.csv');
  let rows;

  if (isCsv) {
    rows = parseCsv(filePath);
  } else {
    const wb = XLSX.readFile(filePath, { codepage: 65001 });
    const ws = wb.Sheets[wb.SheetNames[0]];
    rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
  }

  if (!rows.length) return { headers: [], parsedRows: [], imageColIndex: -1 };

  const headers = rows[0].map(h => String(h || '').trim());
  const imageColIndex = headers.findIndex(h => h === IMAGE_COLUMN_NAME);

  // Найти колонку с наименованием
  let nameColIndex = -1;
  for (const colName of NAME_COLUMN_NAMES) {
    const idx = headers.findIndex(h => h === colName);
    if (idx !== -1) { nameColIndex = idx; break; }
  }

  const parsedRows = rows.slice(1).map((row, i) => {
    const cells = headers.map((_, ci) => String(row[ci] != null ? row[ci] : ''));
    const url = imageColIndex >= 0 ? cells[imageColIndex] : '';
    // Использовать колонку наименования если найдена, иначе первую колонку
    const name = (nameColIndex >= 0 ? cells[nameColIndex] : cells[0]) || '';
    return { rowIndex: i + 1, name, url, cells };
  });

  return { headers, parsedRows, imageColIndex };
}

function generateFilename(productName, contentType) {
  const extMap = {
    'image/jpeg': '.jpg',
    'image/jpg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/svg+xml': '.svg',
  };
  const ext = extMap[contentType && contentType.split(';')[0].trim()] || '.jpg';
  // Использовать полное название, привести к нижнему регистру
  const cleaned = productName
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9а-яёa-z\s\-]/gi, ' ')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
  const filename = cleaned || 'image';
  return filename + ext;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const randDelay = () => sleep(1000 + Math.random() * 2000);

async function downloadImage(url, destPath, retries) {
  if (retries === undefined) retries = 3;
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://www.google.com/',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
  };
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios({
        method: 'GET',
        url: url,
        headers: headers,
        responseType: 'stream',
        timeout: 120000,
        maxRedirects: 5,
      });
      const ct = response.headers['content-type'] || '';
      if (!ct.startsWith('image/')) throw new Error('Not an image: ' + ct);
      // Таймаут на скачивание потока — 120 секунд
      await new Promise(function(resolve, reject) {
        const writer = fs.createWriteStream(destPath);
        writer.setMaxListeners(20);
        let finished = false;

        const timer = setTimeout(function() {
          if (!finished) {
            finished = true;
            response.data.destroy();
            writer.destroy();
            try { fs.unlinkSync(destPath); } catch(e) {}
            reject(new Error('Таймаут скачивания (120 сек)'));
          }
        }, 120000);

        response.data.pipe(writer);

        writer.on('finish', function() {
          if (!finished) {
            finished = true;
            clearTimeout(timer);
            resolve();
          }
        });

        writer.on('error', function(err) {
          if (!finished) {
            finished = true;
            clearTimeout(timer);
            reject(err);
          }
        });

        response.data.on('error', function(err) {
          if (!finished) {
            finished = true;
            clearTimeout(timer);
            reject(err);
          }
        });
      });
      return { success: true, contentType: ct };
    } catch (err) {
      if (attempt < retries) {
        await sleep(1000 * attempt);
      } else {
        throw err;
      }
    }
  }
}

let currentFileInfo = { headers: [], parsedRows: [], imageColIndex: -1, originalName: '' };

app.post('/upload', upload.single('file'), function(req, res) {
  try {
    var parsed = parseFile(req.file.path, req.file.originalname);
    var headers = parsed.headers;
    var parsedRows = parsed.parsedRows;
    var imageColIndex = parsed.imageColIndex;

    currentFileInfo = {
      headers: headers,
      parsedRows: parsedRows,
      imageColIndex: imageColIndex,
      originalName: req.file.originalname,
    };

    if (imageColIndex === -1) {
      return res.status(400).json({
        success: false,
        error: 'Колонка "' + IMAGE_COLUMN_NAME + '" не найдена. Проверьте заголовки файла.',
      });
    }

    var withUrls = parsedRows.filter(function(r) {
      return r.url && String(r.url).startsWith('http');
    }).length;

    var preview = parsedRows.slice(0, 5).map(function(r) {
      return { name: r.name, url: r.url };
    });

    res.json({
      success: true,
      total: parsedRows.length,
      withUrls: withUrls,
      preview: preview,
      headers: headers,
      imageColIndex: imageColIndex,
      imageColName: IMAGE_COLUMN_NAME,
    });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.post('/download-images', async function(req, res) {
  if (progressState.running) {
    return res.status(409).json({ error: 'Already running' });
  }

  var domain = req.body.domain || 'https://yourdomain.com';
  var headers = currentFileInfo.headers;
  var parsedRows = currentFileInfo.parsedRows;
  var imageColIndex = currentFileInfo.imageColIndex;
  var originalName = currentFileInfo.originalName;

  if (imageColIndex === -1) {
    return res.status(400).json({ error: 'Колонка "' + IMAGE_COLUMN_NAME + '" не найдена' });
  }

  progressState = {
    running: true, total: 0, current: 0, currentName: '',
    downloaded: 0, skipped: 0, errors: 0,
    errorsLog: [], done: false, resultFile: null, resultIsCsv: false,
  };
  stopRequested = false;

  var urlRows = parsedRows.filter(function(r) {
    return r.url && String(r.url).startsWith('http');
  });
  progressState.total = urlRows.length;

  res.json({ success: true, total: urlRows.length });

  (async function() {
    var resultRows = [headers].concat(parsedRows.map(function(r) {
      return r.cells.slice();
    }));

    for (var i = 0; i < urlRows.length; i++) {
      if (stopRequested) break;
      var row = urlRows[i];
      progressState.current++;
      progressState.currentName = String(row.name).slice(0, 60);

      try {
        var contentType = 'image/jpeg';
        try {
          var headRes = await axios.head(row.url, {
            timeout: 10000,
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
          }).catch(function() { return null; });
          if (headRes) contentType = headRes.headers['content-type'] || contentType;
        } catch (e) {}

        var filename = generateFilename(String(row.name), contentType);
        var destPath = path.join(IMAGES_DIR, filename);

        await downloadImage(row.url, destPath);
        resultRows[row.rowIndex][imageColIndex] = domain + '/images/' + filename;
        progressState.downloaded++;
      } catch (err) {
        progressState.errors++;
        progressState.errorsLog.push({
          row: row.rowIndex + 1,
          name: String(row.name).slice(0, 80),
          url: String(row.url).slice(0, 200),
          error: err.response ? 'HTTP ' + err.response.status : (err.code || err.message || 'Unknown error'),
        });
      }

      await randDelay();
    }

    var isCsv = originalName.toLowerCase().endsWith('.csv');
    var resultPath;

    if (isCsv) {
      var csvLines = resultRows.map(function(row) {
        return row.map(function(cell) {
          var s = String(cell != null ? cell : '');
          return /[;"'\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
        }).join(';');
      });
      resultPath = path.join(UPLOAD_DIR, 'result_' + Date.now() + '.csv');
      fs.writeFileSync(resultPath, '\uFEFF' + csvLines.join('\r\n'), 'utf-8');
    } else {
      var wb = XLSX.utils.book_new();
      var ws = XLSX.utils.aoa_to_sheet(resultRows);
      XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');
      resultPath = path.join(UPLOAD_DIR, 'result_' + Date.now() + '.xlsx');
      XLSX.writeFile(wb, resultPath);
    }

    progressState.resultFile = resultPath;
    progressState.resultIsCsv = isCsv;
    progressState.running = false;
    progressState.done = true;
    progressState.stopped = stopRequested;
  })();
});

app.get('/progress', function(req, res) {
  res.json(progressState);
});

app.get('/download-result', function(req, res) {
  if (!progressState.resultFile || !fs.existsSync(progressState.resultFile)) {
    return res.status(404).json({ error: 'No result file' });
  }
  var filename = progressState.resultIsCsv ? 'result_updated.csv' : 'result_updated.xlsx';
  res.download(progressState.resultFile, filename);
});

app.get('/download-errors', function(req, res) {
  if (!progressState.errorsLog.length) {
    return res.status(404).json({ error: 'No errors' });
  }
  var lines = ['Row\tName\tURL\tError'].concat(
    progressState.errorsLog.map(function(e) {
      return e.row + '\t' + e.name + '\t' + e.url + '\t' + e.error;
    })
  ).join('\n');
  res.setHeader('Content-Disposition', 'attachment; filename="errors_log.tsv"');
  res.setHeader('Content-Type', 'text/tab-separated-values');
  res.send(lines);
});

app.post('/stop', function(req, res) {
  stopRequested = true;
  res.json({ success: true });
});


app.post('/clear-images', function(req, res) {
  try {
    var files = fs.readdirSync(IMAGES_DIR);
    var deleted = 0;
    files.forEach(function(file) {
      var filePath = path.join(IMAGES_DIR, file);
      try {
        fs.unlinkSync(filePath);
        deleted++;
      } catch (e) {}
    });
    res.json({ success: true, deleted: deleted });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.listen(PORT, function() {
  console.log('Server running at http://localhost:' + PORT);
});
