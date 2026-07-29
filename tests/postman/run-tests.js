const fs = require('fs');
const http = require('http');
const https = require('https');
const path = require('path');
const { spawn } = require('child_process');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .option('environment', {
    alias: 'e',
    description: 'Environment to run tests against',
    type: 'string',
    default: 'local'
  })
  .option('collection', {
    alias: 'c',
    description: 'Collection to run',
    type: 'string',
    default: 'CinemaAbyss'
  })
  .option('folder', {
    alias: 'f',
    description: 'Specific folder in the collection to run',
    type: 'string'
  })
  .option('reporters', {
    alias: 'r',
    description: 'Reporters to use (comma-separated)',
    type: 'string',
    default: 'cli,htmlextra,junit'
  })
  .option('bail', {
    alias: 'b',
    description: 'Stop on first error',
    type: 'boolean',
    default: false
  })
  .option('timeout', {
    alias: 't',
    description: 'Request timeout in ms',
    type: 'number',
    default: 10000
  })
  .option('startup-timeout', {
    description: 'Maximum time to wait for services in ms',
    type: 'number',
    default: 120000
  })
  .strict()
  .help()
  .alias('help', 'h')
  .parse();

const reportsDir = path.join(__dirname, 'reports');
fs.mkdirSync(reportsDir, { recursive: true });

const collectionPath = path.join(__dirname, `${argv.collection}.postman_collection.json`);
const environmentPath = path.join(__dirname, `${argv.environment}.environment.json`);

for (const filePath of [collectionPath, environmentPath]) {
  if (!fs.existsSync(filePath)) {
    console.error(`Required file not found: ${filePath}`);
    process.exit(1);
  }
}

function loadEnvironmentValues(filePath) {
  const environment = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return Object.fromEntries(
    (environment.values || [])
      .filter((item) => item.enabled !== false)
      .map((item) => [item.key, item.value])
  );
}

function requestHealth(urlString) {
  return new Promise((resolve) => {
    const parsed = new URL(urlString);
    const client = parsed.protocol === 'https:' ? https : http;
    const request = client.get(parsed, { timeout: 2000 }, (response) => {
      response.resume();
      resolve(response.statusCode >= 200 && response.statusCode < 300);
    });
    request.on('timeout', () => {
      request.destroy();
      resolve(false);
    });
    request.on('error', () => resolve(false));
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForServices(values) {
  const targets = {
    'Monolith Service': [{ name: 'monolith', url: `${values.baseUrl}/health` }],
    'Movies Microservice': [{ name: 'movies-service', url: `${values.moviesServiceUrl}/api/movies/health` }],
    'Events Microservice': [{ name: 'events-service', url: `${values.eventsServiceUrl}/api/events/health` }],
    'Proxy Service': [{ name: 'proxy-service', url: `${values.proxyServiceUrl}/health` }]
  };

  const selected = argv.folder ? targets[argv.folder] : Object.values(targets).flat();
  if (!selected) {
    throw new Error(`Unknown collection folder: ${argv.folder}`);
  }

  const deadline = Date.now() + argv.startupTimeout;
  for (const target of selected) {
    process.stdout.write(`Waiting for ${target.name} at ${target.url}`);
    while (Date.now() < deadline) {
      if (await requestHealth(target.url)) {
        process.stdout.write(' - ready\n');
        break;
      }
      process.stdout.write('.');
      await sleep(2000);
    }
    if (Date.now() >= deadline && !(await requestHealth(target.url))) {
      process.stdout.write('\n');
      throw new Error(
        `${target.name} did not become ready at ${target.url}. ` +
        'Run "docker compose up -d --build" and check "docker compose ps".'
      );
    }
  }
}

function resolveNewmanCli() {
  try {
    return require.resolve('newman/bin/newman.js');
  } catch (error) {
    throw new Error('Newman is not installed. Run "npm ci" in tests/postman before starting tests.');
  }
}

function runNewman() {
  const timestamp = new Date().toISOString().replace(/:/g, '-');
  const reporters = argv.reporters.split(',').map((item) => item.trim()).filter(Boolean);
  const args = [
    resolveNewmanCli(),
    'run',
    collectionPath,
    '--environment', environmentPath,
    '--reporters', reporters.join(','),
    '--timeout-request', String(argv.timeout),
    '--delay-request', '100'
  ];

  if (reporters.includes('htmlextra')) {
    args.push('--reporter-htmlextra-export', path.join(reportsDir, `report-${argv.environment}-${timestamp}.html`));
  }
  if (reporters.includes('junit')) {
    args.push('--reporter-junit-export', path.join(reportsDir, `junit-report-${argv.environment}-${timestamp}.xml`));
  }
  if (argv.folder) {
    args.push('--folder', argv.folder);
  }
  if (argv.bail) {
    args.push('--bail');
  }

  console.log(`Running tests against ${argv.environment} environment...`);
  const child = spawn(process.execPath, args, { stdio: 'inherit' });
  child.on('error', (error) => {
    console.error(`Unable to start Newman: ${error.message}`);
    process.exit(1);
  });
  child.on('close', (code) => {
    process.exit(code ?? 1);
  });
}

(async () => {
  try {
    const environmentValues = loadEnvironmentValues(environmentPath);
    await waitForServices(environmentValues);
    runNewman();
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
})();
