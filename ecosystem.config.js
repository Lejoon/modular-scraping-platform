const path = require('path');
const cwd = __dirname;

module.exports = {
  apps: [
    {
      name: "scraper",
      script: "main.py",
      interpreter: "python3",
      cwd: cwd,
      env: {
        PYTHONPATH: cwd,
        SCHEDULER_MODE: "enabled"
      },
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      error_file: path.join(cwd, "logs", "scraper-error.log"),
      out_file: path.join(cwd, "logs", "scraper-out.log"),
      log_file: path.join(cwd, "logs", "scraper-combined.log"),
      time: true,
      merge_logs: true
    }
  ]
};