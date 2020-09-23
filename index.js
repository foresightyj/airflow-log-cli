#!/usr/bin/env node

//@ts-check
/** @type {import("assert")} */
const assert = require("assert");
const path = require("path");
const fs = require("fs-extra");
const opn = require("opn");
const program = require("commander");
const axios = require("axios").default;
const package = require("./package.json");

program
  .name(package.name)
  .version(package.version)
  .requiredOption("-d, --dagId <dagId>")
  .requiredOption("-r, --runId <runId>")
  .requiredOption(
    "-b, --baseUrl <baseUrl>",
    "baseUrl of airflow",
    "http://192.168.0.229:8081"
  )
  .option("-f --toFile", "是否将结果写入到文件中，默认是打印到stdout")
  .option("-o --openInBrowser", "是否在浏览器里面打开对应的airflow web页面")
  .option(
    "-i --keepInfo",
    "是否需要保留airflow日志里面的INFO行，默认是过滤掉INFO行"
  )
  .parse(process.argv);

/** @type {string} */
const dagId = program.dagId;
assert(dagId, "dagId is falsy");

/** @type {string} */
const runId = program.runId;
assert(runId, "runId is falsy");

/** @type {string} */
const baseURL = program.baseUrl;
assert(baseURL, "baseURL is falsy");

const TO_FILE = Boolean(program.toFile);
const KEEP_INFO = Boolean(program.keepInfo);
const OPEN_IN_BROWSER = Boolean(program.openInBrowser);

const http = axios.create({ baseURL });

/**
 * @typedef {{dag_id:string, dag_run_url:string, execution_date:string, start_date:string}} Dag
 */

/**
 * @param {string} dagPrefix
 * @returns {Promise<Dag>}
 */
async function getLatestRunOfDag(dagPrefix) {
  const res = await http.get("/api/experimental/latest_runs");
  const d = res.data;
  /** @type {Dag} */
  const dag = d.items.find((d) => d.dag_id.startsWith(dagPrefix));
  assert(dag.dag_id, "dag_id is falsy");
  return dag;
}

/**
 * @param {string} dagId
 * @param {string} taskId
 * @param {string} execution_date
 */
async function getDagRunLog(dagId, taskId, execution_date) {
  const url = `/admin/airflow/get_logs_with_metadata?task_id=${taskId}&dag_id=${dagId}&execution_date=${encodeURIComponent(
    execution_date
  )}&try_number=1&metadata=null`;
  const res = await http.get(url);
  const data = res.data;
  /** @type {string} */
  let content = data.message;
  assert(content, "content falsy");
  assert(typeof content === "string", "content not string");
  let lines = content.split("\n");
  if (!KEEP_INFO) {
    lines = lines.filter((l) => !l.includes("INFO"));
  }
  const smartEnd = lines.findIndex((l) =>
    l.includes("npm ERR! This is probably not a problem with npm")
  );
  if (smartEnd > -1) {
    lines = lines.slice(0, smartEnd);
  }
  content = lines.join("\n");
  if (TO_FILE) {
    const reportDir = path.join(__dirname, "reports");
    await fs.ensureDir(reportDir);
    const filePath = path.join(reportDir, `${taskId}.log`);
    await fs.promises.writeFile(filePath, content, {
      encoding: "utf-8",
    });
    opn(filePath);
  } else {
    console.log(content);
  }

  const webUrl =
    baseURL +
    `/admin/airflow/graph?dag_id=${dagId}&execution_date=${encodeURIComponent(
      execution_date
    )}`;

  if (OPEN_IN_BROWSER) {
    opn(webUrl);
  }
}

(async () => {
  const dag = await getLatestRunOfDag(dagId);
  const runUrl = new URL(baseURL + dag.dag_run_url);
  const params = runUrl.searchParams;
  const execution_date = params.get("execution_date");
  assert(execution_date, "execution_date is falsy");
  await getDagRunLog(dag.dag_id, runId, execution_date);
})();
