#!/usr/bin/env node
//@ts-check
const path = require("path");
const _ = require("lodash");
const shelljs = require("shelljs");
const opn = require("opn");
const fs = require("fs-extra");
const program = require("commander");
const pkg = require("./package.json");
const assert = require("assert");
const { AirflowLog } = require(".");

program
    .name(pkg.name)
    .version(pkg.version)
    .requiredOption("-d, --dagId <dagId>")
    .requiredOption("-t, --taskId <taskId>")
    .requiredOption(
        "-b, --baseUrl <baseUrl>",
        "baseUrl of airflow",
        "http://192.168.0.229:8080"
    )
    .option("-f --toFile", "是否将结果写入到文件中，默认是打印到stdout")
    .option("-o --openInBrowser", "是否在浏览器里面打开对应的airflow web页面")
    .option("-a --avajsStat", "是否统计avajs结果")
    //   .option(
    //     "-i --keepInfo",
    //     "是否需要保留airflow日志里面的INFO行，默认是过滤掉INFO行"
    //   )
    .parse(process.argv);

/** @type {string} */
const dagId = program.dagId;
assert(dagId, "dagId is falsy");

/** @type {string} */
const taskId = program.taskId;
assert(taskId, "taskId is falsy");

/** @type {string} */
const baseURL = program.baseUrl;
assert(baseURL, "baseURL is falsy");

const TO_FILE = Boolean(program.toFile);
const AVAJS_STAT = Boolean(program.avajsStat);
// const KEEP_INFO = Boolean(program.keepInfo);
const OPEN_IN_BROWSER = Boolean(program.openInBrowser);


/**
 * @param {AirflowLog} airflowLog
 * @param {TaskInstance} task
 * @param {string} runId
 */
async function getTaskLog(airflowLog, task, runId) {
    const data = await airflowLog.getTaskLog(task, runId);
    let lines = data.split("\n");
    lines = lines.filter((l) => !l.includes(": Subtask "));
    const firstLine = lines.find((l) => l.startsWith("[202"));
    //_.reverse also mutates array
    const lastLine = _.reverse(lines.slice()).find((l) => l.startsWith("[202"));
    assert(lastLine, "lastLine falsy");
    const content = lines.join("\n");
    if (TO_FILE) {
        console.log(firstLine, lastLine, content);
        const reportDir = path.join(__dirname, "reports");
        const filePath = path.join(reportDir, `${task.task_id}.log`);
        await fs.outputFile(filePath, content, { encoding: "utf-8", });
        opn(filePath);

        if (AVAJS_STAT) {
            let deltaSeconds = 0;
            /**
             * @param {string} s
             * @returns {Date}
             */
            function parseTime(s) {
                const t = s.split("]")[0].substr(1);
                return new Date(t.split(",")[0]);
            }
            if (firstLine && lastLine.includes("[202")) {
                const startTime = parseTime(firstLine);
                const endTime = parseTime(lastLine);
                const deltaMs = endTime.getTime() - startTime.getTime();
                deltaSeconds = Number((deltaMs / 1000).toFixed(0));
            }

            const oks = shelljs
                .grep("INFO -   ✔", filePath)
                .split("\n")
                .filter((s) => s.trim());
            const attentions = shelljs
                .grep("INFO -   ✖", filePath)
                .split("\n")
                .filter((s) => s.trim());
            const okCount = oks.length;
            const failCount = attentions.length;

            const attentionFilePath = `./reports/${runId}.attention-${failCount}_${failCount + okCount
                }.log`;

            const completed = content.includes("Task exited with return code");
            await fs.promises.writeFile(
                attentionFilePath,
                `======================================================\r\nTOTAL: ${okCount + failCount
                } PASS: ${okCount}, FAIL: ${failCount}\r\n
Finished: ${completed}, Time Taken ${deltaSeconds} seconds
======================================================\r\n` +
                attentions.join("\r\n"),
                {
                    encoding: "utf-8",
                }
            );
            opn(attentionFilePath);
        }
    } else {
        console.log(content);
    }

    const webUrl = baseURL + `/log?dag_id=${dagId}&task_id=${task.task_id}&execution_date=${encodeURIComponent(task.execution_date)}`;

    if (OPEN_IN_BROWSER) {
        opn(webUrl);
    }
}


(async () => {
    const airflowLog = new AirflowLog(baseURL);
    const dag = await airflowLog.getLatestRunOfDag(dagId);
    assert(dag.execution_date, "execution_date is falsy");
    const task = await airflowLog.getTaskInstance(dag, taskId);
    const log = await airflowLog.getTaskLog(task, dag.dag_run_id);

    console.log("===================== AIRFLOW LOG BEGIN =====================");
    console.log(log);
    console.log("===================== AIRFLOW LOG END =====================");
})();
