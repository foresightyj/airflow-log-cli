#!/usr/bin/env node
//@ts-check
import path from "path";
import _ from "lodash";
import shelljs from "shelljs";
import opn from "opn";
import fs from "fs-extra";
import program from "commander";
import assert from "assert";
import { AirflowLog } from ".";
import { DagTask } from "./DagTask";

const pkg = require("../package.json");

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
    .parse(process.argv);

const dagId = program.dagId as string;
assert(dagId, "dagId is falsy");

const taskId = program.taskId as string;
assert(taskId, "taskId is falsy");

const baseURL = program.baseUrl as string;
assert(baseURL, "baseURL is falsy");

const TO_FILE = Boolean(program.toFile);
const AVAJS_STAT = Boolean(program.avajsStat);
const OPEN_IN_BROWSER = Boolean(program.openInBrowser);


async function analyzeLog(airflowLog: AirflowLog, task: DagTask, runId: string) {
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
            function parseTime(s: string): Date {
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
    console.log("===================== AIRFLOW LOG BEGIN =====================");
    await analyzeLog(airflowLog, task, dag.dag_run_id);
    console.log("===================== AIRFLOW LOG END =====================");
})();
