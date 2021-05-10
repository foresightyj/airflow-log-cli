#!/usr/bin/env node

const assert = require("assert");
const axios = require("axios").default;

class AirflowLog {
    /**
     * @param {string} baseUrl 
     */
    constructor(baseUrl) {
        this.http = axios.create({
            baseURL: baseUrl,
            auth: {
                username: "airflow",
                password: "airflow"
            }
        });
    }

    /**
     * @param {string} dagId
     * @returns {Promise<DagRun>}
     */
    async getLatestRunOfDag(dagId) {
        const res = await this.http.get(`/api/v1/dags/${dagId}/dagRuns`, {
            params: {
                limit: 1,
                order_by: "-start_date",
            }
        });
        const d = res.data;
        assert.strictEqual(res.status, 200, `get latest_runs returned ${res.status}`);
        /** @type {DagRun} */
        const dag = d.dag_runs[0]
        assert(dag.dag_id, "dag_id is falsy");
        return dag;
    }

    /**
     * @param {DagRun} dagRun
     * @param {string} taskId
     * @returns {Promise<TaskInstance>}
     */
    async getTaskInstance(dagRun, taskId) {
        const url = `/api/v1/dags/${dagRun.dag_id}/dagRuns/${dagRun.dag_run_id}/taskInstances`
        const res = await this.http.get(url);
        const data = res.data;
        const task = data.task_instances.find(inst => inst.task_id === taskId);
        return task;
    }

    /**
     * @param {TaskInstance} task
     * @param {string} runId
     * @returns {Promise<string>}
     */
    async getTaskLog(task, runId) {
        const url = `/api/v1/dags/${task.dag_id}/dagRuns/${runId}/taskInstances/${task.task_id}/logs/${task.try_number}`
        const res = await this.http.get(url);
        const data = res.data;
        /** @type {string} */
        assert(typeof data === "string", "data not string");
        return data;
    }
}


module.exports = {
    AirflowLog,
}