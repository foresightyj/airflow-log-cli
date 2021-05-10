#!/usr/bin/env node

import assert from "assert"
import axios, { AxiosInstance } from "axios";
import { DagRun } from "./DagRun";
import { DagTask } from "./DagTask";

export class AirflowLog {
    private http: AxiosInstance;
    constructor(baseUrl: string) {
        this.http = axios.create({
            baseURL: baseUrl,
            auth: {
                username: "airflow",
                password: "airflow"
            }
        });
    }

    async getLatestRunOfDag(dagId: string): Promise<DagRun> {
        const res = await this.http.get(`/api/v1/dags/${dagId}/dagRuns`, {
            params: {
                limit: 1,
                order_by: "-start_date",
            }
        });
        const d = res.data;
        assert.strictEqual(res.status, 200, `get latest_runs returned ${res.status}`);
        const dag = d.dag_runs[0] as DagRun
        assert(dag.dag_id, "dag_id is falsy");
        return dag;
    }

    async getTaskInstance(dagRun: DagRun, taskId: string): Promise<DagTask> {
        const url = `/api/v1/dags/${dagRun.dag_id}/dagRuns/${dagRun.dag_run_id}/taskInstances`
        const res = await this.http.get(url);
        const data = res.data;
        const task = data.task_instances.find((inst: { task_id: string; }) => inst.task_id === taskId);
        return task;
    }

    async getTaskLog(task: DagTask, runId: string): Promise<string> {
        const url = `/api/v1/dags/${task.dag_id}/dagRuns/${runId}/taskInstances/${task.task_id}/logs/${task.try_number}`
        const res = await this.http.get(url);
        const data = res.data as string;
        assert(typeof data === "string", "data not string");
        return data;
    }
}