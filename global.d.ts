
type DagRun = {
    dag_id: string,
    dag_run_id: string,
    dag_run_url: string,
    execution_date: string,
    start_date: string,
    state: "failed"
}

type TaskInstance = {
    dag_id: string,
    duration: number,
    end_date: string,
    execution_date: string,
    executor_config: string,
    hostname: string,
    max_tries: number,
    operator: string,
    pid: number,
    pool: string,
    pool_slots: number,
    priority_weight: number,
    queue: string,
    queued_when: string,
    start_date: string,
    state: string,
    task_id: string,
    try_number: number,
    unixname: string,
};


declare module "opn";