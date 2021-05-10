export type DagRun = {
    dag_id: string;
    dag_run_id: string;
    dag_run_url: string;
    execution_date: string;
    start_date: string;
    state: "failed";
};
